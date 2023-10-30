#pragma once

#include <optional>
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/query/query.h"

namespace reindexer {

struct NsContext;
class NamespaceImpl;

class ItemModifier {
public:
	ItemModifier(const std::vector<UpdateEntry> &, NamespaceImpl &ns);
	ItemModifier(const ItemModifier &) = delete;
	ItemModifier &operator=(const ItemModifier &) = delete;
	ItemModifier(ItemModifier &&) = delete;
	ItemModifier &operator=(ItemModifier &&) = delete;

	[[nodiscard]] bool Modify(IdType itemId, const NsContext &ctx);
	PayloadValue &GetPayloadValueBackup() { return rollBackIndexData_.GetPayloadValueBackup(); }

private:
	struct FieldData {
		FieldData(const UpdateEntry &entry, NamespaceImpl &ns);
		void updateTagsPath(TagsMatcher &tm, const IndexExpressionEvaluator &ev);
		const UpdateEntry &details() const noexcept { return entry_; }
		const IndexedTagsPath &tagspath() const noexcept { return tagsPath_; }
		const IndexedTagsPath &tagspathWithLastIndex() const noexcept {
			return tagsPathWithLastIndex_ ? *tagsPathWithLastIndex_ : tagsPath_;
		}
		int arrayIndex() const noexcept { return arrayIndex_; }
		int index() const noexcept { return fieldIndex_; }
		bool isIndex() const noexcept { return isIndex_; }
		const std::string &name() const noexcept { return entry_.Column(); }

	private:
		const UpdateEntry &entry_;
		IndexedTagsPath tagsPath_;
		std::optional<IndexedTagsPath> tagsPathWithLastIndex_;
		int fieldIndex_{IndexValueType::SetByJsonPath};
		int arrayIndex_;
		bool isIndex_;
	};
	struct CJsonCache {
		CJsonCache() = default;
		void Assign(std::string_view str) {
			if (data_.capacity() < DefaultCJsonSize) {
				data_.reserve(DefaultCJsonSize);
			}
			data_.assign(str.begin(), str.end());
			cjson_ = std::string_view(&data_[0], str.size());
		}
		p_string Get() const { return p_string(&cjson_); }
		void Reset() { cjson_ = {nullptr, 0}; }
		int Size() const { return cjson_.length(); }

	private:
		enum { DefaultCJsonSize = 4096 };
		std::vector<char> data_;
		std::string_view cjson_;
	};

	void modifyField(IdType itemId, FieldData &field, Payload &pl, VariantArray &values);
	void modifyCJSON(IdType itemId, FieldData &field, VariantArray &values);
	void modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl);

	NamespaceImpl &ns_;
	const std::vector<UpdateEntry> &updateEntries_;
	std::vector<FieldData> fieldsToModify_;
	CJsonCache cjsonCache_;

	class RollBack_ModifiedPayload;

	class IndexRollBack {
	public:
		IndexRollBack(int indexCount) { data_.resize(indexCount); }
		void Reset(PayloadValue &pv) {
			pvSave_ = pv;
			pvSave_.Clone();
			std::fill(data_.begin(), data_.end(), false);
			cjsonChanged_ = false;
			pkModified_ = false;
		}
		void IndexChanged(size_t index, bool isPk) noexcept {
			data_[index] = true;
			pkModified_ = pkModified_ || isPk;
		}
		void IndexAndCJsonChanged(size_t index, bool isPk) noexcept {
			data_[index] = true;
			cjsonChanged_ = true;
			pkModified_ = pkModified_ || isPk;
		}
		void CjsonChanged() noexcept { cjsonChanged_ = true; }
		PayloadValue &GetPayloadValueBackup() noexcept { return pvSave_; }
		const std::vector<bool> &IndexStatus() const noexcept { return data_; }
		bool IsCjsonChanged() const noexcept { return cjsonChanged_; }
		bool IsPkModified() const noexcept { return pkModified_; }

	private:
		std::vector<bool> data_;
		PayloadValue pvSave_;
		bool cjsonChanged_ = false;
		bool pkModified_ = false;
	};

	IndexRollBack rollBackIndexData_;
};

}  // namespace reindexer
