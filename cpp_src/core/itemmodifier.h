#pragma once

#include <optional>
#include "core/keyvalue/p_string.h"
#include "core/namespace/float_vectors_indexes.h"
#include "core/payload/payloadiface.h"
#include "keyvalue/float_vectors_holder.h"
#include "updates/updaterecord.h"

namespace reindexer {

class NsContext;
class NamespaceImpl;
class UpdateEntry;

class [[nodiscard]] ItemModifier {
public:
	ItemModifier(const std::vector<UpdateEntry>&, NamespaceImpl& ns, UpdatesContainer& replUpdates, const NsContext& ctx);
	ItemModifier(const ItemModifier&) = delete;
	ItemModifier& operator=(const ItemModifier&) = delete;
	ItemModifier(ItemModifier&&) = delete;
	ItemModifier& operator=(ItemModifier&&) = delete;

	bool Modify(IdType itemId, const NsContext& ctx, UpdatesContainer& pendedRepl);
	PayloadValue& GetPayloadValueBackup() { return rollBackIndexData_.GetPayloadValueBackup(); }

private:
	using CompositeFlags = h_vector<bool, 32>;
	class [[nodiscard]] FieldData {
	public:
		FieldData(const UpdateEntry& entry, NamespaceImpl& ns, CompositeFlags& affectedComposites);
		const UpdateEntry& Details() const noexcept { return entry_; }
		const IndexedTagsPath& Tagspath() const noexcept { return tagsPath_; }
		const IndexedTagsPath& TagspathWithLastIndex() const noexcept {
			return tagsPathWithLastIndex_ ? *tagsPathWithLastIndex_ : tagsPath_;
		}
		std::optional<TagIndex> ArrayIndex() const noexcept { return arrayIndex_; }
		int Index() const noexcept { return fieldIndex_; }
		bool IsIndex() const noexcept { return isIndex_; }
		std::string_view Name() const noexcept;

	private:
		struct PathData {
			std::string jsonPath;
			TagsPath tagsPath;
			unsigned indexesCountInPath = 0;
			unsigned tailIndexesInPath = 0;
		};

		PathData preprocIndexedTagsPath(const NamespaceImpl& ns, const IndexedTagsPath& tp);
		bool initFromSimpleIndexName(std::string_view name, NamespaceImpl& ns);
		bool initFromSimpleJsonPath(std::string_view name, NamespaceImpl& ns);
		bool initFromIndexedIndexName(const NamespaceImpl& ns, const PathData& pathData, const IndexedTagsPath& indexedTagsPath);
		void initFromIndexedPath(std::string_view name, NamespaceImpl& ns);
		static TagsPath getTagsPathByIndexField(int& field, const NamespaceImpl& ns);

		void appendAffectedIndexes(const NamespaceImpl& ns, CompositeFlags& affectedComposites) const;

		const UpdateEntry& entry_;
		IndexedTagsPath tagsPath_;
		std::optional<IndexedTagsPath> tagsPathWithLastIndex_;
		int fieldIndex_{IndexValueType::SetByJsonPath};
		std::optional<TagIndex> arrayIndex_;
		bool isIndex_{false};
	};
	struct [[nodiscard]] CJsonCache {
		CJsonCache() = default;
		void Assign(std::string_view str) {
			if (data_.capacity() < DefaultCJsonSize) {
				data_.reserve(DefaultCJsonSize);
			}
			data_.assign(str.begin(), str.end());
			cjson_ = !str.empty() ? std::string_view(&data_[0], str.size()) : std::string_view();
		}
		p_string Get() const { return p_string(&cjson_); }
		void Reset() { cjson_ = {nullptr, 0}; }
		int Size() const { return cjson_.length(); }

	private:
		enum { DefaultCJsonSize = 4096 };
		std::vector<char> data_;
		std::string_view cjson_;
	};

	struct VectorIndex {
		int vecField;
		h_vector<int, 2> autoEmbeddingFields;
	};

	struct EmbeddingIndex {
		int autoEmbeddingField;
		h_vector<int, 2> vecFields;
	};

	class [[nodiscard]] EmbedderHelper {
	public:
		explicit EmbedderHelper(NamespaceImpl& ns) noexcept : ns_(ns) {}
		void Init(const std::vector<FieldData>& fieldsToModify) {
			prepareVectorIndexInfo();
			prepareSkipVectorIndexes(fieldsToModify);
		}
		const h_vector<int, 2>& GetVectorIndexForAutoEmbeddingField(int index) const {
			assertrx_throw(index < kMaxIndexes);
			assertrx_throw(index >= 0);
			for (const auto& k : autoEmbeddingIndexesList_) {
				if (k.autoEmbeddingField == index) {
					return k.vecFields;
				}
			}
			static const h_vector<int, 2> emptyVector{};
			return emptyVector;
		}
		bool TestSkipVectorIndex(int vectorIndex) const noexcept { return skipVectorIndexes_.test(vectorIndex); }
		const h_vector<int, 4>& GetVectorIndexesToModify() const noexcept { return vectorIndexesToModify_; }
		void RecalcVectorIndexesToModify(const Payload& pl, const ConstPayload& plOld);
		void AddVectorIndexToModify(const h_vector<int, 2>& indexes) {
			for (const auto ii : indexes) {
				if (std::find(vectorIndexesToModify_.begin(), vectorIndexesToModify_.end(), ii) == vectorIndexesToModify_.end()) {
					vectorIndexesToModify_.emplace_back(ii);
				}
			}
		}

		void AddVectorIndexToModify(int index) {
			if (std::find(vectorIndexesToModify_.begin(), vectorIndexesToModify_.end(), index) == vectorIndexesToModify_.end()) {
				vectorIndexesToModify_.emplace_back(index);
			}
		}

	private:
		void prepareVectorIndexInfo();
		void prepareSkipVectorIndexes(const std::vector<FieldData>& fieldsToModify);
		template <typename I>
		void addToIndex(int index, int vectorIndex, h_vector<I, 2>& indexContainer) {
			auto it = std::find_if(indexContainer.begin(), indexContainer.end(), [index](const auto& v) {
				auto& [key, val] = v;
				return key == index;
			});
			if (it == indexContainer.end()) {
				indexContainer.emplace_back(I{index, {vectorIndex}});
			} else {
				auto& [key, val] = *it;
				auto itVec = std::find(val.begin(), val.end(), vectorIndex);
				if (itVec == val.end()) {
					val.emplace_back(vectorIndex);
				}
			}
		}
		const h_vector<int, 2>& autoEmbeddingFieldsForVectorIndex(int field) const noexcept;

		h_vector<EmbeddingIndex, 2> autoEmbeddingIndexesList_;
		h_vector<VectorIndex, 2> vectorIndexesList_;

		std::bitset<kMaxIndexes> skipVectorIndexes_;

		h_vector<int, 4> vectorIndexesToModify_;

		NamespaceImpl& ns_;
	};

	void modifyField(IdType itemId, FieldData& field, Payload& pl, VariantArray& values);
	void modifyCJSON(IdType itemId, FieldData& field, VariantArray& values, UpdatesContainer& pendedRepl, const NsContext&);
	void modifyIndexValues(IdType itemId, const FieldData& field, VariantArray& values, Payload& pl);

	void deleteItemFromComposite(IdType itemId, auto& indexesCacheCleaner);
	void insertItemIntoComposite(IdType itemId, auto& indexesCacheCleaner);

	void getEmbeddingData(const Payload& pl, const UpsertEmbedder& embedder, std::vector<VariantArray>& data) const;
	std::vector<std::pair<int, std::vector<VariantArray>>> getEmbeddersSourceData(const Payload& pl) const;
	void updateEmbedding(IdType itemId, const RdxContext& rdxContext, Payload& pl);

	NamespaceImpl& ns_;
	const std::vector<UpdateEntry>& updateEntries_;
	std::vector<FieldData> fieldsToModify_;
	CJsonCache cjsonCache_;

	class RollBack_ModifiedPayload;

	class [[nodiscard]] IndexRollBack {
	public:
		IndexRollBack(int indexCount) { data_.resize(indexCount); }
		void Reset(IdType itemId, const PayloadType& pt, const PayloadValue& pv, FloatVectorsIndexes&& fvIndexes);
		void IndexChanged(size_t index, IsPk isPk) noexcept {
			data_[index] = true;
			pkModified_ = pkModified_ || isPk;
		}
		PayloadValue& GetPayloadValueBackup() noexcept { return pvSave_; }
		const std::vector<bool>& IndexStatus() const noexcept { return data_; }
		bool IsPkModified() const noexcept { return pkModified_; }

	private:
		std::vector<bool> data_;
		PayloadValue pvSave_;
		FloatVectorsHolderVector floatVectorsHolder_;
		bool pkModified_ = false;
	};

	IndexRollBack rollBackIndexData_;
	CompositeFlags affectedComposites_;
	const FloatVectorsIndexes vectorIndexes_;

	EmbedderHelper embedderHelper_;
};

}  // namespace reindexer
