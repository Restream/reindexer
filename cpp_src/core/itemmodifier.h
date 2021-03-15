#pragma once

#include "core/payload/payloadiface.h"
#include "core/query/query.h"
#include "estl/h_vector.h"

namespace reindexer {

struct NsContext;
class NamespaceImpl;

class ItemModifier {
public:
	ItemModifier(const h_vector<UpdateEntry, 0> &, NamespaceImpl &ns);
	ItemModifier(const ItemModifier &) = delete;
	ItemModifier &operator=(const ItemModifier &) = delete;
	ItemModifier(ItemModifier &&) = delete;
	ItemModifier &operator=(ItemModifier &&) = delete;

	void Modify(IdType itemId, const NsContext &ctx, bool updateWithJson);

private:
	struct FieldData {
		FieldData(const UpdateEntry &entry, NamespaceImpl &ns);
		void updateTagsPath(TagsMatcher &tm, const IndexExpressionEvaluator &ev);
		const UpdateEntry &details() const noexcept { return entry_; }
		const IndexedTagsPath &tagspath() const noexcept { return tagsPath_; }
		int arrayIndex() const noexcept { return arrayIndex_; }
		int index() const noexcept { return fieldIndex_; }
		bool isIndex() const noexcept { return isIndex_; }
		const string &name() const noexcept { return entry_.column; }
		const string &jsonpath() const noexcept { return jsonPath_; }

	private:
		const UpdateEntry &entry_;
		IndexedTagsPath tagsPath_;
		string jsonPath_;
		int fieldIndex_;
		int arrayIndex_;
		bool isIndex_;
	};

	void modify(IdType itemId);
	void modifyCJSON(IdType itemId, const NsContext &ctx);
	void modifyField(IdType itemId, FieldData &field, Payload &pl);
	void modifyIndexValues(IdType itemId, const FieldData &field, VariantArray &values, Payload &pl);

	NamespaceImpl &ns_;
	const h_vector<UpdateEntry, 0> &updateEntries_;
	vector<FieldData> fieldsToModify_;
};

}  // namespace reindexer
