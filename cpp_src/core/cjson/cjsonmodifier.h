#pragma once

#include "cjsontools.h"

namespace reindexer {

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt);
	void SetFieldValue(std::string_view tuple, IndexedTagsPath path, const VariantArray &v, WrSerializer &ser);
	void SetObject(std::string_view tuple, IndexedTagsPath path, const VariantArray &v, WrSerializer &ser, const Payload *pl);
	void RemoveField(std::string_view tuple, IndexedTagsPath fieldPath, WrSerializer &wrser);

protected:
	struct Context;
	bool updateFieldInTuple(Context &ctx);
	bool dropFieldInTuple(Context &ctx);
	bool buildCJSON(Context &ctx);
	bool needToInsertField(Context &ctx);
	void insertField(Context &ctx);
	void embedFieldValue(int type, int field, Context &ctx, size_t idx);
	void updateObject(Context &ctx, int tagName);
	void updateField(Context &ctx, size_t idx);
	int determineUpdateTagType(const Context &ctx, int field = IndexValueType::NotSet);
	bool checkIfFoundTag(Context &ctx, bool isLastItem = false);

	PayloadType pt_;
	IndexedTagsPath fieldPath_, tagsPath_;
	TagsMatcher &tagsMatcher_;
};
}  // namespace reindexer
