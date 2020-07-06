#pragma once

#include "cjsontools.h"

namespace reindexer {

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt);
	Error SetFieldValue(string_view tuple, TagsPath path, const VariantArray &v, WrSerializer &ser);
	Error SetObject(string_view tuple, TagsPath path, const VariantArray &v, WrSerializer &ser, const Payload *pl);
	Error RemoveField(string_view tuple, TagsPath fieldPath, WrSerializer &wrser);

protected:
	struct Context;
	bool buildTuple(Context &ctx);
	bool buildCJSON(Context &ctx);
	void putNewField(Context &ctx);
	void updateObject(Context &ctx, int tagName);
	void updateField(Context &ctx, size_t idx);
	void copyValue(int type, int field, Context &ctx, size_t idx);
	bool checkIfPathCorrect(Context &ctx);
	int determineUpdateTagType(const Context &ctx, int field = -1);

	PayloadType pt_;
	TagsPath fieldPath_, tagsPath_;
	TagsMatcher &tagsMatcher_;
};
}  // namespace reindexer
