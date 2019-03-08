#pragma once

#include "cjsontools.h"

namespace reindexer {

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt);
	Error SetFieldValue(const string_view &tuple, const TagsPath &fieldPath, const VariantArray &value, WrSerializer &wrser);

protected:
	struct Context {
		Context(const VariantArray &v, WrSerializer &ser, const string_view &tuple);
		const VariantArray &value;
		WrSerializer &wrser;
		Serializer rdser;
		TagsPath currObjPath;
		bool fieldUpdated = false;
	};

	bool buildTuple(Context &ctx);
	void putNewTags(Context &ctx);
	void replaceField(int tagType, Context &ctx, size_t idx);
	bool checkIfPathCorrect(Context &ctx);

	PayloadType pt_;
	TagsPath fieldPath_, tagsPath_;
	TagsMatcher &tagsMatcher_;
};
}  // namespace reindexer
