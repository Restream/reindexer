#pragma once

#include "cjsontools.h"

namespace reindexer {

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt);
	Error SetFieldValue(string_view tuple, const TagsPath &fieldPath, const VariantArray &value, WrSerializer &wrser);
	Error RemoveFieldValue(string_view tuple, const TagsPath &fieldPath, WrSerializer &wrser);

protected:
	struct Context {
		Context(const VariantArray &v, WrSerializer &ser, string_view tuple, FieldModifyMode mode);
		bool isModeSet() const { return mode == FieldModeSet; }
		bool isModeDrop() const { return mode == FieldModeDrop; }
		const VariantArray &value;
		WrSerializer &wrser;
		Serializer rdser;
		TagsPath currObjPath;
		bool fieldUpdated = false;
		FieldModifyMode mode;
	};

	bool buildTuple(Context &ctx);
	void putNewTags(Context &ctx);
	void updateField(Context &ctx, size_t idx);
	bool checkIfPathCorrect(Context &ctx);
	int determineUpdateTagType(const Context &ctx);

	PayloadType pt_;
	TagsPath fieldPath_, tagsPath_;
	TagsMatcher &tagsMatcher_;
};
}  // namespace reindexer
