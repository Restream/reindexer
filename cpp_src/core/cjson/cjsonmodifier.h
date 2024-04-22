#pragma once

#include "core/cjson/tagspath.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"

namespace reindexer {

class TagsMatcher;

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher &tagsMatcher, PayloadType pt) noexcept : pt_(std::move(pt)), tagsMatcher_(tagsMatcher) {}
	void SetFieldValue(std::string_view tuple, IndexedTagsPath path, const VariantArray &v, WrSerializer &ser, const Payload &pl);
	void SetObject(std::string_view tuple, IndexedTagsPath path, const VariantArray &v, WrSerializer &ser, const Payload &pl);
	void RemoveField(std::string_view tuple, IndexedTagsPath fieldPath, WrSerializer &wrser);

private:
	class Context;
	bool updateFieldInTuple(Context &ctx);
	bool dropFieldInTuple(Context &ctx);
	bool buildCJSON(Context &ctx);
	bool needToInsertField(const Context &ctx);
	void insertField(Context &ctx);
	void embedFieldValue(TagType, int field, Context &ctx, size_t idx);
	void updateObject(Context &ctx, int tagName);
	void updateField(Context &ctx, size_t idx);
	TagType determineUpdateTagType(const Context &ctx, int field);
	bool checkIfFoundTag(Context &ctx, bool isLastItem = false);

	PayloadType pt_;
	IndexedTagsPath fieldPath_, tagsPath_;
	TagsMatcher &tagsMatcher_;
};

}  // namespace reindexer
