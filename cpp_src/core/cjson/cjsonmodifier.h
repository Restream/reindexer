#pragma once

#include "core/cjson/tagspath.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"

namespace reindexer {

class TagsMatcher;

class CJsonModifier {
public:
	CJsonModifier(TagsMatcher& tagsMatcher, PayloadType pt) noexcept : pt_(std::move(pt)), tagsMatcher_(tagsMatcher) {}
	void SetFieldValue(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
					   const Payload& pl);
	void SetObject(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser, const Payload& pl);
	void RemoveField(std::string_view tuple, const IndexedTagsPath& fieldPath, WrSerializer& wrser);

private:
	class Context;
	Context initState(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
					  const Payload* pl, FieldModifyMode mode);
	bool updateFieldInTuple(Context& ctx);
	bool dropFieldInTuple(Context& ctx);
	bool buildCJSON(Context& ctx);
	[[nodiscard]] bool needToInsertField(const Context& ctx) const;
	void insertField(Context& ctx) const;
	void embedFieldValue(TagType, int field, Context& ctx, size_t idx) const;
	void updateObject(Context& ctx, int tagName) const;
	void setArray(Context& ctx) const;
	void writeCTag(const ctag& tag, Context& ctx);
	void updateArray(TagType atagType, uint32_t count, int tagName, Context& ctx);
	void copyArray(int TagName, Context& ctx);
	[[nodiscard]] TagType determineUpdateTagType(const Context& ctx, int field) const;
	[[nodiscard]] bool checkIfFoundTag(Context& ctx, bool isLastItem = false) const;
	[[nodiscard]] bool isIndexed(int field) const noexcept { return (field >= 0); }

	PayloadType pt_;
	IndexedTagsPath fieldPath_, tagsPath_;
	TagsMatcher& tagsMatcher_;
};

}  // namespace reindexer
