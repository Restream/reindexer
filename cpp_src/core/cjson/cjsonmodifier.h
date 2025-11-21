#pragma once

#include "core/cjson/tagspath.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"

namespace reindexer {

class TagsMatcher;
class FloatVectorsHolderVector;

class [[nodiscard]] CJsonModifier {
public:
	CJsonModifier(TagsMatcher& tagsMatcher, PayloadType pt) noexcept : pt_(std::move(pt)), tagsMatcher_(tagsMatcher) {}
	void SetFieldValue(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
					   const Payload& pl, FloatVectorsHolderVector&);
	void SetObject(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser, const Payload& pl,
				   FloatVectorsHolderVector&);
	void RemoveField(std::string_view tuple, const IndexedTagsPath& fieldPath, WrSerializer& wrser);

private:
	struct [[nodiscard]] UpdateTagType {
		explicit UpdateTagType(TagType raw) noexcept : rawType{raw} {}
		UpdateTagType(TagType raw, FloatVectorDimension dims) noexcept : rawType{raw}, isFloatVectorRef{true}, valueDims{dims} {}

		TagType rawType;
		bool isFloatVectorRef = false;
		FloatVectorDimension valueDims;
	};

	class Context;
	Context initState(std::string_view tuple, const IndexedTagsPath& fieldPath, const VariantArray& val, WrSerializer& ser,
					  const Payload* pl, FieldModifyMode mode, FloatVectorsHolderVector&);
	bool updateFieldInTuple(Context& ctx);
	bool dropFieldInTuple(Context&, JustCopy);
	bool buildCJSON(Context& ctx);
	bool needToInsertField(const Context& ctx) const;
	void insertField(Context& ctx) const;
	void embedFieldValue(TagType, int field, Context& ctx, size_t idx) const;
	void updateObject(Context&, TagName) const;
	void updateArrayItem(TagType arrayType, const Variant& newValue, Context&) const;
	void writeCTag(const ctag& tag, Context& ctx);
	void updateArray(TagType atagType, uint32_t count, TagName, Context&);
	void copyArray(TagName, Context&);
	UpdateTagType determineUpdateTagType(const Context& ctx, int field) const;
	bool checkIfFoundTag(Context& ctx, TagType tag, bool isLastItem = false) const;
	bool isIndexed(int field) const noexcept { return (field >= 0); }

	PayloadType pt_;
	IndexedTagsPath fieldPath_, tagsPath_;
	TagsMatcher& tagsMatcher_;
};

}  // namespace reindexer
