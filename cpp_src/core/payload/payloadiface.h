#pragma once

#include <type_traits>
#include "core/cjson/tagsmatcher.h"
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "estl/span.h"
#include "fieldsset.h"
#include "payloadfieldvalue.h"
#include "payloadtype.h"
#include "payloadvalue.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;

template <typename T>
class PayloadIface {
	template <typename U>
	friend class PayloadIface;

public:
	PayloadIface(const PayloadType &t, T &v);
	PayloadIface(const PayloadTypeImpl &t, T &v);

	void Reset() { memset(v_->Ptr(), 0, t_.TotalSize()); }
	// Get element(s) by field index
	VariantArray &Get(int field, VariantArray &, bool enableHold = false) const;
	// Get element by field and array index
	Variant Get(int field, int idx, bool enableHold = false) const;

	// Get array as span of typed elements
	template <typename Elem>
	span<Elem> GetArray(int field) {
		assert(field < Type().NumFields());
		assert(Type().Field(field).IsArray());
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		return span<Elem>(reinterpret_cast<Elem *>(v_->Ptr() + arr->offset), arr->len);
	}
	// Get array len
	int GetArrayLen(int field) const {
		assert(field < Type().NumFields());
		assert(Type().Field(field).IsArray());
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		return arr->len;
	}

	// Resize array (grow)
	// return index of 1-st position
	// template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
	int ResizeArray(int field, int grow, bool append);

	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, const VariantArray &keys, bool append = false);

	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(string_view field, const VariantArray &keys, bool append = false);

	// Set element or array by field index and element index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, int idx, const Variant &v);

	// Copies current payload value to a new one
	// according to PayloadType format
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyTo(PayloadType t, bool newFields = true);

	// Get element(s) by field index
	VariantArray &Get(string_view field, VariantArray &, bool enableHold = false) const;

	// Get element(s) by json path
	VariantArray GetByJsonPath(string_view jsonPath, TagsMatcher &tagsMatcher, VariantArray &, KeyValueType expectedType) const;
	VariantArray GetByJsonPath(const TagsPath &jsonPath, VariantArray &, KeyValueType expectedType) const;
	VariantArray GetByJsonPath(const IndexedTagsPath &jsonPath, VariantArray &, KeyValueType expectedType) const;
	VariantArray GetIndexedArrayData(const IndexedTagsPath &jsonPath, int &offset, int &size) const;

	// Get fields count
	int NumFields() const { return t_.NumFields(); }

	// Real size of payload with arrays
	size_t RealSize() const;

	inline const uint8_t *Ptr() const { return v_->Ptr(); }
	const PayloadTypeImpl &Type() const { return t_; }
	const T *Value() const { return v_; }

	// Serialize field values
	void SerializeFields(WrSerializer &ser, const FieldsSet &fields) const;

	// Get hash by fields mask
	size_t GetHash(const FieldsSet &fields) const;
	// Compare is EQ by field mask
	bool IsEQ(const T &other, const FieldsSet &fields) const;
	// Get hash of all document
	uint64_t GetHash() const;

	// Compare 2 objects by field mask
	int Compare(const T &other, const FieldsSet &fields, const CollateOpts &collateOpts = CollateOpts()) const;
	int Compare(const T &other, const FieldsSet &fields, size_t &firstDifferentFieldIdx,
				const h_vector<const CollateOpts *, 1> &collateOpts) const;

	// Get PayloadFieldValue by field index
	PayloadFieldValue Field(int field) const;

	// Add refs to strings - make payload value complete self holding
	void AddRefStrings();
	void AddRefStrings(int field);
	// Release strings
	void ReleaseStrings();
	void ReleaseStrings(int field);

	// Item values' string for printing
	std::string Dump() const;
	// Item as JSON
	std::string GetJSON(const TagsMatcher &tm);
	void GetJSON(const TagsMatcher &tm, WrSerializer &ser);

private:
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyWithNewOrUpdatedFields(PayloadType t);

	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyWithRemovedFields(PayloadType t);

protected:
	// Array of elements types , not owning
	const PayloadTypeImpl &t_;
	// Data of elements, not owning
	T *v_;
};

using Payload = PayloadIface<PayloadValue>;
using ConstPayload = PayloadIface<const PayloadValue>;

}  // namespace reindexer
