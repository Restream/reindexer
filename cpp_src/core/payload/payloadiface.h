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
class StringsHolder;

template <typename T>
class PayloadIface {
	template <typename U>
	friend class PayloadIface;

public:
	PayloadIface(const PayloadType &t, T &v) noexcept : t_(*t.get()), v_(&v) {}
	PayloadIface(const PayloadTypeImpl &t, T &v) noexcept : t_(t), v_(&v) {}

	void Reset() noexcept { memset(v_->Ptr(), 0, t_.TotalSize()); }
	// Get element(s) by field index
	void Get(int field, VariantArray &, Variant::hold_t) const;
	void Get(int field, VariantArray &) const;
	// Get element by field and array index
	[[nodiscard]] Variant Get(int field, int idx, Variant::hold_t) const;
	[[nodiscard]] Variant Get(int field, int idx) const;

	// Get array as span of typed elements
	template <typename Elem>
	span<Elem> GetArray(int field) & {
		assertrx(field < Type().NumFields());
		assertrx(Type().Field(field).IsArray());
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		return span<Elem>(reinterpret_cast<Elem *>(v_->Ptr() + arr->offset), arr->len);
	}
	// Get array len
	int GetArrayLen(int field) const {
		assertrx(field < Type().NumFields());
		assertrx(Type().Field(field).IsArray());
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		return arr->len;
	}

	// Resize array (grow)
	// return index of 1-st position
	// template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
	int ResizeArray(int field, int grow, bool append);

	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, const VariantArray &keys, bool append = false) {
		if (!t_.Field(field).IsArray() && keys.size() >= 1) {
			Field(field).Set(keys[0]);
		} else {
			setArray(field, keys, append);
		}
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, const Variant &key, bool append = false) {
		if (t_.Field(field).IsArray()) {
			Set(field, VariantArray{key}, append);
			return;
		}
		Field(field).Set(key);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, Variant &&key, bool append = false) {
		if (t_.Field(field).IsArray()) {
			Set(field, VariantArray{std::move(key)}, append);
			return;
		}
		Field(field).Set(std::move(key));
	}

	// Set non-array element by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void SetSingleElement(int field, const Variant &key);

	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(std::string_view field, const VariantArray &keys, bool append = false) {
		return Set(t_.FieldByName(field), keys, append);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(std::string_view field, const Variant &key, bool append = false) {
		return Set(t_.FieldByName(field), key, append);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(std::string_view field, Variant &&key, bool append = false) {
		return Set(t_.FieldByName(field), std::move(key), append);
	}

	// Set element or array by field index and element index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void Set(int field, int idx, const Variant &v);

	// Copies current payload value to a new one
	// according to PayloadType format
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyTo(PayloadType t, bool newFields = true);

	// Get element(s) by field name
	void Get(std::string_view field, VariantArray &, Variant::hold_t) const;
	void Get(std::string_view field, VariantArray &) const;

	// Get element(s) by json path
	void GetByJsonPath(std::string_view jsonPath, TagsMatcher &tagsMatcher, VariantArray &, KeyValueType expectedType) const;
	void GetByJsonPath(const TagsPath &jsonPath, VariantArray &, KeyValueType expectedType) const;
	void GetByJsonPath(const IndexedTagsPath &jsonPath, VariantArray &, KeyValueType expectedType) const;
	void GetByFieldsSet(const FieldsSet &, VariantArray &, KeyValueType expectedType,
						const std::vector<KeyValueType> &expectedCompositeTypes) const;
	[[nodiscard]] Variant GetComposite(const FieldsSet &, const std::vector<KeyValueType> &expectedTypes) const;
	VariantArray GetIndexedArrayData(const IndexedTagsPath &jsonPath, int field, int &offset, int &size) const;

	// Get fields count
	int NumFields() const noexcept { return t_.NumFields(); }

	// Real size of payload with arrays
	size_t RealSize() const;

	inline const uint8_t *Ptr() const noexcept { return v_->Ptr(); }
	const PayloadTypeImpl &Type() const noexcept { return t_; }
	const T *Value() const noexcept { return v_; }

	// Serialize field values
	void SerializeFields(WrSerializer &ser, const FieldsSet &fields) const;

	// Get hash by fields mask
	size_t GetHash(const FieldsSet &fields) const;
	// Compare is EQ by field mask
	bool IsEQ(const T &other, const FieldsSet &fields) const;
	// Get hash of all document
	uint64_t GetHash() const noexcept;

	// Compare 2 objects by field mask
	template <WithString>
	int Compare(const T &other, const FieldsSet &fields, const CollateOpts &collateOpts = CollateOpts()) const;
	template <WithString>
	int Compare(const T &other, const FieldsSet &fields, size_t &firstDifferentFieldIdx,
				const h_vector<const CollateOpts *, 1> &collateOpts) const;
	template <WithString>
	int Compare(const PayloadIface<const T> &other, std::string_view field, int fieldIdx, const CollateOpts &collateOpts, TagsMatcher &ltm,
				TagsMatcher &rtm, bool lForceByJsonPath, bool rForceByJsonPath) const;

	// Get PayloadFieldValue by field index
	PayloadFieldValue Field(int field) const noexcept { return PayloadFieldValue(t_.Field(field), v_->Ptr() + t_.Field(field).Offset()); }

	// Add refs to strings - make payload value complete self holding
	void AddRefStrings() noexcept;
	void AddRefStrings(int field) noexcept;
	// Release strings
	void ReleaseStrings() noexcept;
	void ReleaseStrings(int field) noexcept;
	void MoveStrings(int field, StringsHolder &dest);
	void CopyStrings(std::vector<key_string> &dest);

	// Item values' string for printing
	std::string Dump() const;
	// Item as JSON
	std::string GetJSON(const TagsMatcher &tm);
	void GetJSON(const TagsMatcher &tm, WrSerializer &ser);

private:
	enum class HoldPolicy : bool { Hold, NoHold };
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyWithNewOrUpdatedFields(PayloadType t);

	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	T CopyWithRemovedFields(PayloadType t);
	template <typename StrHolder>
	void copyOrMoveStrings(int field, StrHolder &dest, bool copy);
	template <typename P>
	void getByJsonPath(const P &path, VariantArray &, KeyValueType expectedType) const;
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type * = nullptr>
	void setArray(int field, const VariantArray &keys, bool append);
	template <typename HoldT>
	void get(int field, VariantArray &, HoldT h) const;
	template <typename HoldT>
	[[nodiscard]] Variant get(int field, int idx, HoldT h) const;
	template <typename HoldT>
	void get(std::string_view field, VariantArray &, HoldT h) const;

	// Array of elements types , not owning
	const PayloadTypeImpl &t_;
	// Data of elements, not owning
	T *v_;
};

template <>
int PayloadIface<PayloadValue>::ResizeArray(int, int, bool);
template <>
int PayloadIface<const PayloadValue>::ResizeArray(int, int, bool) = delete;

template <>
void PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher &, WrSerializer &);
template <>
std::string PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher &);
template <>
void PayloadIface<PayloadValue>::GetJSON(const TagsMatcher &, WrSerializer &) = delete;
template <>
std::string PayloadIface<PayloadValue>::GetJSON(const TagsMatcher &) = delete;

extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(std::string_view, VariantArray const &, bool);
extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, VariantArray const &, bool);
extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, int, const Variant &);
extern template void PayloadIface<PayloadValue>::SetSingleElement<PayloadValue, static_cast<void *>(0)>(int, const Variant &);

extern template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void *>(0)>(PayloadType t, bool newFields);
extern template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);
extern template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);

extern template int PayloadIface<PayloadValue>::Compare<WithString::Yes>(const PayloadValue &, const FieldsSet &,
																		 const CollateOpts &) const;
extern template int PayloadIface<PayloadValue>::Compare<WithString::No>(const PayloadValue &, const FieldsSet &, const CollateOpts &) const;
extern template int PayloadIface<const PayloadValue>::Compare<WithString::Yes>(const PayloadValue &, const FieldsSet &,
																			   const CollateOpts &) const;
extern template int PayloadIface<const PayloadValue>::Compare<WithString::No>(const PayloadValue &, const FieldsSet &,
																			  const CollateOpts &) const;

extern template class PayloadIface<PayloadValue>;
extern template class PayloadIface<const PayloadValue>;

using Payload = PayloadIface<PayloadValue>;
using ConstPayload = PayloadIface<const PayloadValue>;

}  // namespace reindexer
