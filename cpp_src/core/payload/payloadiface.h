#pragma once

#include <span>
#include <type_traits>
#include "core/cjson/tagsmatcher.h"
#include "core/indexopts.h"
#include "core/keyvalue/variant.h"
#include "fieldsset.h"
#include "payloadfieldvalue.h"
#include "payloadtype.h"
#include "payloadvalue.h"

namespace reindexer {

class TagsMatcher;
class WrSerializer;
class StringsHolder;
class FieldsFilter;

template <typename T>
class PayloadIface {
	template <typename U>
	friend class PayloadIface;

public:
	PayloadIface(const PayloadType& t, T& v) noexcept : t_(*t.get()), v_(&v) {}
	PayloadIface(const PayloadTypeImpl& t, T& v) noexcept : t_(t), v_(&v) {}

	void Reset() noexcept { memset(v_->Ptr(), 0, t_.TotalSize()); }
	// Get element(s) by field index
	void Get(int field, VariantArray&, Variant::HoldT) const;
	void Get(int field, VariantArray&) const;
	// Get element by field and array index
	[[nodiscard]] Variant Get(int field, int idx, Variant::HoldT) const;
	[[nodiscard]] Variant Get(int field, int idx) const;

	// Get array as span of typed elements
	template <typename Elem>
	std::span<const Elem> GetArray(int field) const& {
		assertrx(field < Type().NumFields());
		assertrx(Type().Field(field).IsArray());
		auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
		return std::span<const Elem>(reinterpret_cast<const Elem*>(v_->Ptr() + arr->offset), arr->len);
	}
	// Get array len
	int GetArrayLen(int field) const {
		assertrx(field < Type().NumFields());
		assertrx(Type().Field(field).IsArray());
		auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
		return arr->len;
	}

	// Resize array (grow)
	// return index of 1-st position
	// template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
	int ResizeArray(int field, int grow, bool append);

	// Set element or array by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(int field, const VariantArray& keys, bool append = false) {
		if (t_.Field(field).IsArray()) {
			setArray(field, keys, append);
		} else {
			if (keys.empty() && t_.Field(field).Type().template Is<KeyValueType::FloatVector>()) {
				Field(field).Set(Variant{ConstFloatVectorView{}});
			} else {
				if (keys.size() != 1) {
					throw Error(errLogic, "Set array of {} size to not array field '{}'", keys.size(), t_.Field(field).Name());
				}
				Field(field).Set(keys[0]);
			}
		}
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(int field, const Variant& key, bool append = false) {
		if (t_.Field(field).IsArray()) {
			Set(field, VariantArray{key}, append);
			return;
		}
		Field(field).Set(key);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(int field, Variant&& key, bool append = false) {
		if (t_.Field(field).IsArray()) {
			Set(field, VariantArray{std::move(key)}, append);
			return;
		}
		Field(field).Set(std::move(key));
	}

	// Set non-array element by field index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void SetSingleElement(int field, const Variant& key);

	// Set element or array by index path
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(std::string_view field, const VariantArray& keys, bool append = false) {
		return Set(t_.FieldByName(field), keys, append);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(std::string_view field, const Variant& key, bool append = false) {
		return Set(t_.FieldByName(field), key, append);
	}
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(std::string_view field, Variant&& key, bool append = false) {
		return Set(t_.FieldByName(field), std::move(key), append);
	}

	// Set element or array by field index and element index
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void Set(int field, int idx, const Variant& v);

	// Copies current payload value to a new one
	// according to PayloadType format
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	T CopyTo(PayloadType t, bool newFields = true);

	// Get element(s) by field name
	void Get(std::string_view field, VariantArray&, Variant::HoldT) const;
	void Get(std::string_view field, VariantArray&) const;

	// Get element(s) by json path
	void GetByJsonPath(std::string_view jsonPath, TagsMatcher& tagsMatcher, VariantArray&, KeyValueType expectedType) const;
	void GetByJsonPath(const TagsPath& jsonPath, VariantArray&, KeyValueType expectedType) const;
	void GetByJsonPath(const IndexedTagsPath& jsonPath, VariantArray&, KeyValueType expectedType) const;
	void GetByFieldsSet(const FieldsSet&, VariantArray&, KeyValueType expectedType,
						const h_vector<KeyValueType, 4>& expectedCompositeTypes) const;
	[[nodiscard]] Variant GetComposite(const FieldsSet&, const h_vector<KeyValueType, 4>& expectedTypes) const;
	VariantArray GetIndexedArrayData(const IndexedTagsPath& jsonPath, int field, int& offset, int& size) const;

	// Get fields count
	int NumFields() const noexcept { return t_.NumFields(); }

	// Real size of payload with arrays
	size_t RealSize() const;

	inline const uint8_t* Ptr() const noexcept { return v_->Ptr(); }
	const PayloadTypeImpl& Type() const noexcept { return t_; }
	const T* Value() const noexcept { return v_; }

	// Serialize field values
	void SerializeFields(WrSerializer& ser, const FieldsSet& fields) const;

	// Get hash by fields mask
	size_t GetHash(const FieldsSet& fields) const;
	// Compare is EQ by field mask
	bool IsEQ(const T& other, const FieldsSet& fields) const;
	// Get hash of all document
	uint64_t GetHash(const std::function<uint64_t(unsigned int, ConstFloatVectorView)>& getVectorHashF) const noexcept;

	// Compare single field (indexed or non-indexed)
	template <WithString, NotComparable>
	ComparationResult CompareField(const T& other, int field, const FieldsSet& fields, size_t& tagPathIdx,
								   const CollateOpts& collateOpts) const;
	// Compare 2 objects by field mask
	template <WithString, NotComparable>
	ComparationResult Compare(const T& other, const FieldsSet& fields, const CollateOpts& collateOpts = CollateOpts()) const;
	template <WithString, NotComparable>
	ComparationResult Compare(const T& other, const FieldsSet& fields, size_t& firstDifferentFieldIdx,
							  const h_vector<const CollateOpts*, 1>& collateOpts) const;
	template <WithString, NotComparable>
	ComparationResult RelaxCompare(const PayloadIface<const T>& other, std::string_view field, int fieldIdx, const CollateOpts& collateOpts,
								   TagsMatcher& ltm, TagsMatcher& rtm, bool lForceByJsonPath, bool rForceByJsonPath) const;

	// Get PayloadFieldValue by field index
	PayloadFieldValue Field(int field) const noexcept { return PayloadFieldValue(t_.Field(field), v_->Ptr() + t_.Field(field).Offset()); }

	// Add refs to strings - make payload value complete self holding
	void AddRefStrings() noexcept;
	void AddRefStrings(int field) noexcept;
	// Release strings
	void ReleaseStrings() noexcept;
	void ReleaseStrings(int field) noexcept;
	void MoveStrings(int field, StringsHolder& dest);
	void CopyStrings(std::vector<key_string>& dest);
	void CopyStrings(h_vector<key_string, 16>& dest);

	// Item values' string for printing
	std::string Dump(const TagsMatcher*) const;
	// Item as JSON
	std::string GetJSON(const TagsMatcher& tm, const FieldsFilter&);
	void GetJSON(const TagsMatcher& tm, WrSerializer& ser, const FieldsFilter&);

private:
	enum class HoldPolicy : bool { Hold, NoHold };
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	T CopyWithNewOrUpdatedFields(PayloadType t);

	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	T CopyWithRemovedFields(PayloadType t);
	template <typename StrHolder>
	void copyOrMoveStrings(int field, StrHolder& dest, bool copy);
	template <typename P>
	void getByJsonPath(const P& path, VariantArray&, KeyValueType expectedType) const;
	template <typename U = T, typename std::enable_if<!std::is_const<U>::value>::type* = nullptr>
	void setArray(int field, const VariantArray& keys, bool append);
	template <typename HoldT>
	void get(int field, VariantArray&, HoldT h) const;
	template <typename HoldT>
	[[nodiscard]] Variant get(int field, int idx, HoldT h) const;
	template <typename HoldT>
	void get(std::string_view field, VariantArray&, HoldT h) const;

	// Array of elements types , not owning
	const PayloadTypeImpl& t_;
	// Data of elements, not owning
	T* v_;
};

template <>
int PayloadIface<PayloadValue>::ResizeArray(int, int, bool);
template <>
int PayloadIface<const PayloadValue>::ResizeArray(int, int, bool) = delete;

template <>
void PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher&, WrSerializer&, const FieldsFilter&);
template <>
std::string PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher&, const FieldsFilter&);
template <>
void PayloadIface<PayloadValue>::GetJSON(const TagsMatcher&, WrSerializer&, const FieldsFilter&) = delete;
template <>
std::string PayloadIface<PayloadValue>::GetJSON(const TagsMatcher&, const FieldsFilter&) = delete;

extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(std::string_view, const VariantArray&, bool);
extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(int, const VariantArray&, bool);
extern template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(int, int, const Variant&);
extern template void PayloadIface<PayloadValue>::SetSingleElement<PayloadValue, static_cast<void*>(0)>(int, const Variant&);

extern template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void*>(0)>(PayloadType t, bool newFields);
extern template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void*>(0)>(PayloadType t);
extern template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void*>(0)>(PayloadType t);

extern template ComparationResult PayloadIface<PayloadValue>::Compare<WithString::Yes, NotComparable::Return>(const PayloadValue&,
																											  const FieldsSet&,
																											  const CollateOpts&) const;
extern template ComparationResult PayloadIface<PayloadValue>::Compare<WithString::No, NotComparable::Return>(const PayloadValue&,
																											 const FieldsSet&,
																											 const CollateOpts&) const;
extern template ComparationResult PayloadIface<const PayloadValue>::Compare<WithString::Yes, NotComparable::Return>(
	const PayloadValue&, const FieldsSet&, const CollateOpts&) const;
extern template ComparationResult PayloadIface<const PayloadValue>::Compare<WithString::No, NotComparable::Return>(
	const PayloadValue&, const FieldsSet&, const CollateOpts&) const;
extern template ComparationResult PayloadIface<PayloadValue>::Compare<WithString::Yes, NotComparable::Throw>(const PayloadValue&,
																											 const FieldsSet&,
																											 const CollateOpts&) const;
extern template ComparationResult PayloadIface<PayloadValue>::Compare<WithString::No, NotComparable::Throw>(const PayloadValue&,
																											const FieldsSet&,
																											const CollateOpts&) const;
extern template ComparationResult PayloadIface<const PayloadValue>::Compare<WithString::Yes, NotComparable::Throw>(
	const PayloadValue&, const FieldsSet&, const CollateOpts&) const;
extern template ComparationResult PayloadIface<const PayloadValue>::Compare<WithString::No, NotComparable::Throw>(const PayloadValue&,
																												  const FieldsSet&,
																												  const CollateOpts&) const;

extern template ComparationResult PayloadIface<const PayloadValue>::Compare<WithString::No, NotComparable::Throw>(
	const PayloadValue&, const FieldsSet&, size_t&, const h_vector<const CollateOpts*, 1>&) const;

extern template ComparationResult PayloadIface<const PayloadValue>::CompareField<WithString::No, NotComparable::Throw>(
	const PayloadValue&, int, const FieldsSet&, size_t&, const CollateOpts&) const;

extern template ComparationResult PayloadIface<const PayloadValue>::RelaxCompare<WithString::Yes, NotComparable::Throw>(
	const PayloadIface<const PayloadValue>&, std::string_view, int, const CollateOpts&, TagsMatcher&, TagsMatcher&, bool, bool) const;
extern template ComparationResult PayloadIface<const PayloadValue>::RelaxCompare<WithString::No, NotComparable::Throw>(
	const PayloadIface<const PayloadValue>&, std::string_view, int, const CollateOpts&, TagsMatcher&, TagsMatcher&, bool, bool) const;

extern template class PayloadIface<PayloadValue>;
extern template class PayloadIface<const PayloadValue>;

using Payload = PayloadIface<PayloadValue>;
using ConstPayload = PayloadIface<const PayloadValue>;

}  // namespace reindexer
