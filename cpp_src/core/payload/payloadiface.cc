#include <stdlib.h>

#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/stringsholder.h"
#include "payloadiface.h"
#include "payloadvalue.h"

namespace reindexer {

// Get element(s) by field index
template <typename T>
void PayloadIface<T>::Get(int field, VariantArray &keys, Variant::hold_t h) const {
	get(field, keys, h);
}
template <typename T>
void PayloadIface<T>::Get(int field, VariantArray &keys) const {
	get(field, keys, Variant::no_hold_t{});
}

// Get element by field and array index
template <typename T>
Variant PayloadIface<T>::Get(int field, int idx, Variant::hold_t h) const {
	return get(field, idx, h);
}
template <typename T>
Variant PayloadIface<T>::Get(int field, int idx) const {
	return get(field, idx, Variant::no_hold_t{});
}

// Get element(s) by field name
template <typename T>
void PayloadIface<T>::Get(std::string_view field, VariantArray &kvs, Variant::hold_t h) const {
	get(t_.FieldByName(field), kvs, h);
}
template <typename T>
void PayloadIface<T>::Get(std::string_view field, VariantArray &kvs) const {
	get(t_.FieldByName(field), kvs, Variant::no_hold_t{});
}

template <typename T>
template <typename HoldT>
void PayloadIface<T>::get(int field, VariantArray &keys, HoldT h) const {
	assertrx(field < NumFields());
	keys.clear<false>();
	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		keys.reserve(arr->len);

		for (int i = 0; i < arr->len; i++) {
			PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			keys.push_back(pv.Get(h));
		}
	} else {
		keys.push_back(Field(field).Get(h));
	}
}

template <typename T>
template <typename HoldT>
Variant PayloadIface<T>::get(int field, int idx, HoldT h) const {
	assertrx(field < NumFields());

	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		assertf(idx < arr->len, "Field '%s.%s' bound exceed idx %d > len %d", Type().Name(), Type().Field(field).Name(), idx, arr->len);

		PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + idx * t_.Field(field).ElemSizeof());
		return pv.Get(h);
	} else {
		assertf(idx == 0, "Field '%s.%s' is not array, can't get idx %d", Type().Name(), Type().Field(field).Name(), idx);
		return Field(field).Get(h);
	}
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(std::string_view jsonPath, TagsMatcher &tagsMatcher, VariantArray &kvs,
									KeyValueType expectedType) const {
	VariantArray krefs;
	Get(0, krefs);
	std::string_view tuple(krefs[0]);
	if (tuple.length() == 0) {
		int fieldIdx = t_.FieldByJsonPath(jsonPath);
		if (fieldIdx == -1) {
			kvs.clear<false>();
			return;
		}
		if (t_.Field(fieldIdx).IsArray()) {
			IndexedTagsPath tagsPath = tagsMatcher.path2indexedtag(jsonPath, nullptr, false);
			if (tagsPath.back().IsWithIndex()) {
				kvs.clear<false>();
				kvs.emplace_back(Get(fieldIdx, tagsPath.back().Index()));
				return;
			}
		}
		return Get(fieldIdx, kvs);
	}
	GetByJsonPath(tagsMatcher.path2indexedtag(jsonPath, nullptr, false), kvs, expectedType);
}

template <typename T>
template <typename P>
void PayloadIface<T>::getByJsonPath(const P &path, VariantArray &krefs, KeyValueType expectedType) const {
	krefs.clear<false>();
	if (path.empty()) {
		return;
	}
	const FieldsSet filter{{path}};
	ConstPayload pl(t_, *v_);
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	FieldsExtractor extractor(&krefs, expectedType, path.size(), &filter);
	encoder.Encode(pl, extractor);
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(const TagsPath &tagsPath, VariantArray &krefs, KeyValueType expectedType) const {
	getByJsonPath(tagsPath, krefs, expectedType);
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(const IndexedTagsPath &tagsPath, VariantArray &krefs, KeyValueType expectedType) const {
	getByJsonPath(tagsPath, krefs, expectedType);
}

template <typename T>
void PayloadIface<T>::GetByFieldsSet(const FieldsSet &fields, VariantArray &kvs, KeyValueType expectedType,
									 const std::vector<KeyValueType> &expectedCompositeTypes) const {
	if (expectedType.Is<KeyValueType::Composite>()) {
		kvs.Clear();
		kvs.emplace_back(GetComposite(fields, expectedCompositeTypes));
	} else {
		assertrx_throw(fields.size() == 1);
		if (fields[0] == IndexValueType::SetByJsonPath) {
			assertrx_throw(fields.getTagsPathsLength() == 1);
			if (fields.isTagsPathIndexed(0)) {
				getByJsonPath(fields.getIndexedTagsPath(0), kvs, expectedType);
			} else {
				getByJsonPath(fields.getTagsPath(0), kvs, expectedType);
			}
		} else {
			Get(fields[0], kvs);
		}
	}
}

template <typename T>
Variant PayloadIface<T>::GetComposite(const FieldsSet &fields, const std::vector<KeyValueType> &expectedTypes) const {
	thread_local VariantArray buffer;
	buffer.clear<false>();
	assertrx_throw(fields.size() == expectedTypes.size());
	size_t jsonFieldIdx{0};
	[[maybe_unused]] const size_t maxJsonFieldIdx{fields.getTagsPathsLength()};
	VariantArray buf;
	for (size_t i = 0, s = fields.size(); i < s; ++i) {
		buf.clear<false>();
		if (fields[i] == IndexValueType::SetByJsonPath) {
			assertrx_throw(jsonFieldIdx < maxJsonFieldIdx);
			if (fields.isTagsPathIndexed(jsonFieldIdx)) {
				getByJsonPath(fields.getIndexedTagsPath(jsonFieldIdx), buf, expectedTypes[i]);
			} else {
				getByJsonPath(fields.getTagsPath(jsonFieldIdx), buf, expectedTypes[i]);
			}
			++jsonFieldIdx;
		} else {
			Get(fields[i], buf);
		}
		assertrx_throw(buf.size() == 1);
		buffer.emplace_back(std::move(buf[0]));
	}
	return Variant{buffer};
}

template <typename T>
VariantArray PayloadIface<T>::GetIndexedArrayData(const IndexedTagsPath &tagsPath, int field, int &offset, int &size) const {
	if (tagsPath.empty()) {
		throw Error(errParams, "GetIndexedArrayData(): tagsPath shouldn't be empty!");
	}
	if (field < 0 || field >= kMaxIndexes) {
		throw Error(errParams, "GetIndexedArrayData(): field must be a valid index number");
	}
	VariantArray values;
	FieldsSet filter({tagsPath});
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	offset = -1;
	size = -1;
	FieldsExtractor::FieldParams params{.index = offset, .length = size, .field = field};
	FieldsExtractor extractor(&values, KeyValueType::Undefined{}, tagsPath.size(), &filter, &params);

	ConstPayload pl(t_, *v_);
	encoder.Encode(pl, extractor);
	return values;
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::SetSingleElement(int field, const Variant &key) {
	if (t_.Field(field).IsArray()) {
		throw Error(errLogic, "Unable to set array field via single field setter");
	}
	Field(field).Set(key);
}

// Set element or array by field index and element index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(int field, int idx, const Variant &v) {
	assertrx(idx >= 0);
	assertrx(t_.Field(field).IsArray());
	auto const *const arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	const auto elemSize = t_.Field(field).ElemSizeof();
	assertrx(idx < arr->len);
	PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + idx * elemSize);
	pv.Set(v);
}

template <>
int PayloadIface<PayloadValue>::ResizeArray(int field, int count, bool append) {
	assertrx(t_.Field(field).IsArray());

	size_t realSize = RealSize();
	auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	auto elemSize = t_.Field(field).ElemSizeof();

	size_t grow = elemSize * count;
	size_t strip = 0;
	size_t insert = arr->offset ? (arr->offset + arr->len * elemSize) : realSize;
	if (!append) {
		strip = arr->len * elemSize;
		arr->len = 0;
	}

	assertrx(insert <= realSize);

	v_->Resize(realSize, realSize + grow - strip);
	memmove(v_->Ptr() + insert + grow - strip, v_->Ptr() + insert, realSize - insert);

	arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	if (!arr->offset) arr->offset = insert;

	arr->len += count;
	// Move another arrays, after our
	for (int f = 0; f < NumFields(); f++)
		if (f != field && t_.Field(f).IsArray()) {
			auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(f).p_);
			if (arr->offset >= insert) arr->offset += grow - strip;
		}
	return arr->len - count;
}

// Calc real size of payload with embeded arrays
template <typename T>
size_t PayloadIface<T>::RealSize() const {
	size_t sz = t_.TotalSize();
	for (int field = 0; field < NumFields(); field++)
		if (t_.Field(field).IsArray()) {
			auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
			if (arr->offset >= sz) sz = arr->offset + arr->len * t_.Field(field).ElemSizeof();
		}

	return sz;
}

// Serialize field values
template <typename T>
void PayloadIface<T>::SerializeFields(WrSerializer &ser, const FieldsSet &fields) const {
	size_t tagPathIdx = 0;
	VariantArray varr;
	for (int field : fields) {
		if (field == IndexValueType::SetByJsonPath) {
			assertrx(tagPathIdx < fields.getTagsPathsLength());
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath &tagsPath = fields.getIndexedTagsPath(tagPathIdx);
				GetByJsonPath(tagsPath, varr, KeyValueType::Undefined{});
			} else {
				const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx);
				GetByJsonPath(tagsPath, varr, KeyValueType::Undefined{});
			}
			if (varr.empty()) {
				throw Error(errParams, "PK serializing error: field [%s] cannot not be empty", fields.getJsonPath(tagPathIdx));
			}
			if (varr.size() > 1) {
				throw Error(errParams, "PK serializing error: field [%s] cannot not be array", fields.getJsonPath(tagPathIdx));
			}
			ser.PutVariant(varr[0]);
			++tagPathIdx;
		} else {
			ser.PutVariant(Field(field).Get());
		}
	}
	return;
}

template <typename T>
std::string PayloadIface<T>::Dump() const {
	std::string printString;
	for (int i = 0; i < NumFields(); ++i) {
		VariantArray fieldValues;
		Get(i, fieldValues);

		printString += Type().Field(i).Name();
		printString += ": ";

		if (Type().Field(i).IsArray()) printString += "[";
		for (size_t j = 0; j < fieldValues.size(); ++j) {
			auto &fieldValue = fieldValues[j];
			auto str = fieldValue.As<std::string>();
			if (i != 0)
				printString += str;
			else {
				for (size_t z = 0; z < str.length(); z++) printString += std::to_string(uint8_t(str[z])) + " ";
			}

			if (j != fieldValues.size() - 1) {
				printString += ", ";
			}
		}
		if (Type().Field(i).IsArray()) printString += "]";
		if (i != NumFields() - 1) printString += ", ";
	}
	return printString;
}

template <>
void PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher &tm, WrSerializer &ser) {
	JsonBuilder b(ser);
	JsonEncoder e(&tm);
	e.Encode(*this, b);
}

template <>
std::string PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher &tm) {
	WrSerializer ser;
	GetJSON(tm, ser);
	return std::string(ser.Slice());
}

// Get fields hash
template <typename T>
size_t PayloadIface<T>::GetHash(const FieldsSet &fields) const {
	size_t ret = 0;
	VariantArray keys1;
	size_t tagPathIdx = 0;
	for (auto field : fields) {
		ret *= 127;
		if (field != IndexValueType::SetByJsonPath) {
			auto &f = t_.Field(field);
			if (f.IsArray()) {
				auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
				ret ^= arr->len;
				uint8_t *p = v_->Ptr() + arr->offset;
				for (int i = 0; i < arr->len; i++, p += f.ElemSizeof()) {
					ret ^= PayloadFieldValue(f, p).Hash();
				}
			} else
				ret ^= Field(field).Hash();
		} else {
			assertrx(tagPathIdx < fields.getTagsPathsLength());
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath &tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
			} else {
				const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
			}
			ret ^= keys1.Hash();
		}
	}
	return ret;
}

// Get complete hash
template <typename T>
uint64_t PayloadIface<T>::GetHash() const noexcept {
	uint64_t ret = 0;

	for (int field = 0; field < t_.NumFields(); field++) {
		ret <<= 1;
		const auto &f = t_.Field(field);
		if (f.IsArray()) {
			auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
			ret ^= arr->len;
			uint8_t *p = v_->Ptr() + arr->offset;
			for (int i = 0; i < arr->len; i++, p += f.ElemSizeof()) {
				ret ^= PayloadFieldValue(f, p).Hash();
			}
		} else {
			ret ^= Field(field).Hash();
		}
	}
	return ret;
}

template <typename T>
bool PayloadIface<T>::IsEQ(const T &other, const FieldsSet &fields) const {
	size_t tagPathIdx = 0;
	PayloadIface<const T> o(t_, other);
	VariantArray keys1, keys2;
	for (auto field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			auto &f = t_.Field(field);
			if (f.IsArray()) {
				auto *arr1 = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
				auto *arr2 = reinterpret_cast<PayloadFieldValue::Array *>(o.Field(field).p_);
				if (arr1->len != arr2->len) return false;

				uint8_t *p1 = v_->Ptr() + arr1->offset;
				uint8_t *p2 = o.v_->Ptr() + arr2->offset;

				for (int i = 0; i < arr1->len; i++, p1 += f.ElemSizeof(), p2 += f.ElemSizeof()) {
					if (!PayloadFieldValue(f, p1).IsEQ(PayloadFieldValue(f, p2))) return false;
				}
			} else {
				if (!Field(field).IsEQ(o.Field(field))) return false;
			}
		} else {
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath &tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
				o.GetByJsonPath(tagsPath, keys2, KeyValueType::Undefined{});
			} else {
				const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
				o.GetByJsonPath(tagsPath, keys2, KeyValueType::Undefined{});
			}
			if (keys1 != keys2) {
				return false;
			}
		}
	}
	return true;
}

template <typename T>
template <WithString withString>
int PayloadIface<T>::Compare(const PayloadIface<const T> &other, std::string_view field, int fieldIdx, const CollateOpts &collateOpts,
							 TagsMatcher &ltm, TagsMatcher &rtm, bool lForceByJsonPath, bool rForceByJsonPath) const {
	VariantArray krefs1, krefs2;
	int cmpRes = 0;
	if (lForceByJsonPath || fieldIdx == IndexValueType::SetByJsonPath) {
		GetByJsonPath(field, ltm, krefs1, KeyValueType::Undefined{});
	} else {
		Get(fieldIdx, krefs1);
	}
	if (rForceByJsonPath || fieldIdx == IndexValueType::SetByJsonPath) {
		other.GetByJsonPath(field, rtm, krefs2, KeyValueType::Undefined{});
	} else {
		other.Get(fieldIdx, krefs2);
	}
	const size_t length = std::min(krefs1.size(), krefs2.size());
	for (size_t i = 0; i < length; ++i) {
		cmpRes = krefs1[i].RelaxCompare<withString>(krefs2[i], collateOpts);
		if (cmpRes) return cmpRes;
	}
	if (krefs1.size() == krefs2.size()) {
		return 0;
	} else if (krefs1.size() < krefs2.size()) {
		return -1;
	} else {
		return 1;
	}
}

template <typename T>
template <WithString withString>
int PayloadIface<T>::Compare(const T &other, const FieldsSet &fields, size_t &firstDifferentFieldIdx,
							 const h_vector<const CollateOpts *, 1> &collateOpts) const {
	size_t tagPathIdx = 0;
	VariantArray krefs1, krefs2;
	PayloadIface<const T> o(t_, other);

	bool commonOpts = (collateOpts.size() == 1);

	for (size_t i = 0; i < fields.size(); ++i) {
		const auto field(fields[i]);
		if (commonOpts) {
			assertrx(collateOpts.size());
		} else {
			assertrx(i < collateOpts.size());
		}
		const CollateOpts *opts(commonOpts ? collateOpts[0] : collateOpts[i]);
		if (field != IndexValueType::SetByJsonPath) {
			int cmpRes = Field(field).Get().Compare(o.Field(field).Get(), opts ? *opts : CollateOpts());
			if (cmpRes) {
				firstDifferentFieldIdx = i;
				return cmpRes;
			}
		} else {
			assertrx(tagPathIdx < fields.getTagsPathsLength());
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath &tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, krefs1, KeyValueType::Undefined{});
				o.GetByJsonPath(tagsPath, krefs2, KeyValueType::Undefined{});
			} else {
				const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, krefs1, KeyValueType::Undefined{});
				o.GetByJsonPath(tagsPath, krefs2, KeyValueType::Undefined{});
			}

			size_t length = std::min(krefs1.size(), krefs2.size());
			for (size_t j = 0; j < length; ++j) {
				int cmpRes = krefs1[j].RelaxCompare<withString>(krefs2[j], opts ? *opts : CollateOpts());
				if (cmpRes) {
					firstDifferentFieldIdx = i;
					return cmpRes;
				}
			}

			if (krefs1.size() < krefs2.size()) {
				firstDifferentFieldIdx = i;
				return -1;
			}
			if (krefs1.size() > krefs2.size()) {
				firstDifferentFieldIdx = i;
				return 1;
			}
		}
	}
	return 0;
}

template <typename T>
template <WithString withString>
int PayloadIface<T>::Compare(const T &other, const FieldsSet &fields, const CollateOpts &collateOpts) const {
	size_t firstDifferentFieldIdx = 0;
	return Compare<withString>(other, fields, firstDifferentFieldIdx, {&collateOpts});
}

template <typename T>
void PayloadIface<T>::AddRefStrings(int field) noexcept {
	auto &f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<const p_string *>((v_->Ptr() + f.Offset()));
		key_string_add_ref(const_cast<std::string *>(str.getCxxstr()));
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array *>(v_->Ptr() + f.Offset());
		for (int i = 0; i < arr->len; i++) {
			auto str = *reinterpret_cast<const p_string *>(v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			key_string_add_ref(const_cast<std::string *>(str.getCxxstr()));
		}
	}
}

template <typename T>
void PayloadIface<T>::AddRefStrings() noexcept {
	for (auto field : t_.StrFields()) AddRefStrings(field);
}

template <typename T>
void PayloadIface<T>::ReleaseStrings(int field) noexcept {
	auto &f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<p_string *>((v_->Ptr() + f.Offset()));
		key_string_release(const_cast<std::string *>(str.getCxxstr()));
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array *>(v_->Ptr() + f.Offset());
		for (int i = 0; i < arr->len; i++) {
			auto str = *reinterpret_cast<const p_string *>(v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			key_string_release(const_cast<std::string *>(str.getCxxstr()));
		}
	}
}

template <typename T>
template <typename StrHolder>
void PayloadIface<T>::copyOrMoveStrings(int field, StrHolder &dest, bool copy) {
	auto &f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<p_string *>((v_->Ptr() + f.Offset()));
		dest.emplace_back(reinterpret_cast<base_key_string *>(const_cast<std::string *>(str.getCxxstr())), copy);
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array *>(v_->Ptr() + f.Offset());
		for (int i = 0; i < arr->len; i++) {
			auto str = *reinterpret_cast<const p_string *>(v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			dest.emplace_back(reinterpret_cast<base_key_string *>(const_cast<std::string *>(str.getCxxstr())), copy);
		}
	}
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::setArray(int field, const VariantArray &keys, bool append) {
	if (keys.IsNullValue()) {
		ResizeArray(field, 0, append);
		return;
	}

	int pos = ResizeArray(field, keys.size(), append);
	auto const *const arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	const auto elemSize = t_.Field(field).ElemSizeof();

	for (const Variant &kv : keys) {
		PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + (pos++) * elemSize);
		pv.Set(kv);
	}
}

template <typename T>
void PayloadIface<T>::MoveStrings(int field, StringsHolder &dest) {
	copyOrMoveStrings(field, dest, false);
}

template <typename T>
void PayloadIface<T>::CopyStrings(std::vector<key_string> &dest) {
	for (auto field : t_.StrFields()) {
		copyOrMoveStrings(field, dest, true);
	}
}

template <typename T>
void PayloadIface<T>::ReleaseStrings() noexcept {
	for (auto field : t_.StrFields()) ReleaseStrings(field);
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
T PayloadIface<T>::CopyTo(PayloadType modifiedType, bool newOrUpdatedFields) {
	if (newOrUpdatedFields) {
		return CopyWithNewOrUpdatedFields(modifiedType);
	} else {
		return CopyWithRemovedFields(modifiedType);
	}
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
T PayloadIface<T>::CopyWithNewOrUpdatedFields(PayloadType modifiedType) {
	size_t totalGrow = 0;
	for (int idx = 1; idx < modifiedType.NumFields(); ++idx) {
		if (!t_.Contains(modifiedType.Field(idx).Name())) {
			const PayloadFieldType &fieldType = modifiedType.Field(idx);
			totalGrow += fieldType.IsArray() ? sizeof(PayloadFieldValue::Array) : fieldType.Sizeof();
		} else {
			if (modifiedType.Field(idx).IsArray() && !t_.Field(idx).IsArray()) {
				totalGrow += sizeof(PayloadFieldValue::Array) - t_.Field(idx).Sizeof();
			}
		}
	}

	T pv(RealSize() + totalGrow);
	PayloadIface<T> copyValueInterface(modifiedType, pv);
	for (int idx = 0; idx < t_.NumFields(); ++idx) {
		VariantArray kr;
		Get(idx, kr);
		copyValueInterface.Set(idx, kr, false);
	}

	return pv;
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
T PayloadIface<T>::CopyWithRemovedFields(PayloadType modifiedType) {
	size_t totalReduce = 0;
	std::vector<std::string> fieldsLeft;
	for (int idx = 0; idx < t_.NumFields(); ++idx) {
		const std::string &fieldname(t_.Field(idx).Name());
		if (modifiedType.Contains(fieldname)) {
			fieldsLeft.emplace_back(fieldname);
		} else {
			const PayloadFieldType &fieldType = t_.Field(idx);
			totalReduce += fieldType.IsArray() ? sizeof(PayloadFieldValue::Array) : fieldType.Sizeof();
		}
	}

	VariantArray kr;
	T pv(RealSize() - totalReduce);
	PayloadIface<T> copyValueInterface(modifiedType, pv);
	for (const auto &fieldname : fieldsLeft) {
		Get(fieldname, kr);
		copyValueInterface.Set(fieldname, kr, false);
	}

	return pv;
}

#ifdef _MSC_VER
#pragma warning(disable : 5037)
#endif

template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(std::string_view, VariantArray const &, bool);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, VariantArray const &, bool);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, int, const Variant &);
template void PayloadIface<PayloadValue>::SetSingleElement<PayloadValue, static_cast<void *>(0)>(int, const Variant &);

template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void *>(0)>(PayloadType t, bool newFields);
template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);
template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);

template int PayloadIface<PayloadValue>::Compare<WithString::Yes>(const PayloadValue &, const FieldsSet &, const CollateOpts &) const;
template int PayloadIface<PayloadValue>::Compare<WithString::No>(const PayloadValue &, const FieldsSet &, const CollateOpts &) const;
template int PayloadIface<const PayloadValue>::Compare<WithString::Yes>(const PayloadValue &, const FieldsSet &, const CollateOpts &) const;
template int PayloadIface<const PayloadValue>::Compare<WithString::No>(const PayloadValue &, const FieldsSet &, const CollateOpts &) const;
template int PayloadIface<const PayloadValue>::Compare<WithString::Yes>(const PayloadIface<const PayloadValue> &, std::string_view, int,
																		const CollateOpts &, TagsMatcher &, TagsMatcher &, bool,
																		bool) const;
template int PayloadIface<const PayloadValue>::Compare<WithString::No>(const PayloadIface<const PayloadValue> &, std::string_view, int,
																	   const CollateOpts &, TagsMatcher &, TagsMatcher &, bool, bool) const;

template class PayloadIface<PayloadValue>;
template class PayloadIface<const PayloadValue>;

}  // namespace reindexer
