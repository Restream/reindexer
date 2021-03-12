#include <stdlib.h>

#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "itoa/itoa.h"
#include "payloadiface.h"
#include "payloadvalue.h"

using std::pair;

namespace reindexer {

template <typename T>
PayloadIface<T>::PayloadIface(const PayloadType &t, T &v) : t_(*t.get()), v_(&v) {}
template <typename T>
PayloadIface<T>::PayloadIface(const PayloadTypeImpl &t, T &v) : t_(t), v_(&v) {}

template <typename T>
VariantArray &PayloadIface<T>::Get(int field, VariantArray &keys, bool enableHold) const {
	assert(field < NumFields());
	keys.resize(0);
	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		keys.reserve(arr->len);

		for (int i = 0; i < arr->len; i++) {
			PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			keys.push_back(pv.Get(enableHold));
		}
	} else
		keys.push_back(Field(field).Get(enableHold));
	return keys;
}

template <typename T>
Variant PayloadIface<T>::Get(int field, int idx, bool enableHold) const {
	assert(field < NumFields());

	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		assertf(idx < arr->len, "Field '%s.%s' bound exceed idx %d > len %d", Type().Name(), Type().Field(field).Name(), idx, arr->len);

		PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + idx * t_.Field(field).ElemSizeof());
		return pv.Get(enableHold);

	} else {
		assertf(idx == 0, "Field '%s.%s' is not array, can't get idx %d", Type().Name(), Type().Field(field).Name(), idx);
		return Field(field).Get(enableHold);
	}
}

// Get element(s) by field index
template <typename T>
VariantArray &PayloadIface<T>::Get(string_view field, VariantArray &kvs, bool enableHold) const {
	return Get(t_.FieldByName(field), kvs, enableHold);
}

template <typename T>
VariantArray PayloadIface<T>::GetByJsonPath(string_view jsonPath, TagsMatcher &tagsMatcher, VariantArray &kvs,
											KeyValueType expectedType) const {
	VariantArray krefs;
	Get(0, krefs);
	string_view tuple(krefs[0]);
	if (tuple.length() == 0) {
		int fieldIdx = t_.FieldByJsonPath(jsonPath);
		if (fieldIdx == -1) {
			kvs.clear();
			return kvs;
		};
		if (t_.Field(fieldIdx).IsArray()) {
			IndexedTagsPath tagsPath = tagsMatcher.path2indexedtag(jsonPath, nullptr, false);
			if (tagsPath.back().IsWithIndex()) {
				return {Get(fieldIdx, tagsPath.back().Index())};
			}
		}
		return Get(fieldIdx, kvs);
	}
	VariantArray values = GetByJsonPath(tagsMatcher.path2indexedtag(jsonPath, nullptr, false), kvs, expectedType);
	return values;
}

template <typename T>
VariantArray PayloadIface<T>::GetByJsonPath(const TagsPath &jsonPath, VariantArray &krefs, KeyValueType expectedType) const {
	ConstPayload pl(t_, *v_);
	FieldsSet filter({jsonPath});
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	krefs.resize(0);
	if (!jsonPath.empty()) {
		FieldsExtractor extractor(&krefs, expectedType, jsonPath.size());
		encoder.Encode(&pl, extractor);
	}
	return krefs;
}

template <typename T>
VariantArray PayloadIface<T>::GetByJsonPath(const IndexedTagsPath &tagsPath, VariantArray &krefs, KeyValueType expectedType) const {
	ConstPayload pl(t_, *v_);
	FieldsSet filter({tagsPath});
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	krefs.resize(0);
	if (!tagsPath.empty()) {
		FieldsExtractor extractor(&krefs, expectedType, tagsPath.size(), &filter);
		encoder.Encode(&pl, extractor);
	}
	return krefs;
}

template <typename T>
VariantArray PayloadIface<T>::GetIndexedArrayData(const IndexedTagsPath &tagsPath, int &offset, int &size) const {
	if (tagsPath.empty()) {
		throw Error(errParams, "GetIndexedArrayData(): tagsPath shouldn't be empty!");
	}

	VariantArray values;
	FieldsSet filter({tagsPath});
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	FieldsExtractor extractor(&values, KeyValueUndefined, tagsPath.size(), &filter, &offset, &size);

	ConstPayload pl(t_, *v_);
	encoder.Encode(&pl, extractor);
	return values;
}

// Set element or array by field index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(string_view field, const VariantArray &keys, bool append) {
	return Set(t_.FieldByName(field), keys, append);
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(int field, const VariantArray &keys, bool append) {
	if (!t_.Field(field).IsArray() && keys.size() >= 1) {
		Field(field).Set(keys[0]);
		return;
	}

	if (keys.IsNullValue()) {
		ResizeArray(field, 0, append);
		return;
	}

	int pos = ResizeArray(field, keys.size(), append);
	auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	auto elemSize = t_.Field(field).ElemSizeof();

	for (auto &kv : keys) {
		PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + pos * elemSize);
		pv.Set(kv);
		pos++;
	}
}
// Set element or array by field index and element index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(int field, int idx, const Variant &v) {
	auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	auto elemSize = t_.Field(field).ElemSizeof();
	assert(t_.Field(field).IsArray());
	assert(idx >= 0 && idx < arr->len);

	PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + idx * elemSize);
	pv.Set(v);
}

template <typename T>
// template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
int PayloadIface<T>::ResizeArray(int field, int count, bool append) {
	assert(t_.Field(field).IsArray());

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

	assert(insert <= realSize);

	const_cast<PayloadValue *>(v_)->Resize(realSize, realSize + grow - strip);
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

template <typename T>
PayloadFieldValue PayloadIface<T>::Field(int field) const {
	return PayloadFieldValue(t_.Field(field), v_->Ptr() + t_.Field(field).Offset());
}

// Serialize field values
template <typename T>
void PayloadIface<T>::SerializeFields(WrSerializer &ser, const FieldsSet &fields) const {
	size_t tagPathIdx = 0;
	VariantArray varr;
	for (int field : fields) {
		if (field == IndexValueType::SetByJsonPath) {
			assert(tagPathIdx < fields.getTagsPathsLength());
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx);
			varr = GetByJsonPath(tagsPath, varr, KeyValueUndefined);
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
	string printString;
	for (int i = 0; i < NumFields(); ++i) {
		VariantArray fieldValues;
		Get(i, fieldValues);

		printString += Type().Field(i).Name();
		printString += ": ";

		if (Type().Field(i).IsArray()) printString += "[";
		for (size_t j = 0; j < fieldValues.size(); ++j) {
			auto &fieldValue = fieldValues[j];
			auto str = fieldValue.As<string>();
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
	e.Encode(this, b);
}

template <>
std::string PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher &tm) {
	WrSerializer ser;
	GetJSON(tm, ser);
	return string(ser.Slice());
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
			assert(tagPathIdx < fields.getTagsPathsLength());
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			ret ^= GetByJsonPath(tagsPath, keys1, KeyValueUndefined).Hash();
		}
	}
	return ret;
}

// Get complete hash
template <typename T>
uint64_t PayloadIface<T>::GetHash() const {
	uint64_t ret = 0;
	VariantArray keys1;

	for (int field = 0; field < t_.NumFields(); field++) {
		ret <<= 1;
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
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			if (GetByJsonPath(tagsPath, keys1, KeyValueUndefined) != o.GetByJsonPath(tagsPath, keys2, KeyValueUndefined)) return false;
		}
	}
	return true;
}

template <typename T>
int PayloadIface<T>::Compare(const T &other, const FieldsSet &fields, size_t &firstDifferentFieldIdx,
							 const h_vector<const CollateOpts *, 1> &collateOpts) const {
	size_t tagPathIdx = 0;
	VariantArray krefs1, krefs2;
	PayloadIface<const T> o(t_, other);

	bool commonOpts = (collateOpts.size() == 1);

	for (size_t i = 0; i < fields.size(); ++i) {
		int cmpRes = 0;
		const auto field(fields[i]);
		const CollateOpts *opts(commonOpts ? collateOpts[0] : collateOpts[i]);
		if (field != IndexValueType::SetByJsonPath) {
			cmpRes = Field(field).Get().Compare(o.Field(field).Get(), opts ? *opts : CollateOpts());
		} else {
			assert(tagPathIdx < fields.getTagsPathsLength());
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			krefs1 = GetByJsonPath(tagsPath, krefs1, KeyValueUndefined);
			krefs2 = o.GetByJsonPath(tagsPath, krefs2, KeyValueUndefined);

			size_t length = std::min(krefs1.size(), krefs2.size());
			for (size_t i = 0; i < length; ++i) {
				cmpRes = krefs1[i].RelaxCompare(krefs2[i], opts ? *opts : CollateOpts());
				if (cmpRes) break;
			}
			if (krefs1.size() < krefs2.size()) {
				cmpRes = -1;
			} else if (krefs1.size() > krefs2.size()) {
				cmpRes = 1;
			}
		}

		firstDifferentFieldIdx = i;

		if (cmpRes > 0) return 1;
		if (cmpRes < 0) return -1;
	}
	return 0;
}

template <typename T>
int PayloadIface<T>::Compare(const T &other, const FieldsSet &fields, const CollateOpts &collateOpts) const {
	size_t firstDifferentFieldIdx = 0;
	return Compare(other, fields, firstDifferentFieldIdx, {&collateOpts});
}

template <typename T>
void PayloadIface<T>::AddRefStrings(int field) {
	auto &f = t_.Field(field);
	assert(f.Type() == KeyValueString);

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<const p_string *>((v_->Ptr() + f.Offset()));
		key_string_add_ref(const_cast<string *>(str.getCxxstr()));
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array *>(v_->Ptr() + f.Offset());
		for (int i = 0; i < arr->len; i++) {
			auto str = *reinterpret_cast<const p_string *>(v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			key_string_add_ref(const_cast<string *>(str.getCxxstr()));
		}
	}
}

template <typename T>
void PayloadIface<T>::AddRefStrings() {
	for (auto field : t_.StrFields()) AddRefStrings(field);
}

template <typename T>
void PayloadIface<T>::ReleaseStrings(int field) {
	auto &f = t_.Field(field);
	assert(f.Type() == KeyValueString);

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<p_string *>((v_->Ptr() + f.Offset()));
		key_string_release(const_cast<string *>(str.getCxxstr()));
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array *>(v_->Ptr() + f.Offset());
		for (int i = 0; i < arr->len; i++) {
			auto str = *reinterpret_cast<const p_string *>(v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			key_string_release(const_cast<string *>(str.getCxxstr()));
		}
	}
}

template <typename T>
void PayloadIface<T>::ReleaseStrings() {
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
	std::vector<string> fieldsLeft;
	for (int idx = 0; idx < t_.NumFields(); ++idx) {
		const string &fieldname(t_.Field(idx).Name());
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
	for (size_t i = 0; i < fieldsLeft.size(); ++i) {
		const string &fieldname(fieldsLeft[i]);
		Get(fieldname, kr);
		copyValueInterface.Set(fieldname, kr, false);
	}

	return pv;
}

template class PayloadIface<PayloadValue>;
template class PayloadIface<const PayloadValue>;

#ifdef _MSC_VER
#pragma warning(disable : 5037)
#endif

template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(string_view, VariantArray const &, bool);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, VariantArray const &, bool);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, int, const Variant &);

template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void *>(0)>(PayloadType t, bool newFields);
template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);
template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);

}  // namespace reindexer
