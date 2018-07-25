#include <stdlib.h>

#include "core/cjson/cjsonencoder.h"
#include "core/keyvalue/keyvalue.h"
#include "itoa/itoa.h"
#include "payloadiface.h"
#include "payloadtuple.h"
#include "payloadvalue.h"

using std::pair;

namespace reindexer {

template <typename T>
PayloadIface<T>::PayloadIface(const PayloadType &t, T &v) : t_(*t.get()), v_(&v) {}
template <typename T>
PayloadIface<T>::PayloadIface(const PayloadTypeImpl &t, T &v) : t_(t), v_(&v) {}

template <typename T>
KeyRefs &PayloadIface<T>::Get(int field, KeyRefs &keys) const {
	assert(field < NumFields());
	keys.resize(0);
	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		keys.reserve(arr->len);

		for (int i = 0; i < arr->len; i++) {
			PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			keys.push_back(pv.Get());
		}
	} else
		keys.push_back(Field(field).Get());
	return keys;
}

template <typename T>
KeyValues &PayloadIface<T>::Get(int field, KeyValues &keys) const {
	assert(field < NumFields());
	keys.resize(0);
	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
		keys.reserve(arr->len);

		for (int i = 0; i < arr->len; i++) {
			PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			keys.push_back(pv.Get());
		}
	} else
		keys.push_back(Field(field).Get());
	return keys;
}

// Get element(s) by field index
template <typename T>
KeyRefs &PayloadIface<T>::Get(const string &field, KeyRefs &kvs) const {
	return Get(t_.FieldByName(field), kvs);
}

// Get element(s) by field index
template <typename T>
KeyValues &PayloadIface<T>::Get(const string &field, KeyValues &kvs) const {
	return Get(t_.FieldByName(field), kvs);
}

template <typename T>
KeyRefs PayloadIface<T>::GetByJsonPath(const string &jsonPath, TagsMatcher &tagsMatcher, KeyRefs &kvs) const {
	KeyRefs krefs;
	Get(0, krefs);
	string_view tuple(krefs[0]);
	if (tuple.length() == 0) {
		int fieldIdx = t_.FieldByJsonPath(jsonPath);
		if (fieldIdx == -1) {
			kvs.clear();
			return kvs;
		};
		return Get(fieldIdx, kvs);
	}
	Payload pl(t_, const_cast<PayloadValue &>(*v_));
	CJsonEncoder encoder(tagsMatcher, JsonPrintFilter());

	kvs = encoder.ExtractFieldValue(&pl, jsonPath);
	return kvs;
}

template <typename T>
KeyRefs PayloadIface<T>::GetByJsonPath(const TagsPath &jsonPath, KeyRefs &krefs) const {
	TagsMatcher tagsMatcher;
	Payload pl(t_, const_cast<PayloadValue &>(*v_));
	CJsonEncoder encoder(tagsMatcher, JsonPrintFilter());
	krefs = encoder.ExtractFieldValue(&pl, jsonPath);
	return krefs;
}

template <typename T>
KeyValues PayloadIface<T>::GetByJsonPath(const string &jsonPath, TagsMatcher &tagsMatcher, KeyValues &kvs) const {
	kvs.clear();
	KeyRefs tupleData;
	Get(0, tupleData);
	string_view tuple(tupleData[0]);
	if (tuple.length() == 0) {
		int fieldIdx = t_.FieldByJsonPath(jsonPath);
		if (fieldIdx == -1) {
			return kvs;
		};
		return Get(fieldIdx, kvs);
	}

	Payload pl(t_, const_cast<PayloadValue &>(*v_));
	CJsonEncoder encoder(tagsMatcher, JsonPrintFilter());
	KeyRefs krefs = encoder.ExtractFieldValue(&pl, jsonPath);

	for (KeyRef &kref : krefs) kvs.push_back(std::move(kref));
	return kvs;
}

template <typename T>
KeyValues PayloadIface<T>::GetByJsonPath(const TagsPath &jsonPath, KeyValues &) const {
	TagsMatcher tagsMatcher;
	Payload pl(t_, const_cast<PayloadValue &>(*v_));
	CJsonEncoder encoder(tagsMatcher, JsonPrintFilter());
	KeyRefs krefs = encoder.ExtractFieldValue(&pl, jsonPath);

	KeyValues values;
	for (KeyRef &kref : krefs) values.push_back(std::move(kref));
	return values;
}

// Set element or array by field index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(const string &field, const KeyRefs &keys, bool append) {
	return Set(t_.FieldByName(field), keys, append);
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void PayloadIface<T>::Set(int field, const KeyRefs &keys, bool append) {
	if (!t_.Field(field).IsArray() && keys.size() >= 1) {
		Field(field).Set(keys[0]);
		return;
	}

	assert(t_.Field(field).IsArray());

	size_t realSize = RealSize();
	auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	auto elemSize = t_.Field(field).ElemSizeof();

	size_t grow = elemSize * keys.size();
	size_t strip = 0;
	size_t insert = arr->offset ? (arr->offset + arr->len * elemSize) : realSize;
	if (!append) {
		strip = arr->len * elemSize;
		arr->len = 0;
	}
	assert(insert <= realSize);

	v_->Resize(realSize, realSize + grow - strip);
	memmove(v_->Ptr() + insert + grow - strip, v_->Ptr() + insert, realSize - insert);

	arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(field).p_);
	if (!arr->offset) arr->offset = insert;

	for (auto &kv : keys) {
		PayloadFieldValue pv(t_.Field(field), v_->Ptr() + arr->offset + arr->len * elemSize);
		pv.Set(kv);
		arr->len++;
	}
	// Move another arrays, after our
	for (int f = 0; f < NumFields(); f++)
		if (f != field && t_.Field(f).IsArray()) {
			auto *arr = reinterpret_cast<PayloadFieldValue::Array *>(Field(f).p_);
			if (arr->offset >= insert) arr->offset += grow - strip;
		}
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

// Get primary key for elem
template <typename T>
string PayloadIface<T>::GetPK(const FieldsSet &pkFields) const {
	char buf[1024];
	GetPK(buf, sizeof(buf), pkFields);
	return string(buf);
}

// Get primary key for elem
template <typename T>
void PayloadIface<T>::GetPK(char *buf, size_t size, const FieldsSet &pkFields) const {
	*buf = 0;
	ptrdiff_t n = 0;
	for (int field = 0; field < NumFields() && n < static_cast<int>(size); ++field) {
		if (pkFields.contains(field)) {
			if (n) buf[n++] = '#';
			auto f = Field(field).Get();
			switch (f.Type()) {
				case KeyValueInt:
					n = i32toa(static_cast<int>(f), buf + n) - buf;
					break;
				case KeyValueInt64:
					n = i64toa(static_cast<int64_t>(f), buf + n) - buf;
					break;
				case KeyValueString: {
					p_string v(f);
					auto cnt = std::min(v.length(), size - n);
					memcpy(buf + n, static_cast<const char *>(v.data()), cnt);
					n += cnt;
					break;
				}
				default:
					assert(0);
			}
		}
	}
	buf[n] = 0;
	return;
}

template <typename T>
std::string PayloadIface<T>::Dump() {
	string printString;
	for (int i = 1; i < NumFields(); ++i) {
		KeyRefs fieldValues;
		Get(i, fieldValues);

		printString += Type().Field(i).Name();
		printString += ": ";

		for (size_t j = 0; j < fieldValues.size(); ++j) {
			auto &fieldValue = fieldValues[j];
			printString += fieldValue.As<string>();
			if (j != fieldValues.size() - 1) {
				printString += ", ";
			}
		}
		printString += "\n";
	}
	return printString;
}

// Get fields hash
template <typename T>
size_t PayloadIface<T>::GetHash(const FieldsSet &fields) const {
	size_t ret = 0;
	KeyRefs keys1;
	size_t tagPathIdx = 0;
	for (auto field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			keys1 = Get(field, keys1);
		} else {
			assert(tagPathIdx < fields.getTagsPathsLength());
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			keys1 = GetByJsonPath(tagsPath, keys1);
		}
		ret ^= keys1.Hash();
	}
	return ret;
}

template <typename T>
bool PayloadIface<T>::IsEQ(const T &other, const FieldsSet &fields) const {
	size_t tagPathIdx = 0;
	PayloadIface<const T> o(t_, other);
	KeyRefs keys1, keys2;
	for (auto field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			if (Get(field, keys1) != o.Get(field, keys2)) return false;
		} else {
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			GetByJsonPath(tagsPath, keys1);
			o.GetByJsonPath(tagsPath, keys2);
			if (GetByJsonPath(tagsPath, keys1) != o.GetByJsonPath(tagsPath, keys2)) return false;
		}
	}
	return true;
}

template <typename T>
bool PayloadIface<T>::IsEQ(const T &other) const {
	PayloadIface<const T> o(t_, other);
	KeyRefs keys1, keys2;
	for (int field = 0; field < NumFields(); ++field)
		if (Get(field, keys1) != o.Get(field, keys2)) return false;
	return true;
}

template <typename T>
int PayloadIface<T>::Compare(const T &other, const FieldsSet &fields, const CollateOpts &collateOpts) const {
	size_t tagPathIdx = 0;
	KeyRefs krefs1, krefs2;
	PayloadIface<const T> o(t_, other);
	for (const auto field : fields) {
		int cmpRes = 0;
		if (field != IndexValueType::SetByJsonPath) {
			cmpRes = Field(field).Get().Compare(o.Field(field).Get(), collateOpts);
		} else {
			assert(tagPathIdx < fields.getTagsPathsLength());
			const TagsPath &tagsPath = fields.getTagsPath(tagPathIdx++);
			krefs1 = GetByJsonPath(tagsPath, krefs1);
			krefs2 = o.GetByJsonPath(tagsPath, krefs2);
			cmpRes = (krefs1 == krefs2);
		}
		if (cmpRes > 0) return 1;
		if (cmpRes < 0) return -1;
	}
	return 0;
}

template <typename T>
void PayloadIface<T>::AddRefStrings() {
	for (auto field : t_.StrFields()) {
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
}

template <typename T>
void PayloadIface<T>::ReleaseStrings() {
	for (auto field : t_.StrFields()) {
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
		KeyRefs kr;
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

	KeyRefs kr;
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

template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(string const &, KeyRefs const &, bool);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void *>(0)>(int, KeyRefs const &, bool);

template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void *>(0)>(PayloadType t, bool newFields);
template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);
template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void *>(0)>(PayloadType t);

}  // namespace reindexer
