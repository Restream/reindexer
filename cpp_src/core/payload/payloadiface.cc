#include <stdlib.h>

#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/multidimensional_array_checker.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/stringsholder.h"
#include "core/queryresults/fields_filter.h"
#include "payloadiface.h"
#include "payloadvalue.h"

namespace reindexer {

// Get element(s) by field index
template <typename T>
void PayloadIface<T>::Get(int field, VariantArray& keys, Variant::HoldT h) const {
	get(field, keys, h);
}
template <typename T>
void PayloadIface<T>::Get(int field, VariantArray& keys) const {
	get(field, keys, Variant::noHold);
}

// Get element by field and array index
template <typename T>
Variant PayloadIface<T>::Get(int field, int idx, Variant::HoldT h) const {
	return get(field, idx, h);
}
template <typename T>
Variant PayloadIface<T>::Get(int field, int idx) const {
	return get(field, idx, Variant::noHold);
}

// Get element(s) by field name
template <typename T>
void PayloadIface<T>::Get(std::string_view field, VariantArray& kvs, Variant::HoldT h) const {
	get(t_.FieldByName(field), kvs, h);
}
template <typename T>
void PayloadIface<T>::Get(std::string_view field, VariantArray& kvs) const {
	get(t_.FieldByName(field), kvs, Variant::noHold);
}

template <typename T>
template <typename HoldT>
void PayloadIface<T>::get(int field, VariantArray& keys, HoldT h) const {
	assertrx(field < NumFields());
	keys.Clear();
	const auto& fieldType = t_.Field(field);
	if (fieldType.IsArray()) {
		auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
		keys.reserve(arr->len);
		const auto elemSize = fieldType.ElemSizeof();
		const auto arrPtr = v_->Ptr() + arr->offset;

		for (int i = 0, len = arr->len; i < len; i++) {
			PayloadFieldValue pv(fieldType, arrPtr + i * elemSize);
			keys.push_back(pv.Get(h));
		}
		std::ignore = keys.MarkArray();
	} else {
		keys.push_back(Field(field).Get(h));
	}
}

template <typename T>
template <typename HoldT>
Variant PayloadIface<T>::get(int field, int idx, HoldT h) const {
	assertrx(field < NumFields());

	const auto& fieldType = t_.Field(field);
	if (fieldType.IsArray()) {
		auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
		assertf(idx < arr->len, "Field '{}.{}' bound exceed idx {} > len {}", Type().Name(), fieldType.Name(), idx, arr->len);

		PayloadFieldValue pv(fieldType, v_->Ptr() + arr->offset + idx * fieldType.ElemSizeof());
		return pv.Get(h);
	} else {
		assertf(idx == 0, "Field '{}.{}' is not array, can't get idx {}", Type().Name(), fieldType.Name(), idx);
		return Field(field).Get(h);
	}
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(std::string_view jsonPath, TagsMatcher& tagsMatcher, VariantArray& kvs,
									KeyValueType expectedType) const {
	VariantArray krefs;
	Get(0, krefs);
	std::string_view tuple(krefs[0]);
	if (tuple.length() == 0) {
		int fieldIdx = t_.FieldByJsonPath(jsonPath);
		if (fieldIdx == -1) {
			kvs.Clear();
			return;
		}
		if (t_.Field(fieldIdx).IsArray()) {
			IndexedTagsPath tagsPath = tagsMatcher.path2indexedtag(jsonPath, CanAddField_False);
			if (tagsPath.back().IsTagIndexNotAll()) {
				kvs.Clear();
				kvs.emplace_back(Get(fieldIdx, tagsPath.back().GetTagIndex().AsNumber()));
				std::ignore = kvs.MarkArray();
				return;
			}
		}
		return Get(fieldIdx, kvs);
	}
	GetByJsonPath(tagsMatcher.path2indexedtag(jsonPath, CanAddField_False), kvs, expectedType);
}

template <typename T>
template <typename P>
void PayloadIface<T>::getByJsonPath(const P& path, VariantArray& krefs, KeyValueType expectedType) const {
	krefs.Clear();
	if (path.empty()) {
		return;
	}
	const auto filter = FieldsFilter::FromPath(path);
	ConstPayload pl(t_, *v_);
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	FieldsExtractor extractor(&krefs, expectedType, path);
	encoder.Encode(pl, extractor);
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(const TagsPath& tagsPath, VariantArray& krefs, KeyValueType expectedType) const {
	getByJsonPath(tagsPath, krefs, expectedType);
}

template <typename T>
void PayloadIface<T>::GetByJsonPath(const IndexedTagsPath& tagsPath, VariantArray& krefs, KeyValueType expectedType) const {
	getByJsonPath(tagsPath, krefs, expectedType);
}

template <typename T>
void PayloadIface<T>::GetByFieldsSet(const FieldsSet& fields, VariantArray& kvs, KeyValueType expectedType,
									 const h_vector<KeyValueType, 4>& expectedCompositeTypes) const {
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
Variant PayloadIface<T>::GetComposite(const FieldsSet& fields, const h_vector<KeyValueType, 4>& expectedTypes) const {
	thread_local VariantArray buffer;
	buffer.Clear();
	assertrx_throw(fields.size() == expectedTypes.size());
	size_t jsonFieldIdx{0};
	[[maybe_unused]] const size_t maxJsonFieldIdx{fields.getTagsPathsLength()};
	VariantArray buf;
	for (size_t i = 0, s = fields.size(); i < s; ++i) {
		buf.Clear();
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
VariantArray PayloadIface<T>::GetIndexedArrayData(const IndexedTagsPath& tagsPath, int field, int& offset, int& size) const {
	if (tagsPath.empty()) {
		throw Error(errParams, "GetIndexedArrayData(): tagsPath shouldn't be empty!");
	}
	if (field < 0 || field >= kMaxIndexes) {
		throw Error(errParams, "GetIndexedArrayData(): field must be a valid index number");
	}
	VariantArray values;
	const auto filter = FieldsFilter::FromPath(tagsPath);
	BaseEncoder<FieldsExtractor> encoder(nullptr, &filter);
	offset = -1;
	size = -1;
	FieldsExtractor::FieldParams params{.index = offset, .length = size, .field = field};
	FieldsExtractor extractor(&values, KeyValueType::Undefined{}, tagsPath, &params);

	ConstPayload pl(t_, *v_);
	encoder.Encode(pl, extractor);
	return values;
}

template <typename T>
bool PayloadIface<T>::ContainsMultidimensionalArray(const FieldsFilter& fieldsFilter) const {
	BaseEncoder<MultidimensionalArrayChecker> encoder(nullptr, &fieldsFilter);
	MultidimensionalArrayChecker checker;
	ConstPayload pl(t_, *v_);
	encoder.Encode(pl, checker);
	return checker.Result();
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
void PayloadIface<T>::SetSingleElement(int field, const Variant& key) {
	if (t_.Field(field).IsArray()) {
		throw Error(errLogic, "Unable to set array field via single field setter");
	}
	Field(field).Set(key);
}

// Set element or array by field index and element index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
void PayloadIface<T>::Set(int field, int idx, const Variant& v) {
	assertrx(idx >= 0);
	const auto& fieldType = t_.Field(field);
	assertrx(fieldType.IsArray());
	const auto* const arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
	const auto elemSize = fieldType.ElemSizeof();
	assertrx(idx < arr->len);
	PayloadFieldValue pv(fieldType, v_->Ptr() + arr->offset + idx * elemSize);
	pv.Set(v);
}

template <>
int PayloadIface<PayloadValue>::ResizeArray(int field, int count, Append append) {
	const auto& fieldType = t_.Field(field);
	assertrx(fieldType.IsArray());

	const size_t realSize = RealSize();
	auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
	const auto elemSize = fieldType.ElemSizeof();

	const size_t grow = elemSize * count;
	size_t strip = 0;
	const size_t insert = arr->offset ? (arr->offset + arr->len * elemSize) : realSize;
	if (!append) {
		strip = arr->len * elemSize;
		arr->len = 0;
	}

	assertrx(insert <= realSize);

	v_->Resize(realSize, realSize + grow - strip);
	memmove(v_->Ptr() + insert + grow - strip, v_->Ptr() + insert, realSize - insert);

	arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
	if (!arr->offset) {
		arr->offset = insert;
	}

	arr->len += count;
	// Move another arrays, after our
	for (int f = 0; f < NumFields(); f++) {
		if (f != field && t_.Field(f).IsArray()) {
			auto* arrPtr = reinterpret_cast<PayloadFieldValue::Array*>(Field(f).p_);
			if (arrPtr->offset >= insert) {
				arrPtr->offset += grow - strip;
			}
		}
	}
	return arr->len - count;
}

// Calc real size of payload with embedded arrays
template <typename T>
size_t PayloadIface<T>::RealSize() const {
	size_t sz = t_.TotalSize();
	for (int field = 0, numFields = NumFields(); field < numFields; field++) {
		if (const auto& fieldType = t_.Field(field); fieldType.IsArray()) {
			auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
			if (arr->offset >= sz) {
				sz = arr->offset + arr->len * fieldType.ElemSizeof();
			}
		}
	}

	return sz;
}

// Serialize field values
template <typename T>
void PayloadIface<T>::SerializeFields(WrSerializer& ser, const FieldsSet& fields) const {
	size_t tagPathIdx = 0;
	VariantArray varr;
	for (int field : fields) {
		if (field == IndexValueType::SetByJsonPath) {
			assertrx(tagPathIdx < fields.getTagsPathsLength());
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath& tagsPath = fields.getIndexedTagsPath(tagPathIdx);
				GetByJsonPath(tagsPath, varr, KeyValueType::Undefined{});
			} else {
				const TagsPath& tagsPath = fields.getTagsPath(tagPathIdx);
				GetByJsonPath(tagsPath, varr, KeyValueType::Undefined{});
			}
			if (varr.empty()) {
				throw Error(errParams, "PK serializing error: field [{}] cannot not be empty", fields.getJsonPath(tagPathIdx));
			}
			if (varr.size() > 1) {
				throw Error(errParams, "PK serializing error: field [{}] cannot not be array", fields.getJsonPath(tagPathIdx));
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
std::string PayloadIface<T>::Dump(const TagsMatcher* tm) const {
	static constexpr uint32_t kMaxVectPrint = 3;
	std::string printString;
	if (tm) {
		VariantArray fieldValues;
		Get(0, fieldValues);
		std::ostringstream out;
		const std::string cj = fieldValues[0].As<std::string>();
		DumpCjson(Serializer(cj), out, this, tm);
		printString = out.str();
	}
	for (int i = 0; i < NumFields(); ++i) {
		VariantArray fieldValues;
		Get(i, fieldValues);

		printString += Type().Field(i).Name();
		printString += ": ";

		if (Type().Field(i).IsArray()) {
			printString += "[";
		}
		for (size_t j = 0; j < fieldValues.size(); ++j) {
			auto& fieldValue = fieldValues[j];
			std::string str;
			if (fieldValue.Type().Is<KeyValueType::FloatVector>()) {
				const ConstFloatVectorView vect{fieldValue};
				if (vect.IsEmpty()) {
					str = "<empty>";
				} else {
					const auto dim = uint32_t(vect.Dimension());
					if (vect.IsStripped()) {
						str = std::to_string(dim) + "[<stripped>]";
					} else {
						str = std::to_string(dim) + '[';
						for (uint32_t k = 0; k < std::min(kMaxVectPrint, dim); ++k) {
							if (k != 0) {
								str += ", ";
							}
							str += std::to_string(vect.Data()[k]);
						}
						if (dim < kMaxVectPrint) {
							str += ", ...";
						}
						str += ']';
					}
				}
			} else {
				str = fieldValue.As<std::string>();
			}
			if (i != 0) {
				printString += str;
			} else {
				for (size_t z = 0; z < str.length(); z++) {
					printString += std::to_string(uint8_t(str[z])) + " ";
				}
			}

			if (j != fieldValues.size() - 1) {
				printString += ", ";
			}
		}
		if (Type().Field(i).IsArray()) {
			printString += "]";
		}
		if (i != NumFields() - 1) {
			printString += ", ";
		}
	}
	return printString;
}

template <>
void PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher& tm, WrSerializer& ser, const FieldsFilter& fieldsFilter) {
	JsonBuilder b(ser);
	JsonEncoder e(&tm, &fieldsFilter);
	e.Encode(*this, b);
}

template <>
std::string PayloadIface<const PayloadValue>::GetJSON(const TagsMatcher& tm, const FieldsFilter& fieldsFilter) {
	WrSerializer ser;
	GetJSON(tm, ser, fieldsFilter);
	return std::string(ser.Slice());
}

// Get fields hash
template <typename T>
size_t PayloadIface<T>::GetHash(const FieldsSet& fields) const {
	size_t ret = 0;
	VariantArray keys1;
	size_t tagPathIdx = 0;
	for (auto field : fields) {
		ret *= 127;
		if (field != IndexValueType::SetByJsonPath) {
			auto& f = t_.Field(field);
			if (f.IsArray()) {
				auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
				ret ^= arr->len;
				uint8_t* p = v_->Ptr() + arr->offset;
				for (int i = 0; i < arr->len; i++, p += f.ElemSizeof()) {
					ret ^= PayloadFieldValue(f, p).Hash();
				}
			} else {
				ret ^= Field(field).Hash();
			}
		} else {
			assertrx(tagPathIdx < fields.getTagsPathsLength());
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath& tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
			} else {
				const TagsPath& tagsPath = fields.getTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
			}
			ret ^= keys1.Hash();
		}
	}
	return ret;
}

// Get complete hash
template <typename T>
uint64_t PayloadIface<T>::GetHash(const std::function<uint64_t(unsigned int, ConstFloatVectorView)>& getVectorHashF) const noexcept {
	uint64_t ret = 0;
	for (int field = 0, fields = t_.NumFields(); field < fields; ++field) {
		ret <<= 1;
		const auto& f = t_.Field(field);
		auto fv = Field(field);
		if (f.Type().IsSame(KeyValueType::FloatVector{})) {
			ret ^= getVectorHashF(unsigned(field), ConstFloatVectorView(fv.Get()));
		} else if (f.IsArray()) {
			auto* arr = reinterpret_cast<PayloadFieldValue::Array*>(fv.p_);
			ret ^= arr->len;
			uint8_t* p = v_->Ptr() + arr->offset;
			for (int i = 0; i < arr->len; i++, p += f.ElemSizeof()) {
				ret ^= PayloadFieldValue(f, p).Hash();
			}
		} else {
			ret ^= fv.Hash();
		}
	}
	return ret;
}

template <typename T>
bool PayloadIface<T>::IsEQ(const T& other, const FieldsSet& fields) const {
	size_t tagPathIdx = 0;
	PayloadIface<const T> o(t_, other);
	VariantArray keys1, keys2;
	for (auto field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			auto& f = t_.Field(field);
			if (f.IsArray()) {
				auto* arr1 = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
				auto* arr2 = reinterpret_cast<PayloadFieldValue::Array*>(o.Field(field).p_);
				if (arr1->len != arr2->len) {
					return false;
				}

				uint8_t* p1 = v_->Ptr() + arr1->offset;
				uint8_t* p2 = o.v_->Ptr() + arr2->offset;

				for (int i = 0; i < arr1->len; i++, p1 += f.ElemSizeof(), p2 += f.ElemSizeof()) {
					if (!PayloadFieldValue(f, p1).IsEQ(PayloadFieldValue(f, p2))) {
						return false;
					}
				}
			} else {
				if (!Field(field).IsEQ(o.Field(field))) {
					return false;
				}
			}
		} else {
			if (fields.isTagsPathIndexed(tagPathIdx)) {
				const IndexedTagsPath& tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
				GetByJsonPath(tagsPath, keys1, KeyValueType::Undefined{});
				o.GetByJsonPath(tagsPath, keys2, KeyValueType::Undefined{});
			} else {
				const TagsPath& tagsPath = fields.getTagsPath(tagPathIdx++);
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
template <WithString withString, NotComparable notComparable, NullsHandling nullsHandling>
ComparationResult PayloadIface<T>::CompareField(const T& other, int field, const FieldsSet& fields, size_t& tagPathIdx,
												const CollateOpts& collateOpts) const {
	PayloadIface<const T> o(t_, other);
	VariantArray krefs1, krefs2;
	if (field != IndexValueType::SetByJsonPath) {
		Get(field, krefs1);
		o.Get(field, krefs2);
		size_t length = std::min(krefs1.size(), krefs2.size());
		for (size_t j = 0; j < length; ++j) {
			const auto cmpRes = krefs1[j].Compare<notComparable, nullsHandling>(krefs2[j], collateOpts);
			if (cmpRes != ComparationResult::Eq) {
				return cmpRes;
			}
		}
	} else {
		auto postproc = [](VariantArray& va) {
			if (va.empty()) {
				va.Clear();
				va.emplace_back();	// Treat empty arrays and missing values as 'null'
			}
		};
		assertrx_throw(tagPathIdx < fields.getTagsPathsLength());
		if (fields.isTagsPathIndexed(tagPathIdx)) {
			const IndexedTagsPath& tagsPath = fields.getIndexedTagsPath(tagPathIdx++);
			GetByJsonPath(tagsPath, krefs1, KeyValueType::Undefined{});
			o.GetByJsonPath(tagsPath, krefs2, KeyValueType::Undefined{});
		} else {
			const TagsPath& tagsPath = fields.getTagsPath(tagPathIdx++);
			GetByJsonPath(tagsPath, krefs1, KeyValueType::Undefined{});
			o.GetByJsonPath(tagsPath, krefs2, KeyValueType::Undefined{});
		}
		postproc(krefs1);
		postproc(krefs2);
		size_t length = std::min(krefs1.size(), krefs2.size());
		for (size_t j = 0; j < length; ++j) {
			const auto cmpRes = krefs1[j].RelaxCompare<withString, notComparable, nullsHandling>(krefs2[j], collateOpts);
			if (cmpRes != ComparationResult::Eq) {
				return cmpRes;
			}
		}
	}

	if (krefs1.size() < krefs2.size()) {
		return ComparationResult::Lt;
	} else if (krefs1.size() > krefs2.size()) {
		return ComparationResult::Gt;
	}

	return ComparationResult::Eq;
}

template <typename T>
template <WithString withString, NotComparable notComparable, NullsHandling nullsHandling>
ComparationResult PayloadIface<T>::RelaxCompare(const PayloadIface<const T>& other, std::string_view field, int fieldIdx,
												const CollateOpts& collateOpts, TagsMatcher& ltm, TagsMatcher& rtm, bool lForceByJsonPath,
												bool rForceByJsonPath) const {
	VariantArray krefs1, krefs2;
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
		auto cmpRes = krefs1[i].RelaxCompare<withString, notComparable, nullsHandling>(krefs2[i], collateOpts);
		if (cmpRes != ComparationResult::Eq) {
			return cmpRes;
		}
	}
	if (krefs1.size() < krefs2.size()) {
		return ComparationResult::Lt;
	} else if (krefs1.size() > krefs2.size()) {
		return ComparationResult::Gt;
	}
	return ComparationResult::Eq;
}

template <typename T>
void PayloadIface<T>::AddRefStrings(int field) noexcept {
	auto& f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());
	auto vptr = v_->Ptr();

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<const p_string*>((vptr + f.Offset()));
		key_string_impl::addref_unsafe(str.getBaseKeyString());
	} else {
		const auto elemSize = f.ElemSizeof();
		auto arr = reinterpret_cast<PayloadFieldValue::Array*>(vptr + f.Offset());
		const auto arrOffset = arr->offset;
		for (int i = 0, arrLen = arr->len; i < arrLen; ++i) {
			auto str = reinterpret_cast<const p_string*>(vptr + arrOffset + i * elemSize);
			key_string_impl::addref_unsafe(str->getBaseKeyString());
		}
	}
}

template <typename T>
void PayloadIface<T>::AddRefStrings() noexcept {
	for (auto field : t_.StrFields()) {
		AddRefStrings(field);
	}
}

template <typename T>
void PayloadIface<T>::ReleaseStrings(int field) noexcept {
	auto& f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());
	auto vptr = v_->Ptr();

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = reinterpret_cast<p_string*>((vptr + f.Offset()));
		key_string_impl::release_unsafe(str->getBaseKeyString());
	} else {
		const auto elemSize = f.ElemSizeof();
		auto arr = reinterpret_cast<PayloadFieldValue::Array*>(vptr + f.Offset());
		const auto arrOffset = arr->offset;
		for (int i = 0, arrLen = arr->len; i < arrLen; ++i) {
			auto str = reinterpret_cast<const p_string*>(vptr + arrOffset + i * elemSize);
			key_string_impl::release_unsafe(str->getBaseKeyString());
		}
	}
}

template <typename T>
template <typename StrHolder>
void PayloadIface<T>::copyOrMoveStrings(int field, StrHolder& dest, bool copy) {
	auto& f = t_.Field(field);
	assertrx(f.Type().template Is<KeyValueType::String>());

	// direct payloadvalue manipulation for speed optimize
	if (!f.IsArray()) {
		auto str = *reinterpret_cast<p_string*>((v_->Ptr() + f.Offset()));
		dest.emplace_back(str.getBaseKeyString(), copy);
	} else {
		auto arr = reinterpret_cast<PayloadFieldValue::Array*>(v_->Ptr() + f.Offset());
		const auto arrPtr = v_->Ptr() + arr->offset;
		const auto elemSize = f.ElemSizeof();
		for (int i = 0, arrLen = arr->len; i < arrLen; i++) {
			auto str = *reinterpret_cast<const p_string*>(arrPtr + i * elemSize);
			dest.emplace_back(str.getBaseKeyString(), copy);
		}
	}
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
void PayloadIface<T>::setArray(int field, const VariantArray& keys, Append append) {
	if (keys.IsNullValue()) {
		std::ignore = ResizeArray(field, 0, append);
		return;
	}

	int pos = ResizeArray(field, keys.size(), append);
	const auto* const arr = reinterpret_cast<PayloadFieldValue::Array*>(Field(field).p_);
	auto& fieldType = t_.Field(field);
	const auto elemSize = fieldType.ElemSizeof();
	auto arrElemPtr = v_->Ptr() + arr->offset + pos * elemSize;
	for (const Variant& kv : keys) {
		PayloadFieldValue pv(fieldType, arrElemPtr);
		arrElemPtr += elemSize;
		pv.Set(kv);
	}
}

template <typename T>
void PayloadIface<T>::MoveStrings(int field, StringsHolder& dest) {
	copyOrMoveStrings(field, dest, false);
}

template <typename T>
void PayloadIface<T>::CopyStrings(std::vector<key_string>& dest) {
	for (int field : t_.StrFields()) {
		copyOrMoveStrings(field, dest, true);
	}
}

template <typename T>
void PayloadIface<T>::CopyStrings(h_vector<key_string, 16>& dest) {
	for (int field : t_.StrFields()) {
		copyOrMoveStrings(field, dest, true);
	}
}

template <typename T>
void PayloadIface<T>::ReleaseStrings() noexcept {
	for (int field : t_.StrFields()) {
		ReleaseStrings(field);
	}
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
T PayloadIface<T>::CopyTo(PayloadType modifiedType, bool newOrUpdatedFields) {
	if (newOrUpdatedFields) {
		return CopyWithNewOrUpdatedFields(std::move(modifiedType));
	} else {
		return CopyWithRemovedFields(std::move(modifiedType));
	}
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
T PayloadIface<T>::CopyWithNewOrUpdatedFields(PayloadType modifiedType) {
	size_t totalGrow = 0;
	for (int idx = 1, modNumFields = modifiedType.NumFields(); idx < modNumFields; ++idx) {
		const auto& modifiedFieldType = modifiedType.Field(idx);
		if (!t_.Contains(modifiedFieldType.Name())) {
			totalGrow += modifiedFieldType.IsArray() ? sizeof(PayloadFieldValue::Array) : modifiedFieldType.Sizeof();
		} else {
			if (modifiedFieldType.IsArray() && !t_.Field(idx).IsArray()) {
				totalGrow += sizeof(PayloadFieldValue::Array) - t_.Field(idx).Sizeof();
			}
		}
	}

	T pv(RealSize() + totalGrow);
	PayloadIface<T> copyValueInterface(modifiedType, pv);
	VariantArray kr;
	for (int idx = 0, numFields = t_.NumFields(); idx < numFields; ++idx) {
		Get(idx, kr);
		copyValueInterface.Set(idx, kr, Append_False);
		kr.Clear();
	}

	return pv;
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type*>
T PayloadIface<T>::CopyWithRemovedFields(PayloadType modifiedType) {
	size_t totalReduce = 0;
	std::vector<std::string> fieldsLeft;
	for (int idx = 0, numFields = t_.NumFields(); idx < numFields; ++idx) {
		const auto& fieldType = t_.Field(idx);
		const auto& fieldName(fieldType.Name());
		if (modifiedType.Contains(fieldName)) {
			fieldsLeft.emplace_back(fieldName);
		} else {
			totalReduce += fieldType.IsArray() ? sizeof(PayloadFieldValue::Array) : fieldType.Sizeof();
		}
	}

	VariantArray kr;
	T pv(RealSize() - totalReduce);
	PayloadIface<T> copyValueInterface(modifiedType, pv);
	for (const auto& fieldname : fieldsLeft) {
		Get(fieldname, kr);
		copyValueInterface.Set(fieldname, kr, Append_False);
	}

	return pv;
}

#ifdef _MSC_VER
#pragma warning(disable : 5037)
#endif

// clang-format off
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(std::string_view, const VariantArray&, Append);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(int, const VariantArray&, Append);
template void PayloadIface<PayloadValue>::Set<PayloadValue, static_cast<void*>(0)>(int, int, const Variant&);
template void PayloadIface<PayloadValue>::SetSingleElement<PayloadValue, static_cast<void*>(0)>(int, const Variant&);

template PayloadValue PayloadIface<PayloadValue>::CopyTo<PayloadValue, static_cast<void*>(0)>(PayloadType t, bool newFields);
template PayloadValue PayloadIface<PayloadValue>::CopyWithNewOrUpdatedFields<PayloadValue, static_cast<void*>(0)>(PayloadType t);
template PayloadValue PayloadIface<PayloadValue>::CopyWithRemovedFields<PayloadValue, static_cast<void*>(0)>(PayloadType t);

template
ComparationResult PayloadIface<const PayloadValue>::CompareField<WithString::No, NotComparable::Throw, NullsHandling::AlwaysLess>(
															const PayloadValue&, int, const FieldsSet&, size_t&, const CollateOpts&) const;
template
ComparationResult PayloadIface<const PayloadValue>::CompareField<WithString::No, NotComparable::Return, NullsHandling::AlwaysLess>(
															const PayloadValue&, int, const FieldsSet&, size_t&, const CollateOpts&) const;
template
ComparationResult PayloadIface<const PayloadValue>::CompareField<WithString::Yes, NotComparable::Return, NullsHandling::NotComparable>(
															const PayloadValue&, int, const FieldsSet&, size_t&, const CollateOpts&) const;

template
ComparationResult PayloadIface<const PayloadValue>::RelaxCompare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(
		const PayloadIface<const PayloadValue>&, std::string_view, int, const CollateOpts&, TagsMatcher&, TagsMatcher&, bool, bool) const;
// clang-format on

template class PayloadIface<PayloadValue>;
template class PayloadIface<const PayloadValue>;

}  // namespace reindexer
