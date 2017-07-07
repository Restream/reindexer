#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <stdlib.h>

#include "basepayload.h"

namespace reindexer {

template <typename T>
BasePayload<T>::BasePayload(const PayloadType &t, T *p) : t_(t), p_(p) {}

template <typename T>
KeyRefs &BasePayload<T>::Get(int field, KeyRefs &keys) const {
	keys.resize(0);
	if (t_.Field(field).IsArray()) {
		auto *arr = reinterpret_cast<PayloadValue::PayloadArray *>(Field(field).p_);
		keys.reserve(arr->len);

		for (int i = 0; i < arr->len; i++) {
			PayloadValue pv(t_.Field(field), p_->Ptr() + arr->offset + i * t_.Field(field).ElemSizeof());
			keys.push_back(pv.Get());
		}
	} else
		keys.push_back(Field(field).Get());
	return keys;
}
// Get element(s) by field index
template <typename T>
KeyRefs &BasePayload<T>::Get(const string &field, KeyRefs &kvs) const {
	return Get(t_.FieldByName(field), kvs);
}

// Set element or array by field index
template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void BasePayload<T>::Set(const string &field, const KeyRefs &keys, bool append) {
	return Set(t_.FieldByName(field), keys, append);
}

template <typename T>
template <typename U, typename std::enable_if<!std::is_const<U>::value>::type *>
void BasePayload<T>::Set(int field, const KeyRefs &keys, bool append) {
	if (!t_.Field(field).IsArray() && keys.size() >= 1) {
		Field(field).Set(keys[0]);
		return;
	}

	assert(t_.Field(field).IsArray());

	size_t realSize = RealSize();
	auto *arr = reinterpret_cast<PayloadValue::PayloadArray *>(Field(field).p_);
	auto elemSize = t_.Field(field).ElemSizeof();

	size_t grow = elemSize * keys.size();
	size_t strip = 0;
	size_t insert = arr->offset ? (arr->offset + arr->len * elemSize) : realSize;
	if (!append) {
		strip = arr->len * elemSize;
		arr->len = 0;
	}
	assert(insert <= realSize);

	p_->Resize(realSize, realSize + grow - strip);
	memmove(p_->Ptr() + insert + grow - strip, p_->Ptr() + insert, realSize - insert);

	arr = reinterpret_cast<PayloadValue::PayloadArray *>(Field(field).p_);
	if (!arr->offset) arr->offset = insert;

	for (auto &kv : keys) {
		PayloadValue pv(t_.Field(field), p_->Ptr() + arr->offset + arr->len * elemSize);
		pv.Set(kv);
		arr->len++;
	}
	// Move another arrays, after our
	for (int f = 0; f < NumFields(); f++)
		if (f != field && t_.Field(f).IsArray()) {
			auto *arr = reinterpret_cast<PayloadValue::PayloadArray *>(Field(f).p_);
			if (arr->offset >= insert) arr->offset += grow - strip;
		}
}

// Calc real size of payload with embeded arrays
template <typename T>
size_t BasePayload<T>::RealSize() const {
	size_t sz = t_.TotalSize();
	for (int field = 0; field < NumFields(); field++)
		if (t_.Field(field).IsArray()) {
			auto *arr = reinterpret_cast<PayloadValue::PayloadArray *>(Field(field).p_);
			if (arr->offset >= sz) sz = arr->offset + arr->len * t_.Field(field).ElemSizeof();
		}

	return sz;
}

template <typename T>
PayloadValue BasePayload<T>::Field(int field) const {
	return PayloadValue(t_.Field(field), p_->Ptr() + t_.Field(field).Offset());
}

// Get primary key for elem
template <typename T>
string BasePayload<T>::GetPK() const {
	char buf[1024];
	GetPK(buf, sizeof(buf));
	return string(buf);
}

// Get primary key for elem
template <typename T>
void BasePayload<T>::GetPK(char *buf, size_t size) const {
	*buf = 0;
	int n = 0;
	for (int field = 0; field < NumFields() && n < static_cast<int>(size); ++field) {
		if (t_.Field(field).IsPK()) {
			auto f = Field(field).Get();
			switch (f.Type()) {
				case KeyValueInt:
					n = snprintf(buf, size, "%d#", static_cast<int>(f));
					break;
				case KeyValueInt64:
					n = snprintf(buf, size, "%" PRId64 "#", static_cast<int64_t>(f));
					break;
				case KeyValueString: {
					p_string v(f);
					n = std::min(v.length(), size - 1);
					memcpy(buf, static_cast<const char *>(v.data()), n);
					buf[n] = '#';
					buf[n + 1] = 0;
					break;
				}
				default:
					assert(0);
			}
			if (n < 0) break;
			buf += n;
			size -= n;
		}
	}
	return;
}

template <typename T>
bool BasePayload<T>::IsPKEQ(T *other) const {
	BasePayload<T> o(t_, other);

	for (int field = 0; field < NumFields(); ++field) {
		if (t_.Field(field).IsPK())
			if (Field(field).Get() != o.Field(field).Get()) return false;
	}
	return true;
}

// Get fields hash
template <typename T>
size_t BasePayload<T>::GetHash(const FieldsSet &fields) const {
	size_t ret = 0;
	for (auto field : fields) ret ^= Field(field).Get().Hash();
	return ret;
}

template <typename T>
bool BasePayload<T>::IsEQ(T *other, const FieldsSet &fields) const {
	BasePayload<T> o(t_, other);
	for (auto field : fields)
		if (Field(field).Get() != o.Field(field).Get()) return false;
	return true;
}

template <typename T>
bool BasePayload<T>::Less(T *other, const FieldsSet &fields) const {
	BasePayload<T> o(t_, other);
	for (auto field : fields)
		if (Field(field).Get() >= o.Field(field).Get()) return false;
	return true;
}

// Get PK hash
template <typename T>
size_t BasePayload<T>::GetPKHash() const {
	size_t ret = 0;
	for (int field = 0; field < NumFields(); ++field) {
		if (t_.Field(field).IsPK()) ret ^= Field(field).Get().Hash();
	}
	return ret;
}

#include "payloaddata.h"

template class BasePayload<PayloadData>;
template class BasePayload<const PayloadData>;
template void BasePayload<reindexer::PayloadData>::Set<reindexer::PayloadData, static_cast<void *>(0)>(string const &, KeyRefs const &,
																									   bool);
template void BasePayload<reindexer::PayloadData>::Set<reindexer::PayloadData, static_cast<void *>(0)>(int, KeyRefs const &, bool);
}  // namespace reindexer
