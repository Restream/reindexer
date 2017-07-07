#pragma once

#include <assert.h>
#include <memory>
#include <string>
#include <vector>
#include "core/type_consts.h"
#include "payloaddata.h"
#include "tools/h_vector.h"

using std::string;
using std::vector;
using std::shared_ptr;
using std::make_shared;

namespace reindexer {

typedef shared_ptr<string> key_string;

struct Slice {
	Slice() : ptr_(nullptr), size_(0) {}
	Slice(const char *p, size_t sz) : ptr_(p), size_(sz) {}
	Slice(const string &str) : ptr_(str.data()), size_(str.size()) {}

	const char *data() const { return ptr_; }
	size_t size() const { return size_; }

protected:
	const char *ptr_;
	size_t size_;
};

struct l_string_hdr {
	int length;
	char data[1];
};

// Dark
struct p_string {
	constexpr static uint64_t tagCstr = 0x0ULL;
	constexpr static uint64_t tagLstr = 0x1ULL;
	constexpr static uint64_t tagCxxstr = 0x2ULL;
	constexpr static uint64_t tagShift = 60ULL;
	constexpr static uint64_t tagMask = 0x3ULL << tagShift;

	explicit p_string(const l_string_hdr *lstr) : v(reinterpret_cast<uint64_t>(lstr) | (tagLstr << tagShift)) {}
	explicit p_string(const char *cstr) : v(reinterpret_cast<uint64_t>(cstr) | (tagCstr << tagShift)) {}
	explicit p_string(const string *str) : v(reinterpret_cast<uint64_t>(str) | (tagCxxstr << tagShift)) {}
	p_string() : v(0) {}

	operator Slice() const { return Slice(data(), length()); }
	const char *data() const {
		switch ((v & tagMask) >> tagShift) {
			case tagCstr:
				return static_cast<const char *>(ptr());
			case tagCxxstr:
				return (static_cast<const string *>(ptr()))->data();
			case tagLstr:
				return &(static_cast<const l_string_hdr *>(ptr()))->data[0];
			default:
				abort();
		}
	}
	size_t size() const { return length(); }
	size_t length() const {
		switch ((v & tagMask) >> tagShift) {
			case tagCstr:
				return strlen(static_cast<const char *>(ptr()));
			case tagCxxstr:
				return (static_cast<const string *>(ptr()))->length();
			case tagLstr:
				return (static_cast<const l_string_hdr *>(ptr()))->length;
			default:
				abort();
		}
	}
	int compare(p_string other) const {
		int l1 = length();
		int l2 = other.length();
		int res = memcmp(data(), other.data(), std::min(l1, l2));
		return res ? res : ((l1 < l2) ? -1 : (l1 > l2) ? 1 : 0);
	}
	bool operator>(p_string other) const { return compare(other) > 0; }
	bool operator<(p_string other) const { return compare(other) < 0; }
	bool operator==(p_string other) const { return compare(other) == 0; }
	bool operator>=(p_string other) const { return compare(other) >= 0; }
	bool operator<=(p_string other) const { return compare(other) <= 0; }

protected:
	const void *ptr() const { return v ? reinterpret_cast<const void *>(v & ~tagMask) : ""; }
	uint64_t v;
};

class KeyRef;
class PayloadData;

class KeyValue {
public:
	KeyValue() : type(KeyValueEmpty) {}
	explicit KeyValue(int v) : type(KeyValueInt), value_int(v) {}
	explicit KeyValue(int64_t v) : type(KeyValueInt64), value_int64(v) {}
	explicit KeyValue(double v) : type(KeyValueDouble), value_double(v) {}
	explicit KeyValue(const string &v) : type(KeyValueString), value_string(make_shared<string>(v)) {}
	explicit KeyValue(const key_string &v) : type(KeyValueString), value_string(v) {}
	explicit KeyValue(const PayloadData &v) : type(KeyValueComposite), value_composite(v) {}
	explicit operator int() const {
		assert(type == KeyValueInt);
		return value_int;
	}
	explicit operator int64_t() const {
		assert(type == KeyValueInt64);
		return value_int64;
	}
	explicit operator double() const {
		assert(type == KeyValueDouble);
		return value_double;
	}
	explicit operator const key_string &() const {
		assert(type == KeyValueString);
		return value_string;
	}
	explicit operator key_string &() {
		assert(type == KeyValueString);
		return value_string;
	}
	explicit operator const PayloadData &() {
		assert(type == KeyValueComposite);
		return value_composite;
	}
	explicit operator const string &() const {
		assert(type == KeyValueString);
		return *value_string;
	}
	explicit operator KeyRef() const;
	string toString() const;
	int toInt() const;
	int64_t toInt64() const;
	double toDouble() const;
	int convert(KeyValueType type);
	KeyValueType Type() const { return type; }
	static const char *TypeName(KeyValueType t);
	bool operator<(const KeyValue &v2) const;
	bool operator==(const KeyValue &v2) const;

protected:
	KeyValueType type;
	union {
		int value_int;
		double value_double;
		int64_t value_int64;
	};

	PayloadData value_composite;
	key_string value_string;
};

class KeyRef {
public:
	KeyRef() : type(KeyValueEmpty) {}
	explicit KeyRef(const int &v) : type(KeyValueInt), value_int(v) {}
	explicit KeyRef(const int64_t &v) : type(KeyValueInt64), value_int64(v) {}
	explicit KeyRef(const double &v) : type(KeyValueDouble), value_double(v) {}
	explicit KeyRef(const key_string &v) : type(KeyValueString), value_string(v.get()) {}
	explicit KeyRef(p_string v) : type(KeyValueString), value_string(v) {}
	explicit KeyRef(const PayloadData &v) : type(KeyValueComposite), value_composite(&v) {}
	KeyRef(const KeyRef &other) : type(other.type), value_int64(other.value_int64) {}
	KeyRef &operator=(const KeyRef &other) {
		if (this != &other) {
			type = other.type;
			value_int64 = other.value_int64;
		}
		return *this;
	}

	explicit operator int() const {
		assert(type == KeyValueInt);
		return value_int;
	}
	explicit operator int64_t() const {
		assert(type == KeyValueInt64);
		return value_int64;
	}
	explicit operator double() const {
		assert(type == KeyValueDouble);
		return value_double;
	}

	explicit operator p_string() const {
		assert(type == KeyValueString);
		return value_string;
	}

	explicit operator const PayloadData *() const {
		assert(type == KeyValueComposite);
		return value_composite;
	}
	explicit operator const PayloadData &() const {
		assert(type == KeyValueComposite);
		return *value_composite;
	}

	bool operator==(const KeyRef &other) const { return EQ(other); }
	bool operator!=(const KeyRef &other) const { return !EQ(other); }
	bool operator<(const KeyRef &other) const { return Less(other); }
	bool operator>=(const KeyRef &other) const { return !Less(other); }
	bool EQ(const KeyRef &other) const;
	bool Less(const KeyRef &other) const;
	size_t Hash() const;

	explicit operator KeyValue() const;
	KeyValueType Type() const { return type; }
	static const char *TypeName(KeyValueType t);

protected:
	KeyValueType type;
	union {
		int value_int;
		int64_t value_int64;
		double value_double;
		p_string value_string;
		const PayloadData *value_composite;
	};
};

class KeyRefs : public h_vector<KeyRef, 32> {
public:
	bool operator==(const KeyRefs &other) const { return EQ(other); }
	bool operator!=(const KeyRefs &other) const { return !EQ(other); }
	bool EQ(const KeyRefs &other) const {
		if (other.size() != size()) return false;
		for (size_t i = 0; i < size(); ++i)
			if (at(i) != other.at(i)) return false;
		return true;
	}
	size_t Hash() const {
		size_t ret = size();
		for (size_t i = 0; i < size(); ++i) ret ^= at(i).Hash();
		return ret;
	}
};

class KeyValues : public h_vector<KeyValue, 16> {
public:
	bool operator==(const KeyValues &other) const { return EQ(other); }
	bool operator!=(const KeyValues &other) const { return !EQ(other); }
	bool EQ(const KeyValues &other) const {
		if (other.size() != size()) return false;
		for (size_t i = 0; i < size(); ++i)
			if (static_cast<KeyRef>(at(i)) != static_cast<KeyRef>(other.at(i))) return false;  //?? DYNAMIC
		return true;
	}
	size_t Hash() const {
		size_t ret = size();
		for (size_t i = 0; i < size(); ++i) ret ^= (static_cast<KeyRef>(at(i))).Hash();
		return ret;
	}
};

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::p_string> {
public:
	size_t operator()(const reindexer::p_string &str) const {
		size_t h = 0;
		const char *p = str.data();
		const char *e = p + str.length();
		for (; p != e;) h += h * 65599 + *p++;
		return h ^ (h >> 16);
	}
};

}  // namespace std
