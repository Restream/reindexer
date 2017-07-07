#pragma once

#include <stdint.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <vector>
#include "core/keyvalue.h"
#include "tools/errors.h"

using std::string;
using std::vector;
using std::unordered_map;

namespace reindexer {

static constexpr int maxIndexes = 64;

class FieldsSet : public h_vector<uint8_t, maxIndexes> {
public:
	void push_back(int f) {
		if (!(mask_ & (1ULL << f))) {
			mask_ |= 1ULL << f;
			h_vector<uint8_t, maxIndexes>::push_back(f);
		}
	}
	bool contains(int f) const { return mask_ & (1 << f); }
	bool contains(const FieldsSet &f) const { return (mask_ & f.mask_) == mask_; }
	void clear() {
		h_vector<uint8_t, maxIndexes>::clear();
		mask_ = 0;
	}

protected:
	uint64_t mask_ = 0;
};

// Type of field
class PayloadFieldType {
public:
	PayloadFieldType(KeyValueType t, const string &n, const string &j, bool a, bool pk)
		: type_(t), name_(n), jsonPath_(j), offset_(0), isArray_(a), isPK_(pk) {}

	size_t Sizeof() const;
	size_t ElemSizeof() const;
	size_t Alignof() const;
	bool IsArray() const { return isArray_; };
	void SetArray() { isArray_ = true; };
	bool IsPK() const { return isPK_; };
	void SetOffset(size_t o) { offset_ = o; }
	size_t Offset() const { return offset_; }
	KeyValueType Type() const { return type_; }
	const string &Name() const { return name_; }
	const string &JsonPath() const { return jsonPath_; }

protected:
	KeyValueType type_;
	string name_;
	string jsonPath_;
	size_t offset_;
	bool isArray_;
	bool isPK_;
};

// Type of all payload object
class PayloadType {
public:
	explicit PayloadType(const string &name) : name_(name) {}

	const PayloadFieldType &Field(int field) const {
		assert(field < NumFields());
		return fields_[field];
	}
	const string &Name() const { return name_; }
	int NumFields() const { return fields_.size(); }
	void Add(PayloadFieldType f);
	int FieldByName(const string &field) const;
	int FieldByJsonPath(const string &jsonPath) const;

	size_t TotalSize() const;
	string ToString() const;

protected:
	vector<PayloadFieldType> fields_;
	unordered_map<string, int> fieldsByName_;
	unordered_map<string, int> fieldsByJsonPath_;
	string name_;
};

// Helper field's' value object
class PayloadValue {
public:
	struct PayloadArray {
		unsigned offset;
		int len;
	};
	// Construct object
	PayloadValue(const PayloadFieldType &t, uint8_t *v) : t_(t), p_(v) {}
	// Single value operations
	void Set(KeyRef kv);
	KeyRef Get() const;

	// Type of value, not owning
	const PayloadFieldType &t_;
	// Value data, not owning
	uint8_t *p_;
};

}  // namespace reindexer
