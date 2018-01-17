#pragma once

#include <assert.h>
#include <memory>
#include <string>
#include <vector>
#include "estl/cow.h"
#include "estl/fast_hash_map.h"
#include "payloadfieldtype.h"

namespace reindexer {

using std::string;
using std::vector;

// Type of all payload object
class PayloadType {
public:
	typedef shared_cow_ptr<PayloadType> Ptr;

	PayloadType(const string &name = "") : name_(name) {}

	const PayloadFieldType &Field(int field) const {
		assert(field < NumFields());
		return fields_[field];
	}
	const string &Name() const { return name_; }
	int NumFields() const { return fields_.size(); }
	void Add(PayloadFieldType f);
	int FieldByName(const string &field) const;
	int FieldByJsonPath(const string &jsonPath) const;
	const vector<int> &StrFields() const { return strFields_; }

	size_t TotalSize() const;
	string ToString() const;

protected:
	vector<PayloadFieldType> fields_;
	fast_hash_map<string, int> fieldsByName_;
	fast_hash_map<string, int> fieldsByJsonPath_;
	string name_;
	vector<int> strFields_;
};

}  // namespace reindexer
