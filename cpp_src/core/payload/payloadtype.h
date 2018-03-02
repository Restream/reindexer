#pragma once

#include <string>
#include <vector>
#include "estl/cow.h"
#include "estl/fast_hash_map.h"
#include "payloadfieldtype.h"

namespace reindexer {

using std::string;
using std::vector;

// Type of all payload object
class PayloadTypeImpl {
public:
	PayloadTypeImpl(const string &name) : name_(name) {}

	const PayloadFieldType &Field(int field) const {
		assert(field < NumFields());
		return fields_[field];
	}

	const string &Name() const { return name_; }
	int NumFields() const { return fields_.size(); }
	void Add(PayloadFieldType f);
	bool Drop(const string &field);
	int FieldByName(const string &field) const;
	bool Contains(const string &field) const;
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

class PayloadType : public shared_cow_ptr<PayloadTypeImpl> {
public:
	PayloadType() {}
	PayloadType(const string &name) : shared_cow_ptr<PayloadTypeImpl>(std::make_shared<PayloadTypeImpl>(name)) {}
	const PayloadFieldType &Field(int field) const { return get()->Field(field); }

	const string &Name() const { return get()->Name(); }
	int NumFields() const { return get()->NumFields(); }
	void Add(PayloadFieldType f) { clone()->Add(f); }
	bool Drop(const string &field) { return clone()->Drop(field); };
	int FieldByName(const string &field) const { return get()->FieldByName(field); }
	bool Contains(const string &field) const { return get()->Contains(field); }
	int FieldByJsonPath(const string &jsonPath) const { return get()->FieldByJsonPath(jsonPath); }
	const vector<int> &StrFields() const { return get()->StrFields(); }
	size_t TotalSize() const { return get()->TotalSize(); }
	string ToString() const { return get()->ToString(); }
};

}  // namespace reindexer
