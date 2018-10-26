#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include "estl/cow.h"
#include "estl/fast_hash_map.h"
#include "payloadfieldtype.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"

namespace reindexer {

using std::string;
using std::vector;

// Type of all payload object
class PayloadTypeImpl {
	typedef fast_hash_map<string, int, nocase_hash_str, nocase_equal_str> FieldMap;
	typedef fast_hash_map<string, int> JsonPathMap;

public:
	PayloadTypeImpl(const string &name, std::initializer_list<PayloadFieldType> fields = {}) : fields_(fields), name_(name) {}

	const PayloadFieldType &Field(int field) const {
		assertf(field < NumFields(), "%s: %d, %d", name_.c_str(), field, NumFields());
		return fields_[field];
	}

	const string &Name() const { return name_; }
	int NumFields() const { return fields_.size(); }
	void Add(PayloadFieldType f);
	bool Drop(const string &field);
	int FieldByName(const string &field) const;
	bool FieldByName(const string &name, int &field) const;
	bool Contains(const string &field) const;
	int FieldByJsonPath(const string &jsonPath) const;
	const vector<int> &StrFields() const { return strFields_; }

	void serialize(WrSerializer &ser) const;
	void deserialize(Serializer &ser);

	size_t TotalSize() const;
	string ToString() const;

protected:
	vector<PayloadFieldType> fields_;
	FieldMap fieldsByName_;
	JsonPathMap fieldsByJsonPath_;
	string name_;
	vector<int> strFields_;
};

class PayloadType : public shared_cow_ptr<PayloadTypeImpl> {
public:
	PayloadType() {}
	PayloadType(const string &name, std::initializer_list<PayloadFieldType> fields = {})
		: shared_cow_ptr<PayloadTypeImpl>(std::make_shared<PayloadTypeImpl>(name, fields)) {}
	const PayloadFieldType &Field(int field) const { return get()->Field(field); }

	const string &Name() const { return get()->Name(); }
	int NumFields() const { return get()->NumFields(); }
	void Add(PayloadFieldType f) { clone()->Add(f); }
	bool Drop(const string &field) { return clone()->Drop(field); }
	int FieldByName(const string &field) const { return get()->FieldByName(field); }
	bool FieldByName(const string &name, int &field) const { return get()->FieldByName(name, field); }
	bool Contains(const string &field) const { return get()->Contains(field); }
	int FieldByJsonPath(const string &jsonPath) const { return get()->FieldByJsonPath(jsonPath); }
	const vector<int> &StrFields() const { return get()->StrFields(); }
	size_t TotalSize() const { return get()->TotalSize(); }
	string ToString() const { return get()->ToString(); }
};

}  // namespace reindexer
