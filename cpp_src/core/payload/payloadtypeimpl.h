#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include "estl/fast_hash_map.h"
#include "payloadfieldtype.h"
#include "tools/stringstools.h"

namespace reindexer {

class Serializer;
class WrSerializer;

// Type of all payload object
class PayloadTypeImpl {
	typedef fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str> FieldMap;
	typedef fast_hash_map<std::string, int, hash_str, equal_str> JsonPathMap;

public:
	PayloadTypeImpl(const std::string &name, std::initializer_list<PayloadFieldType> fields = {}) : fields_(fields), name_(name) {}

	const PayloadFieldType &Field(int field) const {
		assertf(field < NumFields(), "%s: %d, %d", name_, field, NumFields());
		return fields_[field];
	}

	const std::string &Name() const noexcept { return name_; }
	void SetName(const std::string &name) { name_ = name; }
	int NumFields() const noexcept { return fields_.size(); }
	void Add(PayloadFieldType f);
	bool Drop(std::string_view field);
	int FieldByName(std::string_view field) const;
	bool FieldByName(std::string_view name, int &field) const;
	bool Contains(std::string_view field) const;
	int FieldByJsonPath(std::string_view jsonPath) const;
	const std::vector<int> &StrFields() const { return strFields_; }

	void serialize(WrSerializer &ser) const;
	void deserialize(Serializer &ser);

	size_t TotalSize() const;
	std::string ToString() const;
	void Dump(std::ostream &, std::string_view step, std::string_view offset) const;

protected:
	std::vector<PayloadFieldType> fields_;
	FieldMap fieldsByName_;
	JsonPathMap fieldsByJsonPath_;
	std::string name_;
	std::vector<int> strFields_;
};

}  // namespace reindexer
