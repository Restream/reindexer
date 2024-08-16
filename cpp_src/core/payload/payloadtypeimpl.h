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
	typedef fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str, nocase_less_str> FieldMap;
	typedef fast_hash_map<std::string, int, hash_str, equal_str, less_str> JsonPathMap;

public:
	PayloadTypeImpl(std::string name, std::initializer_list<PayloadFieldType> fields = {}) : fields_(fields), name_(std::move(name)) {}

	const PayloadFieldType& Field(int field) const& noexcept {
		assertf(field < NumFields(), "%s: %d, %d", name_, field, NumFields());
		return fields_[field];
	}
	const PayloadFieldType& Field(int) const&& = delete;

	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() const&& = delete;
	void SetName(std::string_view name) noexcept { name_ = std::string(name); }
	int NumFields() const noexcept { return fields_.size(); }
	void Add(PayloadFieldType f);
	bool Drop(std::string_view field);
	int FieldByName(std::string_view field) const;
	bool FieldByName(std::string_view name, int& field) const noexcept;
	bool Contains(std::string_view field) const noexcept { return fieldsByName_.find(field) != fieldsByName_.end(); }
	int FieldByJsonPath(std::string_view jsonPath) const noexcept;
	const std::vector<int>& StrFields() const& noexcept { return strFields_; }
	const std::vector<int>& StrFields() const&& = delete;

	void serialize(WrSerializer& ser) const;
	void deserialize(Serializer& ser);

	size_t TotalSize() const noexcept;
	std::string ToString() const;
	void Dump(std::ostream&, std::string_view step, std::string_view offset) const;

private:
	void checkNewJsonPathBeforeAdd(const PayloadFieldType& f, const std::string& jsonPath) const;

	std::vector<PayloadFieldType> fields_;
	FieldMap fieldsByName_;
	JsonPathMap fieldsByJsonPath_;
	std::string name_;
	std::vector<int> strFields_;
};

}  // namespace reindexer
