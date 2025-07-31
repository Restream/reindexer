#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include "estl/fast_hash_map.h"
#include "payloadfieldtype.h"
#include "tools/stringstools.h"

namespace reindexer {

class Embedder;
class Serializer;
class WrSerializer;

// Type of all payload object
class [[nodiscard]] PayloadTypeImpl {
	typedef fast_hash_map<std::string, int, nocase_hash_str, nocase_equal_str, nocase_less_str> FieldMap;
	typedef fast_hash_map<std::string, int, hash_str, equal_str, less_str> JsonPathMap;

public:
	explicit PayloadTypeImpl(std::string name, std::initializer_list<PayloadFieldType> fields = {})
		: fields_(fields), name_(std::move(name)) {}

	[[nodiscard]] const PayloadFieldType& Field(int field) const& noexcept {
		assertf(field < NumFields(), "{}: {}, {}", name_, field, NumFields());
		return fields_[field];
	}

	[[nodiscard]] const std::string& Name() const& noexcept { return name_; }
	void SetName(std::string name) noexcept { name_ = std::move(name); }
	[[nodiscard]] int NumFields() const noexcept { return fields_.size(); }
	void Add(PayloadFieldType f);
	void Drop(std::string_view field);
	void Replace(int field, PayloadFieldType f);
	[[nodiscard]] int FieldByName(std::string_view field) const;
	[[nodiscard]] bool FieldByName(std::string_view name, int& field) const noexcept;
	[[nodiscard]] bool Contains(std::string_view field) const noexcept { return fieldsByName_.find(field) != fieldsByName_.end(); }
	[[nodiscard]] int FieldByJsonPath(std::string_view jsonPath) const noexcept;
	[[nodiscard]] const std::vector<int>& StrFields() const& noexcept { return strFields_; }

	void serialize(WrSerializer& ser) const;
	void deserialize(Serializer& ser);

	[[nodiscard]] size_t TotalSize() const noexcept;
	[[nodiscard]] std::string ToString() const;
	void Dump(std::ostream&, std::string_view step, std::string_view offset) const;

	[[nodiscard]] std::string_view CheckEmbeddersAuxiliaryField(std::string_view fieldName) const;

	[[nodiscard]] auto Field(int) const&& = delete;
	[[nodiscard]] auto Name() const&& = delete;
	[[nodiscard]] auto StrFields() const&& = delete;

private:
	void checkNewJsonPathBeforeAdd(const PayloadFieldType& fieldType, const std::string& jsonPath) const;
	void checkNewNameBeforeAdd(const PayloadFieldType& fieldType) const;
	void checkEmbedderFields(const PayloadFieldType& fieldType);

	std::vector<PayloadFieldType> fields_;
	FieldMap fieldsByName_;
	JsonPathMap fieldsByJsonPath_;
	std::string name_;
	std::vector<int> strFields_;
};

}  // namespace reindexer
