#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include "estl/cow.h"
#include "payloadfieldtype.h"

namespace reindexer {

class PayloadTypeImpl;

class [[nodiscard]] PayloadType : public shared_cow_ptr<PayloadTypeImpl> {
public:
	PayloadType() noexcept = default;
	PayloadType(PayloadType&&) noexcept = default;
	PayloadType(const PayloadType&) = default;
	PayloadType& operator=(PayloadType&&) noexcept = default;
	PayloadType& operator=(const PayloadType&) = default;
	explicit PayloadType(const std::string& name, std::initializer_list<PayloadFieldType> = {});
	explicit PayloadType(const PayloadTypeImpl& impl);
	~PayloadType();
	const PayloadFieldType& Field(int field) const& noexcept;
	const std::string& Name() const& noexcept;
	void SetName(std::string_view name);
	int NumFields() const noexcept;
	void Add(PayloadFieldType f);
	void Drop(std::string_view field);
	void Replace(int field, PayloadFieldType f);
	int FieldByName(std::string_view field) const;
	bool FieldByName(std::string_view name, int& field) const noexcept;
	bool Contains(std::string_view field) const noexcept;
	int FieldByJsonPath(std::string_view jsonPath) const noexcept;
	const std::vector<int>& StrFields() const& noexcept;
	size_t TotalSize() const noexcept;
	std::string ToString() const;

	void Dump(std::ostream&, std::string_view step = "  ", std::string_view offset = "") const;

	std::string_view CheckEmbeddersAuxiliaryField(std::string_view fieldName) const;

	auto Field(int field) const&& = delete;
	auto Name() const&& = delete;
	auto StrFields() const&& = delete;
};

}  // namespace reindexer
