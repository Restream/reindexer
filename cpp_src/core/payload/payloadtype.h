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
	[[nodiscard]] const PayloadFieldType& Field(int field) const& noexcept;
	[[nodiscard]] const std::string& Name() const& noexcept;
	void SetName(std::string_view name);
	[[nodiscard]] int NumFields() const noexcept;
	void Add(PayloadFieldType f);
	void Drop(std::string_view field);
	void Replace(int field, PayloadFieldType f);
	[[nodiscard]] int FieldByName(std::string_view field) const;
	[[nodiscard]] bool FieldByName(std::string_view name, int& field) const noexcept;
	[[nodiscard]] bool Contains(std::string_view field) const noexcept;
	[[nodiscard]] int FieldByJsonPath(std::string_view jsonPath) const noexcept;
	[[nodiscard]] const std::vector<int>& StrFields() const& noexcept;
	[[nodiscard]] size_t TotalSize() const noexcept;
	[[nodiscard]] std::string ToString() const;

	void Dump(std::ostream&, std::string_view step = "  ", std::string_view offset = "") const;

	[[nodiscard]] std::string_view CheckEmbeddersAuxiliaryField(std::string_view fieldName) const;

	[[nodiscard]] auto Field(int field) const&& = delete;
	[[nodiscard]] auto Name() const&& = delete;
	[[nodiscard]] auto StrFields() const&& = delete;
};

}  // namespace reindexer
