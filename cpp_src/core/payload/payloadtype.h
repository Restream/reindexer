#pragma once

#include <initializer_list>
#include <string>
#include <vector>
#include "estl/cow.h"
#include "payloadfieldtype.h"

namespace reindexer {

class PayloadTypeImpl;

class PayloadType : public shared_cow_ptr<PayloadTypeImpl> {
public:
	PayloadType() = default;
	PayloadType(PayloadType &&) = default;
	PayloadType(const PayloadType &) = default;
	PayloadType &operator=(PayloadType &&) = default;
	PayloadType &operator=(const PayloadType &) = default;
	PayloadType(const std::string &name, std::initializer_list<PayloadFieldType> fields = {});
	PayloadType(const PayloadTypeImpl &impl);
	~PayloadType();
	const PayloadFieldType &Field(int field) const;

	const std::string &Name() const;
	void SetName(const std::string &name);
	int NumFields() const;
	void Add(PayloadFieldType);
	bool Drop(std::string_view field);
	int FieldByName(std::string_view field) const;
	bool FieldByName(std::string_view name, int &field) const;
	bool Contains(std::string_view field) const;
	int FieldByJsonPath(std::string_view jsonPath) const;
	const std::vector<int> &StrFields() const;
	size_t TotalSize() const;
	std::string ToString() const;
	void Dump(std::ostream &, std::string_view step = "  ", std::string_view offset = "") const;
};

}  // namespace reindexer
