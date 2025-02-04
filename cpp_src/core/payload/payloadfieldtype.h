#pragma once
#include <string>
#include <vector>
#include "core/key_value_type.h"

namespace reindexer {

class Index;
struct IndexDef;
// Type of field
class PayloadFieldType {
public:
	explicit PayloadFieldType(const Index&, const IndexDef&) noexcept;
	PayloadFieldType(KeyValueType t, std::string n, std::vector<std::string> j, bool a) noexcept
		: type_(t), name_(std::move(n)), jsonPaths_(std::move(j)), offset_(0), isArray_(a), arrayDim_(-1) {}

	size_t Sizeof() const noexcept;
	size_t ElemSizeof() const noexcept;
	size_t Alignof() const noexcept;
	bool IsArray() const noexcept { return isArray_; }
	int8_t ArrayDim() const noexcept { return arrayDim_; }
	void SetArray() noexcept { isArray_ = true; }
	void SetOffset(size_t o) noexcept { offset_ = o; }
	size_t Offset() const noexcept { return offset_; }
	KeyValueType Type() const noexcept { return type_; }
	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() && = delete;
	const std::vector<std::string>& JsonPaths() const& noexcept { return jsonPaths_; }
	const std::vector<std::string>& JsonPaths() && = delete;
	void AddJsonPath(const std::string& jsonPath) { jsonPaths_.push_back(jsonPath); }
	std::string ToString() const;

private:
	KeyValueType type_;
	std::string name_;
	std::vector<std::string> jsonPaths_;
	size_t offset_;
	bool isArray_;
	int8_t arrayDim_;
};

}  // namespace reindexer
