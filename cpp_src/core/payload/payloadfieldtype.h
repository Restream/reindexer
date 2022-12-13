#pragma once
#include <string>
#include <vector>
#include "core/key_value_type.h"

namespace reindexer {

// Type of field
class PayloadFieldType {
public:
	PayloadFieldType(KeyValueType t, std::string n, std::vector<std::string> j, bool a) noexcept
		: type_(t), name_(std::move(n)), jsonPaths_(std::move(j)), offset_(0), isArray_(a) {}

	size_t Sizeof() const noexcept;
	size_t ElemSizeof() const noexcept;
	size_t Alignof() const noexcept;
	bool IsArray() const noexcept { return isArray_; };
	void SetArray() noexcept { isArray_ = true; };
	void SetOffset(size_t o) noexcept { offset_ = o; }
	size_t Offset() const noexcept { return offset_; }
	KeyValueType Type() const noexcept { return type_; }
	const std::string &Name() const noexcept { return name_; }
	const std::vector<std::string> &JsonPaths() const noexcept { return jsonPaths_; }
	void AddJsonPath(const std::string &jsonPath) { jsonPaths_.push_back(jsonPath); }

private:
	KeyValueType type_;
	std::string name_;
	std::vector<std::string> jsonPaths_;
	size_t offset_;
	bool isArray_;
};

}  // namespace reindexer
