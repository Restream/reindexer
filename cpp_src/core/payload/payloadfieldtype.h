#pragma once
#include <string>
#include <vector>
#include "core/type_consts.h"

namespace reindexer {

// Type of field
class PayloadFieldType {
public:
	PayloadFieldType(KeyValueType t, std::string n, std::vector<std::string> j, bool a)
		: type_(t), name_(std::move(n)), jsonPaths_(std::move(j)), offset_(0), isArray_(a) {}

	size_t Sizeof() const;
	size_t ElemSizeof() const;
	size_t Alignof() const;
	bool IsArray() const { return isArray_; };
	void SetArray() { isArray_ = true; };
	void SetOffset(size_t o) { offset_ = o; }
	size_t Offset() const { return offset_; }
	KeyValueType Type() const { return type_; }
	const std::string &Name() const { return name_; }
	const std::vector<std::string> &JsonPaths() const { return jsonPaths_; }
	void AddJsonPath(const std::string &jsonPath) { jsonPaths_.push_back(jsonPath); }

private:
	KeyValueType type_;
	std::string name_;
	std::vector<std::string> jsonPaths_;
	size_t offset_;
	bool isArray_;
};

}  // namespace reindexer
