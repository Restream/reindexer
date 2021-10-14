#pragma once
#include <string>
#include <vector>
#include "core/type_consts.h"
#include "estl/h_vector.h"

namespace reindexer {

// Type of field
class PayloadFieldType {
public:
	PayloadFieldType(KeyValueType t, const std::string &n, const h_vector<std::string, 0> &j, bool a)
		: type_(t), name_(n), jsonPaths_(j), offset_(0), isArray_(a) {}

	size_t Sizeof() const;
	size_t ElemSizeof() const;
	size_t Alignof() const;
	bool IsArray() const { return isArray_; };
	void SetArray() { isArray_ = true; };
	void SetOffset(size_t o) { offset_ = o; }
	size_t Offset() const { return offset_; }
	KeyValueType Type() const { return type_; }
	const std::string &Name() const { return name_; }
	const h_vector<std::string, 0> &JsonPaths() const { return jsonPaths_; }
	void AddJsonPath(const std::string &jsonPath) { jsonPaths_.push_back(jsonPath); }

protected:
	KeyValueType type_;
	std::string name_;
	h_vector<std::string, 0> jsonPaths_;
	size_t offset_;
	bool isArray_;
};

}  // namespace reindexer
