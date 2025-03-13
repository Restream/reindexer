#pragma once

#ifdef RX_WITH_STDLIB_DEBUG
#include <limits>
#endif	// RX_WITH_STDLIB_DEBUG

#include <string>
#include <vector>
#include "core/enums.h"
#include "core/key_value_type.h"

namespace reindexer {

class Index;
class IndexDef;
// Type of field
class PayloadFieldType {
public:
	explicit PayloadFieldType(const Index&, const IndexDef&) noexcept;
	PayloadFieldType(KeyValueType t, std::string n, std::vector<std::string> j, bool a,
					 reindexer::FloatVectorDimension dims = reindexer::FloatVectorDimension()) noexcept
		: type_(t), name_(std::move(n)), jsonPaths_(std::move(j)), offset_(0), arrayDims_(dims.Value()), isArray_(a) {
		assertrx(!t.Is<KeyValueType::FloatVector>() || !FloatVectorDimension().IsZero());
	}

	size_t Sizeof() const noexcept;
	size_t ElemSizeof() const noexcept;
	bool IsArray() const noexcept { return isArray_; }
	[[nodiscard]] uint32_t ArrayDims() const noexcept { return arrayDims_; }
	[[nodiscard]] reindexer::FloatVectorDimension FloatVectorDimension() const noexcept {
		assertrx_dbg(arrayDims_ <= std::numeric_limits<reindexer::FloatVectorDimension::value_type>::max());
		// NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
		return reindexer::FloatVectorDimension(arrayDims_);
	}
	void SetArray() noexcept { isArray_ = true; }
	void SetOffset(size_t o) noexcept { offset_ = o; }
	size_t Offset() const noexcept { return offset_; }
	KeyValueType Type() const noexcept { return type_; }
	bool IsFloatVector() const noexcept { return type_.Is<KeyValueType::FloatVector>(); }
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
	uint32_t arrayDims_;
	bool isArray_;
};

}  // namespace reindexer
