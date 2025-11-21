#pragma once

#ifdef RX_WITH_STDLIB_DEBUG
#include <limits>
#endif	// RX_WITH_STDLIB_DEBUG

#include <memory>
#include <string>
#include <vector>
#include "core/enums.h"
#include "core/indexopts.h"
#include "core/key_value_type.h"

namespace reindexer {

class Index;
class IndexDef;
class QueryEmbedder;
class UpsertEmbedder;
class EmbeddersCache;

// Type of field
class [[nodiscard]] PayloadFieldType {
public:
	PayloadFieldType(std::string_view nsName, const Index& index, const IndexDef& indexDef,
					 const std::shared_ptr<EmbeddersCache>& embeddersCache, bool enablePerfStat);
	PayloadFieldType(KeyValueType t, std::string n, std::vector<std::string> j, IsArray a,
					 reindexer::FloatVectorDimension dims = reindexer::FloatVectorDimension()) noexcept
		: type_(t), name_(std::move(n)), jsonPaths_(std::move(j)), offset_(0), arrayDims_(dims.Value()), isArray_(a) {
		assertrx(!t.Is<KeyValueType::FloatVector>() || !FloatVectorDimension().IsZero());
	}

	size_t Sizeof() const noexcept;
	size_t ElemSizeof() const noexcept;
	reindexer::IsArray IsArray() const noexcept { return isArray_; }
	uint32_t ArrayDims() const noexcept { return arrayDims_; }
	reindexer::FloatVectorDimension FloatVectorDimension() const {
		assertrx_dbg(arrayDims_ <= std::numeric_limits<reindexer::FloatVectorDimension::value_type>::max());
		// NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
		return reindexer::FloatVectorDimension(arrayDims_);
	}
	void SetArray() noexcept { isArray_ = IsArray_True; }
	void SetOffset(size_t o) noexcept { offset_ = o; }
	size_t Offset() const noexcept { return offset_; }
	KeyValueType Type() const noexcept { return type_; }
	bool IsFloatVector() const noexcept { return type_.Is<KeyValueType::FloatVector>(); }
	const std::string& Name() const& noexcept { return name_; }
	const std::vector<std::string>& JsonPaths() const& noexcept { return jsonPaths_; }
	void AddJsonPath(const std::string& jsonPath) { jsonPaths_.push_back(jsonPath); }
	std::string ToString() const;
	std::shared_ptr<const reindexer::UpsertEmbedder> UpsertEmbedder() const { return upsertEmbedder_; }
	std::shared_ptr<const reindexer::QueryEmbedder> QueryEmbedder() const { return queryEmbedder_; }

	auto Name() const&& = delete;
	auto JsonPaths() const&& = delete;

private:
	void createEmbedders(std::string_view nsName, const std::optional<FloatVectorIndexOpts::EmbeddingOpts>& embeddingOpts,
						 const std::shared_ptr<EmbeddersCache>& embeddersCache, bool enablePerfStat);

	KeyValueType type_;
	std::string name_;
	std::vector<std::string> jsonPaths_;
	size_t offset_{0};
	uint32_t arrayDims_{0};
	reindexer::IsArray isArray_{IsArray_False};
	std::shared_ptr<const reindexer::UpsertEmbedder> upsertEmbedder_;
	std::shared_ptr<const reindexer::QueryEmbedder> queryEmbedder_;
};

}  // namespace reindexer
