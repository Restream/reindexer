#pragma once

#include <span>
#include <string>
#include <vector>
#include "core/enums.h"
#include "core/indexopts.h"
#include "core/type_consts.h"
#include "estl/expected.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class WrSerializer;

using JsonPaths = std::vector<std::string>;

class [[nodiscard]] IndexDef {
public:
	explicit IndexDef(std::string name) : name_{std::move(name)} { Validate(); }
	IndexDef(std::string name, JsonPaths jsonPaths, std::string indexType, std::string fieldType, IndexOpts opts, int64_t expireAfter = 0);
	IndexDef(std::string name, JsonPaths, ::IndexType, IndexOpts, int64_t expireAfter);
	IndexDef(std::string name, std::string indexType, std::string fieldType, IndexOpts opts);
	IndexDef(std::string name, JsonPaths jsonPaths, ::IndexType type, IndexOpts opts);
	void GetJSON(WrSerializer& ser, ExtraIndexDescription withExtras = ExtraIndexDescription_False) const;
	::IndexType IndexType() const { return DetermineIndexType(name_, indexType_, fieldType_); }
	const std::string& Name() const& noexcept { return name_; }
	const reindexer::JsonPaths& JsonPaths() const& noexcept { return jsonPaths_; }
	const std::string& IndexTypeStr() const& noexcept { return indexType_; }
	const std::string& FieldType() const& noexcept { return fieldType_; }
	const IndexOpts& Opts() const& noexcept { return opts_; }
	int64_t ExpireAfter() const noexcept { return expireAfter_; }

	template <typename Str, std::enable_if_t<std::is_constructible_v<std::string, Str>>* = nullptr>
	void SetName(Str&& name) {
		name_ = std::forward<Str>(name);
	}
	void SetOpts(const IndexOpts& opts);
	void SetOpts(IndexOpts&& opts);
	void SetJsonPaths(const reindexer::JsonPaths&);
	void SetJsonPaths(reindexer::JsonPaths&&);
	void SetExpireAfter(int64_t exp) noexcept { expireAfter_ = exp; }
	void SetIndexTypeStr(std::string type) noexcept { indexType_ = std::move(type); }

	static Expected<IndexDef> FromJSON(std::string_view json);
	static Expected<IndexDef> FromJSON(std::span<char> json);
	static IndexDef FromJSON(const gason::JsonNode& jvalue);

	size_t HeapSize() const noexcept {
		size_t size = name_.size() + indexType_.size() + fieldType_.size() + opts_.HeapSize();
		for (auto& path : jsonPaths_) {
			size += path.size();
		}
		return size;
	}

	static ::IndexType DetermineIndexType(std::string_view indexName, std::string_view indexType, std::string_view fieldType);
	// Throws if IndexDef is not valid
	void Validate() const;

	auto Name() const&& = delete;
	auto JsonPaths() const&& = delete;
	auto IndexTypeStr() const&& = delete;
	auto FieldType() const&& = delete;
	auto Opts() const&& = delete;

	enum class [[nodiscard]] Diff : uint8_t {
		Name = 1,
		JsonPaths = 1 << 1,
		IndexType = 1 << 2,
		FieldType = 1 << 3,
		ExpireAfter = 1 << 4,
	};

	using DiffResult = compare_enum::Diff<IndexDef::Diff, IndexOpts::ParamsDiff, IndexOpts::OptsDiff, FloatVectorIndexOpts::Diff>;
	DiffResult Compare(const IndexDef& o) const noexcept;

private:
	void initFromIndexType(::IndexType);
	static void validate(::IndexType, size_t jsonPathsCount, const IndexOpts&);
	bool isSortable() const noexcept;

	std::string name_;
	reindexer::JsonPaths jsonPaths_;
	std::string indexType_;
	std::string fieldType_;
	IndexOpts opts_;
	int64_t expireAfter_ = 0;
};

bool isStore(IndexType type) noexcept;
bool validateIndexName(std::string_view name, IndexType type) noexcept;

}  // namespace reindexer
