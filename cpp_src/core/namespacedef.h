#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "core/indexdef.h"
#include "tools/errors.h"
#include "tools/lsn.h"
#include "tools/stringstools.h"

namespace reindexer {

class WrSerializer;
class RdxContext;
class Namespace;

struct [[nodiscard]] NamespaceDef {
	struct NameOnly {};

	NamespaceDef() = default;
	explicit NamespaceDef(std::string_view iname, StorageOpts istorage = StorageOpts().Enabled().CreateIfMissing())
		: name(iname), storage(istorage) {}
	explicit NamespaceDef(std::string_view iname, NameOnly) : name{iname}, isNameOnly{true} {}

	NamespaceDef& AddIndex(const std::string& iname, const std::string& indexType, const std::string& fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.emplace_back(iname, JsonPaths{iname}, indexType, fieldType, std::move(opts));
		return *this;
	}

	NamespaceDef& AddIndex(const std::string& iname, const JsonPaths& jsonPaths, const std::string& indexType, const std::string& fieldType,
						   IndexOpts opts = IndexOpts()) {
		indexes.emplace_back(iname, jsonPaths, indexType, fieldType, std::move(opts));
		return *this;
	}

	NamespaceDef& AddIndex(const IndexDef& idxDef) {
		indexes.emplace_back(idxDef);
		return *this;
	}

	Error FromJSON(std::span<char> json);
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(WrSerializer&, ExtraIndexDescription withIndexExtras = ExtraIndexDescription_False) const;
	bool HasSchema() const noexcept { return !schemaJson.empty() && schemaJson != "{}"; }
	size_t HeapSize() const noexcept {
		size_t size = name.size() + indexes.size() * sizeof(IndexDef) + schemaJson.size();
		for (auto& idx : indexes) {
			size += idx.HeapSize();
		}
		return size;
	}

public:
	std::string name;
	StorageOpts storage;
	std::vector<IndexDef> indexes;
	bool isNameOnly = false;
	std::string schemaJson = "{}";
};

enum [[nodiscard]] EnumNamespacesOpt {
	kEnumNamespacesWithClosed = 1,
	kEnumNamespacesOnlyNames = 1 << 1,
	kEnumNamespacesHideSystem = 1 << 2,
	kEnumNamespacesHideTemporary = 1 << 3,
};

struct [[nodiscard]] EnumNamespacesOpts {
	EnumNamespacesOpts() : options_(0) {}

	bool IsWithClosed() const noexcept { return options_ & kEnumNamespacesWithClosed; }
	bool IsOnlyNames() const noexcept { return options_ & kEnumNamespacesOnlyNames; }
	bool IsHideSystem() const noexcept { return options_ & kEnumNamespacesHideSystem; }
	bool IsHideTemporary() const noexcept { return options_ & kEnumNamespacesHideTemporary; }
	bool MatchFilter(std::string_view nsName, const Namespace& ns, const RdxContext& ctx) const;
	bool MatchNameFilter(std::string_view nsName) const noexcept {
		return (filter_.empty() || iequals(filter_, nsName)) && (!IsHideSystem() || (!nsName.empty() && nsName[0] != '#')) &&
			   (!IsHideTemporary() || (!nsName.empty() && nsName[0] != '@')) && (!nsName.empty() && nsName[0] != '!');
	}

	// Add not opened namespaces to enumeration
	EnumNamespacesOpts& WithClosed(bool value = true) noexcept {
		options_ = value ? options_ | kEnumNamespacesWithClosed : options_ & ~(kEnumNamespacesWithClosed);
		return *this;
	}
	// Return only namespaces names (faster, and do not try rlock nses)
	EnumNamespacesOpts& OnlyNames(bool value = true) noexcept {
		options_ = value ? options_ | kEnumNamespacesOnlyNames : options_ & ~(kEnumNamespacesOnlyNames);
		return *this;
	}
	// Hide system namespaces from enumeration
	EnumNamespacesOpts& HideSystem(bool value = true) noexcept {
		options_ = value ? options_ | kEnumNamespacesHideSystem : options_ & ~(kEnumNamespacesHideSystem);
		return *this;
	}
	// Hide temporary namespaces from enumeration
	EnumNamespacesOpts& HideTemporary(bool value = true) noexcept {
		options_ = value ? options_ | kEnumNamespacesHideTemporary : options_ & ~(kEnumNamespacesHideTemporary);
		return *this;
	}
	// Add name filter
	EnumNamespacesOpts& WithFilter(std::string_view flt) noexcept {
		filter_ = flt;
		return *this;
	}
	std::string_view filter_;
	uint16_t options_;
};

struct [[nodiscard]] NsReplicationOpts {
	Error FromJSON(std::span<char> json);
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(WrSerializer&) const;

	std::optional<int32_t> tmStateToken;
	lsn_t nsVersion;
};

}  // namespace reindexer
