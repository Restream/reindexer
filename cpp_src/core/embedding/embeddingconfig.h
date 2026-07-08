#pragma once

#include "estl/h_vector.h"
#include "fmt/format.h"

namespace reindexer {

struct [[nodiscard]] PoolConfig {
	size_t connections{10};
	std::string endpointUrl;
	size_t connect_timeout_ms{300};
	size_t read_timeout_ms{5'000};
	size_t write_timeout_ms{5'000};
	bool operator==(const PoolConfig& other) const noexcept = default;
};

class [[nodiscard]] CacheTag final {
public:
	explicit CacheTag(std::string_view tag) : tag_{tag} {
		if (!tag_.empty()) {
			hash_ = std::hash<std::string>()(tag_);
		}
	}
	CacheTag() noexcept = default;
	CacheTag(const CacheTag& other) noexcept : tag_{other.tag_}, hash_{other.hash_} {}
	CacheTag(CacheTag&& other) noexcept = default;
	CacheTag& operator=(const CacheTag&) noexcept = delete;
	CacheTag& operator=(CacheTag&&) noexcept = default;

	const std::string& Tag() const& noexcept { return tag_; }
	auto Tag() const&& = delete;
	size_t Hash() const noexcept { return hash_; }
	bool operator==(const CacheTag& other) const noexcept = default;

private:
	std::string tag_;
	size_t hash_{0};
};
struct [[nodiscard]] CacheTagHash {
	std::size_t operator()(const CacheTag& tag) const noexcept { return tag.Hash(); }
};
struct [[nodiscard]] CacheTagEqual {
	bool operator()(const CacheTag& lhs, const CacheTag& rhs) const noexcept { return lhs.Tag() == rhs.Tag(); }
};
struct [[nodiscard]] CacheTagLess {
	bool operator()(const CacheTag& lhs, const CacheTag& rhs) const noexcept { return lhs.Tag() < rhs.Tag(); }
};

struct [[nodiscard]] EmbedderConfig {
	CacheTag tag;
	reindexer::h_vector<std::string, 1> fields;
	enum class [[nodiscard]] Strategy { Always, EmptyOnly, Strict } strategy{Strategy::Always};
	bool operator==(const EmbedderConfig& other) const noexcept = default;
};

}  // namespace reindexer

template <>
struct [[nodiscard]] fmt::formatter<reindexer::CacheTag> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::CacheTag& tag, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(tag.Tag(), ctx);
	}
};
