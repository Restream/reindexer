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
	CacheTag& operator=(CacheTag&&) noexcept = delete;

	[[nodiscard]] const std::string& Tag() const& noexcept { return tag_; }
	[[nodiscard]] auto Tag() const&& = delete;
	[[nodiscard]] size_t Hash() const noexcept { return hash_; }

private:
	const std::string tag_;
	size_t hash_{0};
};
struct CacheTagHash {
	[[nodiscard]] std::size_t operator()(const CacheTag& tag) const noexcept { return tag.Hash(); }
};
struct CacheTagEqual {
	[[nodiscard]] bool operator()(const CacheTag& lhs, const CacheTag& rhs) const noexcept { return lhs.Tag() == rhs.Tag(); }
};
struct CacheTagLess {
	[[nodiscard]] bool operator()(const CacheTag& lhs, const CacheTag& rhs) const noexcept { return lhs.Tag() < rhs.Tag(); }
};

struct [[nodiscard]] EmbedderConfig {
	CacheTag tag;
	reindexer::h_vector<std::string, 1> fields;
	enum class Strategy { Always, EmptyOnly, Strict } strategy{Strategy::Always};
};

}  // namespace reindexer

template <>
struct fmt::formatter<reindexer::CacheTag> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::CacheTag& tag, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(tag.Tag(), ctx);
	}
};
