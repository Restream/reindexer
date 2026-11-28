#pragma once

#include "fmt/format.h"
#include "urlparser/urlparser.h"
#include "yaml-cpp/node/node.h"

namespace reindexer {

namespace cluster {
struct ShardingConfig;
class AsyncReplNodeConfig;
}  // namespace cluster

class [[nodiscard]] DSN {
public:
	DSN() noexcept = default;
	explicit DSN(std::string dsn) : dsn_(std::move(dsn)) { processDSN(); }

	const httpparser::UrlParser& Parser() const& noexcept { return parsed_; }
	const httpparser::UrlParser& Parser() const&& = delete;

	DSN& WithDb(std::string&& db) &;
	DSN&& WithDb(std::string&& db) &&;

	struct [[nodiscard]] RefWrapperCompare {
		bool operator()(const std::reference_wrapper<const DSN>& lhs, const std::reference_wrapper<const DSN>& rhs) const noexcept {
			return lhs.get().dsn_ < rhs.get().dsn_;
		}
	};

private:
	friend bool Compare(const DSN& lhs, const DSN& rhs) noexcept;
	friend bool RelaxCompare(const DSN& lhs, const DSN& rhs) noexcept;

	friend struct cluster::ShardingConfig;
	friend class cluster::AsyncReplNodeConfig;

	template <typename Cout>
	friend Cout& operator<<(Cout& cout, const DSN& dsn);

	template <typename T>
	friend struct ::YAML::convert;
	friend struct std::hash<reindexer::DSN>;

	template <typename, typename, typename>
	friend struct fmt::formatter;

	void processDSN();
	std::string dsn_, masked_;
	httpparser::UrlParser parsed_;
};

template <typename Cout>
Cout& operator<<(Cout& cout, const reindexer::DSN& dsn) {
	cout << dsn.masked_;
	return cout;
}

inline bool Compare(const DSN& lhs, const DSN& rhs) noexcept { return lhs.dsn_ == rhs.dsn_; }
inline bool RelaxCompare(const DSN& lhs, const DSN& rhs) noexcept {
	return lhs.parsed_.hostname() == rhs.parsed_.hostname() && lhs.parsed_.port() == rhs.parsed_.port() &&
		   lhs.parsed_.path() == rhs.parsed_.path();
}

// The comparison operator is made a template to ensure that in gtests it loses to a less strict,
// but necessary in tests, non-template implementation
template <typename DsnT>
inline std::enable_if_t<std::is_convertible_v<DsnT, DSN>, bool> operator==(const DsnT& lhs, const DsnT& rhs) noexcept {
	if constexpr (std::is_same_v<DsnT, DSN>) {
		return Compare(lhs, rhs);
	} else {
		return RelaxCompare(lhs, rhs);
	}
}

}  // namespace reindexer

template <>
struct [[nodiscard]] std::hash<reindexer::DSN> {
	auto operator()(const reindexer::DSN& dsn) const noexcept { return std::hash<std::string>{}(dsn.dsn_); }
};

template <>
struct [[nodiscard]] fmt::formatter<reindexer::DSN> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::DSN& dsn, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(dsn.masked_, ctx);
	}
};

namespace YAML {
template <>
struct [[nodiscard]] convert<reindexer::DSN> {
	static Node encode(const reindexer::DSN& dsn) { return Node(dsn.dsn_); }
};
}  // namespace YAML
