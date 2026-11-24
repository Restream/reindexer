#pragma once

namespace reindexer {

template <typename... Ts>
struct [[nodiscard]] overloaded : Ts... {
	using Ts::operator()...;
};
template <typename... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

}  // namespace reindexer
