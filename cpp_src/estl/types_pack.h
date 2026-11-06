#pragma once

namespace reindexer {

template <typename... Ts>
class [[nodiscard]] TypesPack;

template <template <typename...> class TargetT, typename... Ts>
struct [[nodiscard]] WithTypesPack;

template <template <typename...> class TargetT, typename... Ts>
struct [[nodiscard]] WithTypesPack<TargetT, TypesPack<Ts...>> {
	using type = TargetT<Ts...>;
};

}  // namespace reindexer
