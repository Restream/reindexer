#pragma once
#include <tuple>

namespace reindexer {
namespace meta {
template <typename... Pairs>
class Map {
	template <auto key_value, std::size_t N = 0>
	static constexpr auto get() {
		static_assert(N < sizeof...(Pairs), "Type not found.");

		using P = std::decay_t<decltype(std::get<N>(std::tuple<Pairs...>{}))>;
		using Key = typename P::Type;
		using Value = typename P::Value;
		if constexpr (key_value == Key::value) {
			return Value{};
		} else {
			return get<key_value, N + 1>();
		}
	}

	// to satisfy clang-tidy when use GetType-using and avoid false-positive error private method access
	// and don't make the get-method public
	template <auto key_value>
	struct GetTypeImpl {
		using type = decltype(get<key_value>());
	};

public:
	template <auto key_value>
	using GetType = typename GetTypeImpl<key_value>::type;

	template <auto key_value>
	static constexpr auto GetValue() {
		GetType<key_value> res;
		return res.value;
	}
};

template <auto K, typename V>
struct Pair {
	using Type = std::integral_constant<decltype(K), K>;
	using Value = V;
};

template <auto K, auto V>
constexpr Pair<K, std::integral_constant<decltype(V), V>> MakePair();
template <auto K, typename V>
constexpr Pair<K, V> MakePair();
}  // namespace meta
}  // namespace reindexer

#define RDX_META_PAIR(Key, Value) decltype(meta::MakePair<Key, Value>())