#pragma once
#include <tuple>

namespace meta {
template <typename...>
class Map;

template <auto... Args>
using Values2Types = std::tuple<std::integral_constant<decltype(Args), Args>...>;

template <auto... Enums, typename... Types>
class Map<Values2Types<Enums...>, std::tuple<Types...>> {
	using Keys = Values2Types<Enums...>;
	using Values = std::tuple<Types...>;

	template <auto type, std::size_t N = 0>
	static constexpr auto get() {
		static_assert(N >= 0, "N should be non-negative.");
		static_assert(N < sizeof...(Enums), "Type not found.");
		if constexpr (type == std::get<N>(Keys{})) {
			return std::get<N>(Values{});
		} else {
			return get<type, N + 1>();
		}
	}

	template <auto type>
	using type_ = std::enable_if_t<sizeof...(Enums) == sizeof...(Types), decltype(get<type>())>;

public:
	// to satisfy clang-tidy when use GetType-using and avoid false-positive error private method access
	// and don't make the get-method public
	template <auto t_>
	static constexpr type_<t_> GetTypeImpl();

	template <auto type>
	static constexpr auto GetValue() {
		type_<type> res;
		return res.value;
	}

	template <auto type>
	using GetType = decltype(Map::template GetTypeImpl<type>());
};
}  // namespace meta