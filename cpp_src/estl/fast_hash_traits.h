#pragma once

#include <string_view>
#include <type_traits>

namespace reindexer {

template <typename K>
struct [[nodiscard]] is_recommends_sc_hash_map {
	constexpr static bool value = false;
};
template <>
struct [[nodiscard]] is_recommends_sc_hash_map<std::string_view> {
	constexpr static bool value = true;
};
template <typename K>
inline constexpr bool is_recommends_sc_hash_map_v = is_recommends_sc_hash_map<K>::value;

template <typename K>
struct [[nodiscard]] is_using_sc_version {
	constexpr static bool value = is_recommends_sc_hash_map_v<K> ||
								  (std::is_trivially_copy_assignable_v<K> && std::is_trivially_copy_constructible_v<K>) ||
								  !std::is_nothrow_move_assignable_v<K> || !std::is_nothrow_move_constructible_v<K>;
};
template <typename K>
inline constexpr bool is_using_sc_version_v = is_using_sc_version<K>::value;

}  // namespace reindexer
