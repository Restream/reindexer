#pragma once

#include <type_traits>

template <typename T>
struct [[nodiscard]] always_false : std::false_type {};
