#pragma once

#include "core/namespace/namespacename.h"
#include "fmt/format.h"

template <>
struct fmt::formatter<reindexer::NamespaceName> : public fmt::formatter<std::string_view> {};
