#pragma once

#include <string>
#include <string_view>

namespace reindexer::ns_indexes {

// The PK index alias in Registry::NamesMap - not a real index, just another name for whichever index has Opts().IsPK()
inline constexpr std::string_view kPKIndexName{"#pk"};
// The always-present index at position 0, backing the '-tuple' payload field
inline const std::string kTupleName{"-tuple"};

}  // namespace reindexer::ns_indexes
