#pragma once

#include <string_view>
#include "core/enums.h"
#include "vendor/frozen/string.h"
#include "vendor/frozen/unordered_map.h"

namespace reindexer {

constexpr std::string_view kParamNamespaces = "namespaces";
constexpr std::string_view kParamItems = "items";
constexpr std::string_view kParamCacheEnabled = "cache_enabled";
constexpr std::string_view kParamAggregations = "aggregations";
constexpr std::string_view kParamExplain = "explain";
constexpr std::string_view kParamTotalItems = "total_items";
constexpr std::string_view kParamColumns = "columns";
constexpr std::string_view kParamName = "name";
constexpr std::string_view kParamWidthPercents = "width_percents";
constexpr std::string_view kParamMaxChars = "max_chars";
constexpr std::string_view kParamWidthChars = "width_chars";
constexpr std::string_view kParamSuccess = "success";
constexpr std::string_view kParamResponseCode = "response_code";
constexpr std::string_view kParamDescription = "description";
constexpr std::string_view kParamUpdated = "updated";
constexpr std::string_view kParamQueryTotalItems = "query_total_items";
constexpr std::string_view kTxId = "tx_id";

// clang-format off
constexpr auto kProtoQueryResultsFields = frozen::make_unordered_map<frozen::string, TagName>(
																	{{kParamItems,        1_Tag}, {kParamNamespaces,      2_Tag},
																	 {kParamCacheEnabled, 3_Tag}, {kParamExplain,         4_Tag},
																	 {kParamTotalItems,   5_Tag}, {kParamQueryTotalItems, 6_Tag},
																	 {kParamColumns,      7_Tag}, {kParamAggregations,    8_Tag}});

constexpr auto kProtoColumnsFields = frozen::make_unordered_map<frozen::string, TagName>(
																	{{kParamName,         1_Tag}, {kParamWidthPercents,   2_Tag},
																	 {kParamMaxChars,     3_Tag}, {kParamWidthChars,      4_Tag}});

constexpr auto kProtoModifyResultsFields = frozen::make_unordered_map<frozen::string, TagName>(
																	{{kParamItems,        1_Tag}, {kParamUpdated,         2_Tag},
																	 {kParamSuccess,      3_Tag}});

constexpr auto kProtoErrorResultsFields = frozen::make_unordered_map<frozen::string, TagName>(
																	{{kParamSuccess,      1_Tag}, {kParamResponseCode,    2_Tag},
																	 {kParamDescription,  3_Tag}});

constexpr auto kProtoBeginTxResultsFields = frozen::make_unordered_map<frozen::string, TagName>(
																	{{kTxId,      1_Tag}});
// clang-format on

}  // namespace reindexer
