#pragma once

#include <string_view>
#include <unordered_map>

namespace reindexer {

const std::string_view kParamNamespaces = "namespaces";
const std::string_view kParamItems = "items";
const std::string_view kParamCacheEnabled = "cache_enabled";
const std::string_view kParamAggregations = "aggregations";
const std::string_view kParamExplain = "explain";
const std::string_view kParamTotalItems = "total_items";
const std::string_view kParamColumns = "columns";
const std::string_view kParamName = "name";
const std::string_view kParamWidthPercents = "width_percents";
const std::string_view kParamMaxChars = "max_chars";
const std::string_view kParamWidthChars = "width_chars";
const std::string_view kParamSuccess = "success";
const std::string_view kParamResponseCode = "response_code";
const std::string_view kParamDescription = "description";
const std::string_view kParamUpdated = "updated";
const std::string_view kParamQueryTotalItems = "query_total_items";
const std::string_view kTxId = "tx_id";

const std::unordered_map<std::string_view, int> kProtoQueryResultsFields = {
	{kParamItems, 1},	   {kParamNamespaces, 2},	   {kParamCacheEnabled, 3}, {kParamExplain, 4},
	{kParamTotalItems, 5}, {kParamQueryTotalItems, 6}, {kParamColumns, 7},		{kParamAggregations, 8}};
const std::unordered_map<std::string_view, int> kProtoColumnsFields = {
	{kParamName, 1}, {kParamWidthPercents, 2}, {kParamMaxChars, 3}, {kParamWidthChars, 4}};
const std::unordered_map<std::string_view, int> kProtoModifyResultsFields = {{kParamItems, 1}, {kParamUpdated, 2}, {kParamSuccess, 3}};
const std::unordered_map<std::string_view, int> kProtoErrorResultsFields = {
	{kParamSuccess, 1}, {kParamResponseCode, 2}, {kParamDescription, 3}};
}  // namespace reindexer
