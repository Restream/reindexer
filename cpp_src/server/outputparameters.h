#pragma once

#include <unordered_map>
#include "estl/string_view.h"

namespace reindexer {

const reindexer::string_view kParamNamespaces = "namespaces";
const reindexer::string_view kParamItems = "items";
const reindexer::string_view kParamCacheEnabled = "cache_enabled";
const reindexer::string_view kParamAggregations = "aggregations";
const reindexer::string_view kParamExplain = "explain";
const reindexer::string_view kParamTotalItems = "total_items";
const reindexer::string_view kParamColumns = "columns";
const reindexer::string_view kParamName = "name";
const reindexer::string_view kParamWidthPercents = "width_percents";
const reindexer::string_view kParamMaxChars = "max_chars";
const reindexer::string_view kParamWidthChars = "width_chars";
const reindexer::string_view kParamSuccess = "success";
const reindexer::string_view kParamResponseCode = "response_code";
const reindexer::string_view kParamDescription = "description";
const reindexer::string_view kParamUpdated = "updated";
const reindexer::string_view kParamLsn = "lsn";
const reindexer::string_view kParamItem = "item";
const reindexer::string_view kParamQueryTotalItems = "query_total_items";
const reindexer::string_view kTxId = "tx_id";

const std::unordered_map<reindexer::string_view, int> kProtoQueryResultsFields = {
	{kParamItems, 1},	   {kParamNamespaces, 2},	   {kParamCacheEnabled, 3}, {kParamExplain, 4},
	{kParamTotalItems, 5}, {kParamQueryTotalItems, 6}, {kParamColumns, 7},		{kParamAggregations, 8}};
const std::unordered_map<reindexer::string_view, int> kProtoColumnsFields = {
	{kParamName, 1}, {kParamWidthPercents, 2}, {kParamMaxChars, 3}, {kParamWidthChars, 4}};
const std::unordered_map<reindexer::string_view, int> kProtoModifyResultsFields = {
	{kParamItems, 1}, {kParamUpdated, 2}, {kParamSuccess, 3}};
const std::unordered_map<reindexer::string_view, int> kProtoErrorResultsFields = {
	{kParamSuccess, 1}, {kParamResponseCode, 2}, {kParamDescription, 3}};
}  // namespace reindexer
