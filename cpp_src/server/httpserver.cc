#include "httpserver.h"

#include "base64/base64.h"
#include "core/cjson/csvbuilder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/msgpackbuilder.h"
#include "core/cjson/protobufbuilder.h"
#include "core/cjson/protobufschemabuilder.h"
#include "core/dbconfig.h"
#include "core/queryresults/tableviewbuilder.h"
#include "core/schema.h"
#include "core/type_consts.h"
#include "debug/crashqueryreporter.h"
#include "estl/gift_str.h"
#include "gason/gason.h"
#include "itoa/itoa.h"
#include "loggerwrapper.h"
#include "net/http/serverconnection.h"
#include "net/listener.h"
#include "outputparameters.h"
#include "reindexer_version.h"
#include "resources_wrapper.h"
#include "statscollect/istatswatcher.h"
#include "statscollect/prometheus.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "vendor/sort/pdqsort.hpp"
#include "wal/walrecord.h"

using namespace std::string_view_literals;

namespace reindexer_server {

constexpr size_t kTxIdLen = 20;
constexpr auto kTxDeadlineCheckPeriod = std::chrono::seconds(1);

class HTTPServer::TxCommitOption final : public HTTPServer::IQRSerializingOption {
public:
	TxCommitOption(const QueryResults& qr) noexcept : totalCount_{qr.Count()} {}

	std::optional<size_t> TotalCount() const noexcept override { return totalCount_; }
	std::optional<size_t> QueryTotalCount() const noexcept override { return std::nullopt; }
	unsigned ExternalLimit() const noexcept override { return kDefaultLimit; }
	unsigned ExternalOffset() const noexcept override { return kDefaultOffset; }

private:
	size_t totalCount_;
};

class HTTPServer::TwoLevelLimitOffsetOption final : public HTTPServer::IQRSerializingOption {
public:
	TwoLevelLimitOffsetOption(QueryResults& qr, unsigned externalLimit, unsigned externalOffset)
		: totalCount_{qr.Count()}, externalLimit_{externalLimit}, externalOffset_{externalOffset} {
		for (auto& agg : qr.GetAggregationResults()) {
			if (auto type = agg.GetType(); type == AggType::AggCount || type == AggType::AggCountCached) {
				queryTotalCount_.emplace(qr.TotalCount());
				break;
			}
		}
	}

	std::optional<size_t> TotalCount() const noexcept override { return totalCount_; }
	std::optional<size_t> QueryTotalCount() const noexcept override { return queryTotalCount_; }
	unsigned ExternalLimit() const noexcept override { return externalLimit_; }
	unsigned ExternalOffset() const noexcept override { return externalOffset_; }

private:
	size_t totalCount_;
	std::optional<size_t> queryTotalCount_;
	unsigned externalLimit_;
	unsigned externalOffset_;
};

class HTTPServer::ReqularQueryResultsOption final : public HTTPServer::IQRSerializingOption {
public:
	ReqularQueryResultsOption(QueryResults& qr) noexcept {
		for (auto& agg : qr.GetAggregationResults()) {
			if (auto type = agg.GetType(); type == AggType::AggCount || type == AggType::AggCountCached) {
				totalCount_.emplace(qr.TotalCount());
				break;
			}
		}
	}

	std::optional<size_t> TotalCount() const noexcept override { return totalCount_; }
	std::optional<size_t> QueryTotalCount() const noexcept override { return totalCount_; }
	unsigned ExternalLimit() const noexcept override { return kDefaultLimit; }
	unsigned ExternalOffset() const noexcept override { return kDefaultOffset; }

private:
	std::optional<size_t> totalCount_;
};

class HTTPServer::ItemsQueryResultsOption final : public HTTPServer::IQRSerializingOption {
public:
	ItemsQueryResultsOption(QueryResults& qr) noexcept : totalCount_{qr.TotalCount()} {}

	std::optional<size_t> TotalCount() const noexcept override { return totalCount_; }
	std::optional<size_t> QueryTotalCount() const noexcept override { return std::nullopt; }
	unsigned ExternalLimit() const noexcept override { return kDefaultLimit; }
	unsigned ExternalOffset() const noexcept override { return kDefaultOffset; }

private:
	size_t totalCount_;
};

HTTPServer::HTTPServer(DBManager& dbMgr, LoggerWrapper& logger, const ServerConfig& serverConfig, Prometheus* prometheus,
					   IStatsWatcher* statsWatcher)
	: dbMgr_(dbMgr),
	  serverConfig_(serverConfig),
	  prometheus_(prometheus),
	  statsWatcher_(statsWatcher),
	  webRoot_(reindexer::fs::JoinPath(serverConfig.WebRoot, "")),
	  logger_(logger),
	  startTs_(system_clock_w::now()) {}

Error HTTPServer::execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults& res, http::Context& ctx) {
	const auto q = reindexer::Query::FromSQL(sqlQuery);
	const std::string_view sharding = ctx.request->params.Get("sharding"sv, "on"sv);
	switch (q.Type()) {
		case QuerySelect: {
			return (!isParameterSetOn(sharding) ? getDB<kRoleDataRead>(ctx).WithShardId(ShardingKeyType::ProxyOff, false)
												: getDB<kRoleDataRead>(ctx))
				.Select(q, res);
		}
		case QueryDelete: {
			return (!isParameterSetOn(sharding) ? getDB<kRoleDataWrite>(ctx).WithShardId(ShardingKeyType::ProxyOff, false)
												: getDB<kRoleDataWrite>(ctx))
				.Delete(q, res);
		}
		case QueryUpdate: {
			return (!isParameterSetOn(sharding) ? getDB<kRoleDataWrite>(ctx).WithShardId(ShardingKeyType::ProxyOff, false)
												: getDB<kRoleDataWrite>(ctx))
				.Update(q, res);
		}
		case QueryTruncate: {
			return (!isParameterSetOn(sharding) ? getDB<kRoleDBAdmin>(ctx).WithShardId(ShardingKeyType::ProxyOff, false)
												: getDB<kRoleDBAdmin>(ctx))
				.TruncateNamespace(q.NsName());
		}
	}
	throw Error(errParams, "unknown query type {}", int(q.Type()));
}

int HTTPServer::GetSQLQuery(http::Context& ctx) {
	const std::string sqlQuery = urldecode2(ctx.request->params.Get("q"sv));
	if (sqlQuery.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Missing `q` parameter"));
	}

	const std::string_view limitParam = ctx.request->params.Get("limit"sv);
	const std::string_view offsetParam = ctx.request->params.Get("offset"sv);

	const unsigned limit = prepareLimit(limitParam);
	const unsigned offset = prepareOffset(offsetParam);

	reindexer::QueryResults res;
	reindexer::ActiveQueryScope scope(sqlQuery);
	const auto err = execSqlQueryByType(sqlQuery, res, ctx);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}

	if (limit != kDefaultLimit || offset != kDefaultOffset) {
		return queryResults(ctx, res, TwoLevelLimitOffsetOption(res, limit, offset));
	}
	return queryResults(ctx, res, ReqularQueryResultsOption(res));
}

int HTTPServer::GetSQLSuggest(http::Context& ctx) {
	const std::string sqlQuery = urldecode2(ctx.request->params.Get("q"sv));
	if (sqlQuery.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Missing `q` parameter"));
	}

	const std::string_view posParam = ctx.request->params.Get("pos"sv);
	if (posParam.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Missing `pos` parameter"));
	}
	const std::string_view lineParam = ctx.request->params.Get("line"sv);
	if (lineParam.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Missing `line` parameter"));
	}
	const int pos = stoi(posParam);
	if (pos < 0) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "`pos` parameter should be >= 0"));
	}
	const int line = stoi(lineParam);
	if (line < 0) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "`line` parameter should be >= 0"));
	}

	size_t bytePos = 0;
	auto err = cursorPosToBytePos(sqlQuery, line, pos, bytePos);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, err.whatStr()));
	}

	logFmt(LogTrace, "GetSQLSuggest() incoming data: {}, {}", sqlQuery, bytePos);

	std::vector<std::string> suggestions;
	err = getDB<kRoleDataRead>(ctx).GetSqlSuggestions(sqlQuery, bytePos, suggestions);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, err.whatStr()));
	}

	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	auto node = builder.Array("suggests");
	for (auto& suggest : suggestions) {
		node.Put(TagName::Empty(), suggest);
	}
	node.End();
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostSQLQuery(http::Context& ctx) {
	const std::string sqlQuery = ctx.body->Read();
	if (sqlQuery.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Query is empty"));
	}
	reindexer::QueryResults res;
	reindexer::ActiveQueryScope scope(sqlQuery);
	const auto err = execSqlQueryByType(sqlQuery, res, ctx);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return queryResults(ctx, res, ReqularQueryResultsOption(res));
}

int HTTPServer::PostQuery(http::Context& ctx) {
	reindexer::Query q;
	try {
		q = Query::FromJSON(ctx.body->Read());
	} catch (Error& err) {
		return status(ctx, http::HttpStatus(err));
	}

	auto db = getDB<kRoleDataRead>(ctx);

	reindexer::QueryResults res;
	reindexer::ActiveQueryScope scope(q, QuerySelect);
	const auto err = db.Select(q, res);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return queryResults(ctx, res, ReqularQueryResultsOption(res));
}

int HTTPServer::DeleteQuery(http::Context& ctx) {
	reindexer::Query q;
	try {
		q = Query::FromJSON(ctx.body->Read());
	} catch (Error& err) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}

	auto db = getDB<kRoleDataWrite>(ctx);

	reindexer::QueryResults res;
	reindexer::ActiveQueryScope scope(q, QueryDelete);
	const auto err = db.Delete(q, res);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	builder.Put("updated", res.Count());
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::UpdateQuery(http::Context& ctx) {
	reindexer::Query q;
	try {
		q = Query::FromJSON(ctx.body->Read());
	} catch (Error& err) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}

	auto db = getDB<kRoleDataWrite>(ctx);

	reindexer::QueryResults res;
	reindexer::ActiveQueryScope scope(q, QueryUpdate);
	const auto err = db.Update(q, res);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);
	builder.Put("updated", res.Count());
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetDatabases(http::Context& ctx) {
	const std::string_view sortOrder = ctx.request->params.Get("sort_order"sv);
	ComparationResult comp = ComparationResult::NotComparable;
	if (sortOrder == "asc"sv) {
		comp = ComparationResult::Lt;
	} else if (sortOrder == "desc"sv) {
		comp = ComparationResult::Gt;
	} else if (!sortOrder.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	auto dbs = dbMgr_.EnumDatabases();

	if (comp != ComparationResult::NotComparable) {
		boost::sort::pdqsort(dbs.begin(), dbs.end(), [comp](const std::string& lhs, const std::string& rhs) {
			return collateCompare<CollateASCII>(lhs, rhs, SortingPrioritiesTable()) == comp;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", dbs.size());
		auto arrNode = builder.Array("items");
		for (auto& db : dbs) {
			arrNode.Put(TagName::Empty(), db);
		}
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostDatabase(http::Context& ctx) {
	const std::string newDbName = getNameFromJson(ctx.body->Read());

	auto dbs = dbMgr_.EnumDatabases();
	for (auto& db : dbs) {
		if (db == newDbName) {
			return status(ctx, http::HttpStatus(http::StatusBadRequest, "Database already exists"));
		}
	}

	AuthContext dummyCtx;
	AuthContext* actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData*>(ctx.clientData.get());
		assertrx(clientData);
		actx = &clientData->auth;  // -V522
	}

	const auto err = dbMgr_.OpenDatabase(newDbName, *actx, true);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::DeleteDatabase(http::Context& ctx) {
	const std::string dbName = urldecode2(ctx.request->urlParams[0]);

	AuthContext dummyCtx;
	AuthContext* actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData*>(ctx.clientData.get());
		assertrx(clientData);
		actx = &clientData->auth;  // -V522
	}

	auto err = dbMgr_.Login(dbName, *actx);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(http::StatusUnauthorized, err.whatStr()));
	}

	if (statsWatcher_) {
		// Avoid database access from the stats collecting thread during database drop
		auto statsSuspend = statsWatcher_->SuspendStatsThread();
		err = dbMgr_.DropDatabase(*actx);
	} else {
		err = dbMgr_.DropDatabase(*actx);
	}
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::GetNamespaces(http::Context& ctx) {
	const std::string_view sortOrder = ctx.request->params.Get("sort_order"sv);
	ComparationResult comp = ComparationResult::NotComparable;
	if (sortOrder == "asc"sv) {
		comp = ComparationResult::Lt;
	} else if (sortOrder == "desc"sv) {
		comp = ComparationResult::Gt;
	} else if (!sortOrder.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	std::vector<reindexer::NamespaceDef> nsDefs;
	const auto err = getDB<kRoleDataRead>(ctx).EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames());
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}

	if (comp != ComparationResult::NotComparable) {
		boost::sort::pdqsort(nsDefs.begin(), nsDefs.end(), [comp](const NamespaceDef& lhs, const NamespaceDef& rhs) {
			return collateCompare<CollateASCII>(lhs.name, rhs.name, SortingPrioritiesTable()) == comp;
		});
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefs.size());
		auto arrNode = builder.Array("items");
		for (auto& nsDef : nsDefs) {
			auto objNode = arrNode.Object();
			objNode.Put("name", nsDef.name);
		}
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetNamespace(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDataRead>(ctx);

	std::vector<reindexer::NamespaceDef> nsDefs;
	const auto err = db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}

	if (nsDefs.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer ser(ctx.writer->GetChunk());
	nsDefs[0].GetJSON(ser);
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostNamespace(http::Context& ctx) {
	const std::string body = ctx.body->Read();

	reindexer::NamespaceDef nsdef("");
	auto err = nsdef.FromJSON(giftStr(body));
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	err = db.AddNamespace(nsdef);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::DeleteNamespace(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.DropNamespace(nsName);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::TruncateNamespace(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.TruncateNamespace(nsName);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::RenameNamespace(http::Context& ctx) {
	const std::string srcNsName = urldecode2(ctx.request->urlParams[1]);
	if (srcNsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	const std::string dstNsName = urldecode2(ctx.request->urlParams[2]);
	if (dstNsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "New namespace name is not specified"));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.RenameNamespace(srcNsName, dstNsName);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}

	return status(ctx);
}

int HTTPServer::GetItems(http::Context& ctx) {
	const std::string_view sharding = ctx.request->params.Get("sharding"sv, "on"sv);
	const std::string_view shardIds = ctx.request->params.Get("with_shard_ids"sv);
	const std::string_view withVectors = ctx.request->params.Get("with_vectors"sv, "off"sv);

	auto db =
		!isParameterSetOn(sharding) ? getDB<kRoleDataRead>(ctx).WithShardId(ShardingKeyType::ProxyOff, false) : getDB<kRoleDataRead>(ctx);

	const std::string nsName = urldecode2(ctx.request->urlParams[1]);

	const std::string_view limitParam = ctx.request->params.Get("limit"sv);
	const std::string_view offsetParam = ctx.request->params.Get("offset"sv);
	const std::string_view sortField = ctx.request->params.Get("sort_field"sv);
	const std::string_view sortOrder = ctx.request->params.Get("sort_order"sv);

	const std::string filterParam = urldecode2(ctx.request->params.Get("filter"sv));
	std::string fields = urldecode2(ctx.request->params.Get("fields"sv));

	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	if (fields.empty()) {
		fields = "*";
	}
	if (isParameterSetOn(withVectors)) {
		fields += ", ";
		fields += FieldsNamesFilter::kAllVectorFieldsName;
	}

	reindexer::WrSerializer querySer;
	querySer << "SELECT " << fields << " FROM " << nsName;
	if (!filterParam.empty()) {
		querySer << " WHERE " << filterParam;
	}
	if (!sortField.empty()) {
		querySer << " ORDER BY " << sortField;

		if (sortOrder == "desc"sv) {
			querySer << " DESC";
		} else if (!sortOrder.empty() && (sortOrder != "asc")) {
			return status(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
		}
	}
	if (!limitParam.empty()) {
		querySer << " LIMIT " << prepareLimit(limitParam);
	}
	if (!offsetParam.empty()) {
		querySer << " OFFSET " << prepareOffset(offsetParam);
	}

	auto q = Query::FromSQL(querySer.Slice());
	if (ctx.request->params.Get("format"sv) != kCSVFileFmt) {
		q.ReqTotal();
	}

	int flags = kResultsCJson | kResultsWithPayloadTypes;
	if (isParameterSetOn(shardIds)) {
		flags |= kResultsNeedOutputShardId | kResultsWithShardId;
	}

	reindexer::QueryResults res(flags);
	const auto ret = db.Select(q, res);
	if (!ret.ok()) {
		return status(ctx, http::HttpStatus(ret));
	}
	return queryResults(ctx, res, ItemsQueryResultsOption(res));
}

int HTTPServer::DeleteItems(http::Context& ctx) { return modifyItems(ctx, ModeDelete); }
int HTTPServer::PutItems(http::Context& ctx) { return modifyItems(ctx, ModeUpdate); }
int HTTPServer::PostItems(http::Context& ctx) { return modifyItems(ctx, ModeInsert); }
int HTTPServer::PatchItems(http::Context& ctx) { return modifyItems(ctx, ModeUpsert); }

int HTTPServer::GetMetaList(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	const std::string_view sortOrder = ctx.request->params.Get("sort_order"sv);
	ComparationResult comp = ComparationResult::NotComparable;
	if (sortOrder == "asc"sv) {
		comp = ComparationResult::Lt;
	} else if (sortOrder == "desc"sv) {
		comp = ComparationResult::Gt;
	} else if (!sortOrder.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Invalid `sort_order` parameter"));
	}

	const std::string_view withValParam = ctx.request->params.Get("with_values"sv);
	const bool withValues = isParameterSetOn(withValParam);
	const std::string_view limitParam = ctx.request->params.Get("limit"sv);
	const std::string_view offsetParam = ctx.request->params.Get("offset"sv);
	const unsigned limit = prepareLimit(limitParam, 0);
	const unsigned offset = prepareOffset(offsetParam, 0);

	auto db = getDB<kRoleDataRead>(ctx);

	std::vector<std::string> keys;
	auto err = db.EnumMeta(nsName, keys);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	if (comp == ComparationResult::Lt) {
		boost::sort::pdqsort(keys.begin(), keys.end());
	} else if (comp == ComparationResult::Gt) {
		boost::sort::pdqsort(keys.begin(), keys.end(), std::greater<std::string>());
	}
	auto keysIt = keys.begin();
	auto keysEnd = keys.end();
	if (offset >= keys.size()) {
		keysEnd = keysIt;
	} else {
		std::advance(keysIt, offset);
	}
	if (limit > 0 && limit + offset < keys.size()) {
		keysEnd = keysIt;
		std::advance(keysEnd, limit);
	}

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("total_items", keys.size());
	JsonBuilder arrNode = builder.Array("meta");
	for (; keysIt != keysEnd; ++keysIt) {
		auto objNode = arrNode.Object();
		objNode.Put("key", *keysIt);
		if (withValues) {
			std::string value;
			err = db.GetMeta(nsName, *keysIt, value);
			if (!err.ok()) {
				return jsonStatus(ctx, http::HttpStatus(err));
			}
			objNode.Put("value", value);
		}
		objNode.End();
	}
	arrNode.End();
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::GetMetaByKey(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	const std::string key = urldecode2(ctx.request->urlParams[2]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDataRead>(ctx);

	std::string value;
	const auto err = db.GetMeta(nsName, key, value);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put("key", key);
	builder.Put("value", value);
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PutMetaByKey(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	try {
		const std::string body = ctx.body->Read();

		gason::JsonParser parser;
		const auto root = parser.Parse(giftStr(body));
		const std::string key = root["key"].As<std::string>();
		const std::string value = root["value"].As<std::string>();

		auto db = getDB<kRoleDataWrite>(ctx);
		const auto err = db.PutMeta(nsName, key, value);
		if (!err.ok()) {
			return status(ctx, http::HttpStatus(err));
		}
	} catch (const gason::Exception& ex) {
		return status(ctx, http::HttpStatus(Error(errParseJson, "Meta: {}", ex.what())));
	}
	return status(ctx);
}

int HTTPServer::DeleteMetaByKey(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	const std::string key = urldecode2(ctx.request->urlParams[2]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	auto db = getDB<kRoleDataWrite>(ctx);
	const auto err = db.DeleteMeta(nsName, key);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::GetIndexes(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDataRead>(ctx);

	std::vector<reindexer::NamespaceDef> nsDefs;
	const auto err = db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	if (nsDefs.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusNotFound, "Namespace is not found"));
	}

	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("total_items", nsDefs[0].indexes.size());
		auto arrNode = builder.Array("items");
		for (auto& idxDef : nsDefs[0].indexes) {
			arrNode.Raw("");
			idxDef.GetJSON(ser);
		}
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::PostIndex(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	const std::string json = ctx.body->Read();
	const std::string newIdxName = getNameFromJson(json);

	auto db = getDB<kRoleDBAdmin>(ctx);

	std::vector<reindexer::NamespaceDef> nsDefs;
	auto err = db.EnumNamespaces(nsDefs, EnumNamespacesOpts().WithFilter(nsName));
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}

	auto indexDef = reindexer::IndexDef::FromJSON(giftStr(json));
	if (!indexDef) {
		return status(ctx, http::HttpStatus{indexDef.error()});
	}

	if (!nsDefs.empty()) {
		auto& indexes = nsDefs[0].indexes;
		auto foundIndexIt =
			std::find_if(indexes.begin(), indexes.end(), [&newIdxName](const IndexDef& idx) { return idx.Name() == newIdxName; });
		if (foundIndexIt != indexes.end()) {
			return status(ctx, http::HttpStatus(http::StatusBadRequest, "Index already exists"));
		}
	}

	err = db.AddIndex(nsName, *indexDef);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus{err});
	}

	return status(ctx);
}

int HTTPServer::PutIndex(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	const std::string body = ctx.body->Read();
	auto indexDef = reindexer::IndexDef::FromJSON(giftStr(body));
	if (!indexDef) {
		return status(ctx, http::HttpStatus{indexDef.error()});
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.UpdateIndex(nsName, *indexDef);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus{err});
	}
	return status(ctx);
}

int HTTPServer::PutSchema(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.SetSchema(nsName, ctx.body->Read());
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::GetSchema(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return jsonStatus(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	auto db = getDB<kRoleDataRead>(ctx);

	std::string schema;
	const auto err = db.GetSchema(nsName, JsonSchemaType, schema);
	if (!err.ok()) {
		return jsonStatus(ctx, http::HttpStatus(err));
	}
	return ctx.JSON(http::StatusOK, schema.empty() ? "{}"sv : schema);
}

int HTTPServer::GetProtobufSchema(http::Context& ctx) {
	Reindexer db = getDB<kRoleDataRead>(ctx);

	std::vector<std::string> nses;
	for (auto& p : ctx.request->params) {
		if (p.name == "ns"sv || p.name == "ns[]"sv) {
			nses.emplace_back(urldecode2(p.val));
		}
	}

	WrSerializer ser;
	const auto err = db.GetProtobufSchema(ser, nses);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return ctx.String(http::StatusOK, ser.Slice());
}

int HTTPServer::DeleteIndex(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	const IndexDef idef{urldecode2(ctx.request->urlParams[2])};

	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}
	if (idef.Name().empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Index is not specified"));
	}

	auto db = getDB<kRoleDBAdmin>(ctx);
	const auto err = db.DropIndex(nsName, idef);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	return status(ctx);
}

int HTTPServer::Check(http::Context& ctx) {
	WrSerializer ser(ctx.writer->GetChunk());
	{
		JsonBuilder builder(ser);
		builder.Put("version", REINDEX_VERSION);

		size_t startTs = std::chrono::duration_cast<std::chrono::seconds>(startTs_.time_since_epoch()).count();
		size_t uptime = std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now() - startTs_).count();
		builder.Put("start_time", startTs);
		builder.Put("uptime", uptime);
		builder.Put("rpc_address", serverConfig_.RPCAddr);
		builder.Put("rpcs_address", serverConfig_.RPCsAddr);
		builder.Put("http_address", serverConfig_.HTTPAddr);
		builder.Put("https_address", serverConfig_.HTTPsAddr);
		builder.Put("urpc_address", serverConfig_.RPCUnixAddr);
		builder.Put("storage_path", serverConfig_.StoragePath);
		builder.Put("rpc_log", serverConfig_.RpcLog);
		builder.Put("http_log", serverConfig_.HttpLog);
		builder.Put("log_level", serverConfig_.LogLevel);
		builder.Put("core_log", serverConfig_.CoreLog);
		builder.Put("server_log", serverConfig_.ServerLog);
		{
			auto heapWatcher = builder.Object("heap_watcher");
			if (serverConfig_.AllocatorCacheLimit >= 0) {
				heapWatcher.Put("cache_limit_bytes", serverConfig_.AllocatorCacheLimit);
			} else {
				heapWatcher.Put("cache_limit_bytes", "disabled");
			}
			std::string allocatorCachePartStr;
			if (serverConfig_.AllocatorCachePart >= 0) {
				allocatorCachePartStr = std::to_string(int(serverConfig_.AllocatorCachePart * 100));
				allocatorCachePartStr += '%';
			} else {
				allocatorCachePartStr = "disabled";
			}
			heapWatcher.Put("cache_limit_part", allocatorCachePartStr);
		}

#if REINDEX_WITH_JEMALLOC
		if (alloc_ext::JEMallocIsAvailable()) {
			size_t val = 0, val1 = 1, sz = sizeof(size_t);

			uint64_t epoch = 1;
			sz = sizeof(epoch);
			std::ignore = alloc_ext::mallctl("epoch", &epoch, &sz, &epoch, sz);

			std::ignore = alloc_ext::mallctl("stats.resident", &val, &sz, NULL, 0);
			builder.Put("heap_size", val);

			std::ignore = alloc_ext::mallctl("stats.allocated", &val, &sz, NULL, 0);
			builder.Put("current_allocated_bytes", val);

			std::ignore = alloc_ext::mallctl("stats.active", &val1, &sz, NULL, 0);
			builder.Put("pageheap_free", val1 - val);

			std::ignore = alloc_ext::mallctl("stats.retained", &val, &sz, NULL, 0);
			builder.Put("pageheap_unmapped", val);
		}
#elif REINDEX_WITH_GPERFTOOLS
		if (alloc_ext::TCMallocIsAvailable()) {
			size_t val = 0;
			alloc_ext::instance()->GetNumericProperty("generic.current_allocated_bytes", &val);
			builder.Put("current_allocated_bytes", val);

			alloc_ext::instance()->GetNumericProperty("generic.heap_size", &val);
			builder.Put("heap_size", val);

			alloc_ext::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &val);
			builder.Put("pageheap_free", val);

			alloc_ext::instance()->GetNumericProperty("tcmalloc.pageheap_unmapped_bytes", &val);
			builder.Put("pageheap_unmapped", val);
		}
#endif
	}

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}
int HTTPServer::DocHandler(http::Context& ctx) {
	std::string path(ctx.request->path.substr(1));

	const bool endsWithSlash = (!path.empty() && path.back() == '/');
	if (endsWithSlash) {
		path.pop_back();
	}

	if (path == "" || path == "/") {
		return ctx.Redirect("face/");
	}

	web web(webRoot_);

	const auto stat = web.stat(path);
	if (stat.fstatus == fs::StatFile) {
		const bool enableCache = checkIfStartsWith("face/"sv, path);
		return web.file(ctx, http::StatusOK, path, stat.isGzip, enableCache);
	}

	if (stat.fstatus == fs::StatDir && !endsWithSlash) {
		return ctx.Redirect(path + "/");
	}

	for (; !path.empty();) {
		const std::string file = fs::JoinPath(path, "index.html");
		const auto pathStatus = web.stat(file);
		if (pathStatus.fstatus == fs::StatFile) {
			return web.file(ctx, http::StatusOK, file, pathStatus.isGzip, false);
		}

		const auto pos = path.find_last_of('/');
		if (pos == std::string::npos) {
			break;
		}

		path = path.erase(pos);
	}

	return NotFoundHandler(ctx);
}

int HTTPServer::NotFoundHandler(http::Context& ctx) { return status(ctx, http::HttpStatus(http::StatusNotFound, "Not found")); }

void HTTPServer::Start(const std::string& addr, ev::dynamic_loop& loop) {
	router_.NotFound<HTTPServer, &HTTPServer::NotFoundHandler>(this);

	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/swagger/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/face/*", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging", this);
	router_.GET<HTTPServer, &HTTPServer::DocHandler>("/facestaging/*", this);

	router_.GET<HTTPServer, &HTTPServer::Check>("/api/v1/check", this);

	router_.GET<HTTPServer, &HTTPServer::GetSQLQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostQuery>("/api/v1/db/:db/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostSQLQuery>("/api/v1/db/:db/sqlquery", this);
	router_.POST<HTTPServer, &HTTPServer::UpdateQuery>("/api/v1/db/:db/dslquery", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteQuery>("/api/v1/db/:db/query", this);
	router_.PUT<HTTPServer, &HTTPServer::UpdateQuery>("/api/v1/db/:db/query", this);
	router_.GET<HTTPServer, &HTTPServer::GetSQLSuggest>("/api/v1/db/:db/suggest", this);

	router_.GET<HTTPServer, &HTTPServer::GetProtobufSchema>("/api/v1/db/:db/protobuf_schema", this);

	router_.GET<HTTPServer, &HTTPServer::GetDatabases>("/api/v1/db", this);
	router_.POST<HTTPServer, &HTTPServer::PostDatabase>("/api/v1/db", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteDatabase>("/api/v1/db/:db", this);

	router_.GET<HTTPServer, &HTTPServer::GetNamespaces>("/api/v1/db/:db/namespaces", this);
	router_.GET<HTTPServer, &HTTPServer::GetNamespace>("/api/v1/db/:db/namespaces/:ns", this);
	router_.POST<HTTPServer, &HTTPServer::PostNamespace>("/api/v1/db/:db/namespaces", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteNamespace>("/api/v1/db/:db/namespaces/:ns", this);
	router_.DELETE<HTTPServer, &HTTPServer::TruncateNamespace>("/api/v1/db/:db/namespaces/:ns/truncate", this);
	router_.GET<HTTPServer, &HTTPServer::RenameNamespace>("/api/v1/db/:db/namespaces/:ns/rename/:nns", this);

	router_.GET<HTTPServer, &HTTPServer::GetItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.PUT<HTTPServer, &HTTPServer::PutItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.POST<HTTPServer, &HTTPServer::PostItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.PATCH<HTTPServer, &HTTPServer::PatchItems>("/api/v1/db/:db/namespaces/:ns/items", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteItems>("/api/v1/db/:db/namespaces/:ns/items", this);

	router_.GET<HTTPServer, &HTTPServer::GetIndexes>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.POST<HTTPServer, &HTTPServer::PostIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.PUT<HTTPServer, &HTTPServer::PutIndex>("/api/v1/db/:db/namespaces/:ns/indexes", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteIndex>("/api/v1/db/:db/namespaces/:ns/indexes/:idx", this);
	router_.PUT<HTTPServer, &HTTPServer::PutSchema>("/api/v1/db/:db/namespaces/:ns/schema", this);
	router_.GET<HTTPServer, &HTTPServer::GetSchema>("/api/v1/db/:db/namespaces/:ns/schema", this);

	router_.GET<HTTPServer, &HTTPServer::GetMetaList>("/api/v1/db/:db/namespaces/:ns/metalist", this);
	router_.GET<HTTPServer, &HTTPServer::GetMetaByKey>("/api/v1/db/:db/namespaces/:ns/metabykey/:key", this);
	router_.PUT<HTTPServer, &HTTPServer::PutMetaByKey>("/api/v1/db/:db/namespaces/:ns/metabykey", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteMetaByKey>("/api/v1/db/:db/namespaces/:ns/metabykey/:key", this);

	router_.POST<HTTPServer, &HTTPServer::BeginTx>("/api/v1/db/:db/namespaces/:ns/transactions/begin", this);
	router_.POST<HTTPServer, &HTTPServer::CommitTx>("/api/v1/db/:db/transactions/:tx/commit", this);
	router_.POST<HTTPServer, &HTTPServer::RollbackTx>("/api/v1/db/:db/transactions/:tx/rollback", this);
	router_.PUT<HTTPServer, &HTTPServer::PutItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.POST<HTTPServer, &HTTPServer::PostItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.PATCH<HTTPServer, &HTTPServer::PatchItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteItemsTx>("/api/v1/db/:db/transactions/:tx/items", this);
	router_.GET<HTTPServer, &HTTPServer::GetSQLQueryTx>("/api/v1/db/:db/transactions/:tx/query", this);
	router_.DELETE<HTTPServer, &HTTPServer::DeleteQueryTx>("/api/v1/db/:db/transactions/:tx/query", this);
	router_.POST<HTTPServer, &HTTPServer::PostMemReset>("/api/v1/allocator/drop_cache", this);
	router_.GET<HTTPServer, &HTTPServer::GetMemInfo>("/api/v1/allocator/info", this);

	router_.GET<HTTPServer, &HTTPServer::GetRole>("/api/v1/user/role", this);

	router_.GET<HTTPServer, &HTTPServer::GetDefaultConfigs>("/api/v1/db/default_configs", this);

	router_.OnResponse(this, &HTTPServer::OnResponse);
	router_.Middleware<HTTPServer, &HTTPServer::CheckAuth>(this);

	if (logger_) {
		router_.Logger<HTTPServer, &HTTPServer::Logger>(this);
	}

	if (serverConfig_.DebugPprof) {
		pprof_.Attach(router_);
	}
	if (prometheus_) {
		prometheus_->Attach(router_);
	}

	auto sslCtx =
		serverConfig_.HTTPsAddr == addr ? openssl::create_server_context(serverConfig_.SslCertPath, serverConfig_.SslKeyPath) : nullptr;
	if (serverConfig_.HttpThreadingMode == ServerConfig::kDedicatedThreading) {
		listener_ = std::make_unique<ForkedListener>(loop, http::ServerConnection::NewFactory(router_, serverConfig_.MaxHttpReqSize),
													 std::move(sslCtx));
	} else {
		listener_ = std::make_unique<Listener<ListenerType::Shared>>(
			loop, http::ServerConnection::NewFactory(router_, serverConfig_.MaxHttpReqSize), std::move(sslCtx));
	}
	deadlineChecker_.set<HTTPServer, &HTTPServer::deadlineTimerCb>(this);
	deadlineChecker_.set(loop);
	deadlineChecker_.start(std::chrono::duration_cast<std::chrono::seconds>(kTxDeadlineCheckPeriod).count(),
						   std::chrono::duration_cast<std::chrono::seconds>(kTxDeadlineCheckPeriod).count());

	listener_->Bind(addr, socket_domain::tcp);
}

Error HTTPServer::modifyItem(Reindexer& db, std::string_view nsName, Item& item, ItemModifyMode mode) {
	switch (mode) {
		case ModeUpsert:
			return db.Upsert(nsName, item);
		case ModeDelete:
			return db.Delete(nsName, item);
		case ModeInsert:
			return db.Insert(nsName, item);
		case ModeUpdate:
			return db.Update(nsName, item);
	}
	return {};
}

Error HTTPServer::modifyItem(Reindexer& db, std::string_view nsName, Item& item, QueryResults& qr, ItemModifyMode mode) {
	switch (mode) {
		case ModeUpsert:
			return db.Upsert(nsName, item, qr);
		case ModeDelete:
			return db.Delete(nsName, item, qr);
		case ModeInsert:
			return db.Insert(nsName, item, qr);
		case ModeUpdate:
			return db.Update(nsName, item, qr);
	}
	return {};
}

int HTTPServer::modifyItemsJSON(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode) {
	auto db = getDB<kRoleDataWrite>(ctx);
	std::string itemJson = ctx.body->Read();
	int cnt = 0;
	std::vector<std::string> updatedItems;

	if (itemJson.size()) {
		char* jsonPtr = &itemJson[0];
		size_t jsonLeft = itemJson.size();
		while (jsonPtr && *jsonPtr) {
			Item item = db.NewItem(nsName);
			if (!item.Status().ok()) {
				return jsonStatus(ctx, http::HttpStatus(item.Status()));
			}
			const char* prevPtr = jsonPtr;
			const auto str = std::string_view(jsonPtr, jsonLeft);
			if (jsonPtr != &itemJson[0] && isBlank(str)) {
				break;
			}
			auto err = item.Unsafe().FromJSON(str, &jsonPtr, false);  // TODO: for mode == ModeDelete deserialize PK and sharding key only
			jsonLeft -= (jsonPtr - prevPtr);
			if (!err.ok()) {
				return jsonStatus(ctx, http::HttpStatus(err));
			}

			item.SetPrecepts(precepts);
			err = modifyItem(db, nsName, item, mode);
			if (!err.ok()) {
				return jsonStatus(ctx, http::HttpStatus(err));
			}

			if (item.GetID() != -1) {
				++cnt;
				if (!precepts.empty()) {
					updatedItems.emplace_back(item.GetJSON());
				}
			}
		}
	}

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put(kParamUpdated, cnt);
	builder.Put(kParamSuccess, true);
	if (!precepts.empty()) {
		auto itemsArray = builder.Array(kParamItems);
		for (const std::string& item : updatedItems) {
			itemsArray.Raw(item);
		}
		itemsArray.End();
	}
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::modifyItemsMsgPack(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode) {
	QueryResults qr;
	int totalItems = 0;

	auto db = getDB<kRoleDataWrite>(ctx);
	const std::string sbuffer = ctx.body->Read();

	size_t length = sbuffer.size();
	size_t offset = 0;

	while (offset < length) {
		Item item = db.NewItem(nsName);
		if (!item.Status().ok()) {
			return msgpackStatus(ctx, http::HttpStatus(item.Status()));
		}

		auto err = item.FromMsgPack(std::string_view(sbuffer.data(), sbuffer.size()), offset);
		if (!err.ok()) {
			return msgpackStatus(ctx, http::HttpStatus(err));
		}

		item.SetPrecepts(precepts);
		err = precepts.empty() ? modifyItem(db, nsName, item, mode) : modifyItem(db, nsName, item, qr, mode);
		if (!err.ok()) {
			return msgpackStatus(ctx, http::HttpStatus(err));
		}

		if (item.GetID() != -1) {
			++totalItems;
		}
	}

	WrSerializer ser(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(ser, ObjType::TypeObject, precepts.empty() ? 2 : 3);
	msgpackBuilder.Put(kParamUpdated, totalItems);
	msgpackBuilder.Put(kParamSuccess, true);
	if (!precepts.empty()) {
		auto itemsArray = msgpackBuilder.Array(kParamItems, qr.Count());
		for (auto& it : qr) {
			const auto err = it.GetMsgPack(ser, false);
			if (!err.ok()) {
				return msgpackStatus(ctx, http::HttpStatus(err));
			}
		}
		itemsArray.End();
	}

	return ctx.MSGPACK(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::modifyItemsProtobuf(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode) {
	WrSerializer ser(ctx.writer->GetChunk());
	ProtobufBuilder builder(&ser);

	auto sendResponse = [&](int items, const Error& err) {
		if (err.ok()) {
			builder.Put(kProtoModifyResultsFields.at(kParamUpdated), int(items));
			builder.Put(kProtoModifyResultsFields.at(kParamSuccess), err.ok());
		} else {
			builder.Put(kProtoErrorResultsFields.at(kParamDescription), err.whatStr());
			builder.Put(kProtoErrorResultsFields.at(kParamResponseCode), err.code());
		}
		return ctx.Protobuf(reindexer::net::http::HttpStatus::errCodeToHttpStatus(err.code()), ser.DetachChunk());
	};

	auto db = getDB<kRoleDataWrite>(ctx);
	Item item = db.NewItem(nsName);
	if (!item.Status().ok()) {
		return sendResponse(0, item.Status());
	}

	const std::string sbuffer = ctx.body->Read();
	auto err = item.FromProtobuf(sbuffer);
	if (!err.ok()) {
		return sendResponse(0, err);
	}

	const bool hasPrecepts = !precepts.empty();
	item.SetPrecepts(std::move(precepts));
	err = modifyItem(db, nsName, item, mode);
	if (!err.ok()) {
		return sendResponse(0, item.Status());
	}

	int totalItems = 0;
	if (item.GetID() != -1) {
		if (hasPrecepts) {
			auto object = builder.Object(kProtoModifyResultsFields.at(kParamItems));
			err = item.GetProtobuf(ser);
			object.End();
		}
		++totalItems;
	}

	return sendResponse(totalItems, item.Status());
}

int HTTPServer::modifyItemsTxJSON(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode) {
	std::string itemJson = ctx.body->Read();

	if (!itemJson.empty()) {
		char* jsonPtr = &itemJson[0];
		size_t jsonLeft = itemJson.size();
		while (jsonPtr && *jsonPtr) {
			Item item = tx.NewItem();
			if (!item.Status().ok()) {
				http::HttpStatus httpStatus(item.Status());
				return jsonStatus(ctx, httpStatus);
			}
			char* prevPtr = jsonPtr;
			const auto str = std::string_view(jsonPtr, jsonLeft);
			if (jsonPtr != &itemJson[0] && isBlank(str)) {
				break;
			}
			auto err = item.FromJSON(std::string_view(jsonPtr, jsonLeft), &jsonPtr,
									 false);  // TODO: for mode == ModeDelete deserialize PK and sharding key only
			jsonLeft -= (jsonPtr - prevPtr);
			if (!err.ok()) {
				http::HttpStatus httpStatus(err);
				return jsonStatus(ctx, httpStatus);
			}
			item.SetPrecepts(precepts);
			err = tx.Modify(std::move(item), mode);
			if (!err.ok()) {
				return jsonStatus(ctx, http::HttpStatus(err));
			}
		}
	}

	return jsonStatus(ctx);
}

int HTTPServer::modifyItemsTxProtobuf(http::Context& ctx, Transaction& tx, std::vector<string>&& precepts, ItemModifyMode mode) {
	const std::string sbuffer = ctx.body->Read();

	Item item = tx.NewItem();
	if (!item.Status().ok()) {
		return protobufStatus(ctx, http::HttpStatus(item.Status()));
	}

	auto err = item.FromProtobuf(sbuffer);
	if (!err.ok()) {
		return protobufStatus(ctx, http::HttpStatus(err));
	}

	item.SetPrecepts(precepts);
	err = tx.Modify(std::move(item), mode);
	if (!err.ok()) {
		return protobufStatus(ctx, http::HttpStatus(err));
	}

	return protobufStatus(ctx);
}

int HTTPServer::modifyItemsTxMsgPack(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode) {
	const std::string sbuffer = ctx.body->Read();
	const size_t length = sbuffer.size();
	size_t offset = 0;

	while (offset < length) {
		Item item = tx.NewItem();
		if (!item.Status().ok()) {
			return msgpackStatus(ctx, http::HttpStatus(item.Status()));
		}

		auto err = item.FromMsgPack(sbuffer, offset);
		if (!err.ok()) {
			return msgpackStatus(ctx, http::HttpStatus(err));
		}

		item.SetPrecepts(precepts);
		err = tx.Modify(std::move(item), mode);
		if (!err.ok()) {
			return msgpackStatus(ctx, http::HttpStatus(err));
		}
	}

	return msgpackStatus(ctx);
}

int HTTPServer::modifyItems(http::Context& ctx, ItemModifyMode mode) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	std::vector<std::string> precepts;
	for (auto& p : ctx.request->params) {
		if (p.name == "precepts"sv) {
			precepts.emplace_back(urldecode2(p.val));
		}
	}

	switch (getDataFormat(ctx)) {
		case DataFormat::JSON:
			return modifyItemsJSON(ctx, nsName, std::move(precepts), mode);
		case DataFormat::MsgPack:
			return modifyItemsMsgPack(ctx, nsName, std::move(precepts), mode);
		case DataFormat::Protobuf:
			return modifyItemsProtobuf(ctx, nsName, std::move(precepts), mode);
		case DataFormat::CSVFile:
		default:
			throwUnsupportedOpFormat(ctx);
	}
}

int HTTPServer::modifyItemsTx(http::Context& ctx, ItemModifyMode mode) {
	std::string dbName;
	auto db = getDB<kRoleDataWrite>(ctx, &dbName);
	const std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	std::vector<std::string> precepts;
	for (auto& p : ctx.request->params) {
		if (p.name == "precepts"sv) {
			precepts.emplace_back(urldecode2(p.val));
		}
	}

	auto tx = getTx(dbName, txId);
	switch (getDataFormat(ctx)) {
		case DataFormat::JSON:
			return modifyItemsTxJSON(ctx, *tx, std::move(precepts), mode);
		case DataFormat::MsgPack:
			return modifyItemsTxMsgPack(ctx, *tx, std::move(precepts), mode);
		case DataFormat::Protobuf:
			return modifyItemsTxProtobuf(ctx, *tx, std::move(precepts), mode);
		case DataFormat::CSVFile:
		default:
			throwUnsupportedOpFormat(ctx);
	}
}

int HTTPServer::queryResultsJSON(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption, bool withColumns,
								 int width) {
	const auto offset = qrOption.ExternalOffset();
	const auto limit = qrOption.ExternalLimit();

	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);

	auto iarray = builder.Array(kParamItems);
	auto it = offset < res.Count() ? (res.begin() + offset) : res.end();
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	WrSerializer itemSer;
	std::optional<Reindexer> db;
	auto cjsonViewer = [this, &res, &ctx, &db](std::string_view cjson) {
		if (!db.has_value()) {
			db.emplace(getDB<kRoleDataRead>(ctx));
		}
		auto item = db->NewItem(res.GetNamespaces()[0]);
		const auto err = item.FromCJSON(cjson);
		if (!err.ok()) {
			throw Error(err.code(), "Unable to parse CJSON for WAL item: {}", err.whatStr());
		}
		return std::string(item.GetJSON());
	};
	const bool isWALQuery = res.IsWALQuery();
	for (size_t i = 0; it != res.end() && i < limit; ++i, ++it) {
		if (!isWALQuery) {
			iarray.Raw("");
			if (withColumns) {
				itemSer.Reset();
				const auto err = it.GetJSON(itemSer, false);
				if (!err.ok()) {
					return jsonStatus(ctx, http::HttpStatus(err));
				}
				jsonData.emplace_back(itemSer.Slice());
				ser.Write(itemSer.Slice());
			} else {
				const auto err = it.GetJSON(ser, false);
				if (!err.ok()) {
					return jsonStatus(ctx, http::HttpStatus(err));
				}
			}
		} else {
			auto obj = iarray.Object();
			{
				auto lsnObj = obj.Object(kWALParamLsn);
				it.GetLSN().GetJSON(lsnObj);
			}
			if (!it.IsRaw()) {
				iarray.Raw(kWALParamItem, "");
				if (withColumns) {
					itemSer.Reset();
					const auto err = it.GetJSON(itemSer, false);
					if (!err.ok()) {
						return jsonStatus(ctx, http::HttpStatus(err));
					}
					jsonData.emplace_back(itemSer.Slice());
					ser.Write(itemSer.Slice());
				} else {
					const auto err = it.GetJSON(ser, false);
					if (!err.ok()) {
						return jsonStatus(ctx, http::HttpStatus(err));
					}
				}
			} else {
				reindexer::WALRecord rec(it.GetRaw());
				rec.GetJSON(obj, cjsonViewer);
			}
		}

		if (i == offset) {
			ser.Reserve(ser.Len() * (std::min(limit, unsigned(res.Count() - offset)) + 1));
		}
	}
	iarray.End();

	auto& aggs = res.GetAggregationResults();
	if (!aggs.empty()) {
		auto arrNode = builder.Array(kParamAggregations);
		for (auto& agg : aggs) {
			arrNode.Raw("");
			agg.GetJSON(ser);
		}
	}

	queryResultParams(builder, res, std::move(jsonData), qrOption, withColumns, width);
	builder.End();

	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::queryResultsCSV(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption) {
	if (!res.GetAggregationResults().empty()) {
		throw Error(errForbidden, "Aggregations are not supported in CSV");
	}
	if (!res.GetExplainResults().empty()) {
		throw Error(errForbidden, "Explain is not supported in CSV");
	}

	const size_t kChunkMaxSize = 0x1000;
	auto createChunk = [](const WrSerializer& from, WrSerializer& to) {
		char szBuf[64];
		size_t l = u32toax(from.Len(), szBuf) - szBuf;
		to << std::string_view(szBuf, l);
		to << "\r\n";
		to << from.Slice();
		to << "\r\n";
	};

	auto createCSVHeaders = [&res](WrSerializer& ser, const CsvOrdering& ordering) {
		const auto& tm = res.GetTagsMatcher(0);
		for (auto it = ordering.begin(); it != ordering.end(); ++it) {
			if (it != ordering.begin()) {
				ser << ',';
			}
			ser << tm.tag2name(*it);
		}
		if (res.ToLocalQr().joined_.empty()) {
			ser << "\n";
		} else {
			ser << ",joined_namespaces\n";
		}
	};

	WrSerializer wrSerRes(ctx.writer->GetChunk()), wrSerChunk;
	wrSerChunk.Reserve(kChunkMaxSize);
	const auto schema = res.GetSchema(0);
	const bool withSchema = schema && !schema->IsEmpty();
	if (!res.IsLocal() && !withSchema) {
		throw Error(errLogic, "Uploads in csv format without a namespace scheme are allowed only for local queries");
	}

	const auto limit = qrOption.ExternalLimit();
	const auto offset = qrOption.ExternalOffset();
	auto ordering =
		withSchema ? CsvOrdering{schema->MakeCsvTagOrdering(res.GetTagsMatcher(0))} : res.ToLocalQr().MakeCSVTagOrdering(limit, offset);

	createCSVHeaders(wrSerChunk, ordering);
	auto it = res.begin() + offset;
	for (size_t i = 0; it != res.end() && i < limit; ++i, ++it) {
		auto err = it.GetCSV(wrSerChunk, ordering);
		if (!err.ok()) {
			throw Error(err.code(), "Unable to get {} item as CSV: {}", i, err.whatStr());
		}

		wrSerChunk << '\n';

		if (wrSerChunk.Len() > kChunkMaxSize) {
			createChunk(wrSerChunk, wrSerRes);
			wrSerChunk.Reset();
		}
	}

	if (wrSerChunk.Len()) {
		createChunk(wrSerChunk, wrSerRes);
	}

	return ctx.CSV(http::StatusOK, wrSerRes.DetachChunk());
}

int HTTPServer::queryResultsMsgPack(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption,
									bool withColumns, int width) {
	int paramsToSend = 3;
	if (!res.GetAggregationResults().empty()) {
		++paramsToSend;
	}
	if (!res.GetExplainResults().empty()) {
		++paramsToSend;
	}
	if (withColumns) {
		++paramsToSend;
	}
	if (qrOption.TotalCount().has_value()) {
		++paramsToSend;
	}
	if (qrOption.QueryTotalCount().has_value()) {
		++paramsToSend;
	}

	WrSerializer ser(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(ser, ObjType::TypeObject, paramsToSend);

	const auto offset = qrOption.ExternalOffset();
	const auto limit = qrOption.ExternalLimit();
	WrSerializer itemSer;
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	auto itemsArray = msgpackBuilder.Array(kParamItems, std::min(size_t(limit), size_t(res.Count() - offset)));
	auto it = res.begin() + offset;
	for (size_t i = 0; it != res.end() && i < limit; ++i, ++it) {
		auto err = it.GetMsgPack(ser, false);
		if (!err.ok()) {
			return msgpackStatus(ctx, http::HttpStatus(err));
		}
		if (withColumns) {
			itemSer.Reset();
			err = it.GetJSON(itemSer, false);
			if (!err.ok()) {
				return msgpackStatus(ctx, http::HttpStatus(err));
			}
			jsonData.emplace_back(itemSer.Slice());
		}
	}
	itemsArray.End();

	if (!res.GetAggregationResults().empty()) {
		auto& aggs = res.GetAggregationResults();
		auto aggregationsArray = msgpackBuilder.Array(kParamAggregations, aggs.size());
		for (auto& agg : aggs) {
			agg.GetMsgPack(ser);
		}
	}

	queryResultParams(msgpackBuilder, res, std::move(jsonData), qrOption, withColumns, width);
	msgpackBuilder.End();

	return ctx.MSGPACK(http::StatusOK, ser.DetachChunk());
}

int HTTPServer::queryResultsProtobuf(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption,
									 bool withColumns, int width) {
	WrSerializer ser(ctx.writer->GetChunk());
	ProtobufBuilder protobufBuilder(&ser);

	const auto offset = qrOption.ExternalOffset();
	const auto limit = qrOption.ExternalLimit();
	auto& lres = res.ToLocalQr();
	WrSerializer itemSer;
	std::vector<std::string> jsonData;
	if (withColumns) {
		size_t size = res.Count();
		if (limit > offset && limit - offset < size) {
			size = limit - offset;
		}
		jsonData.reserve(size);
	}
	for (size_t i = offset; i < lres.Count() && i < offset + limit; i++) {
		auto it = lres[i];
		auto err = it.GetProtobuf(ser);
		if (!err.ok()) {
			return protobufStatus(ctx, http::HttpStatus(err));
		}
		if (withColumns) {
			itemSer.Reset();
			err = it.GetJSON(itemSer, false);
			if (!err.ok()) {
				return protobufStatus(ctx, http::HttpStatus(err));
			}
			jsonData.emplace_back(itemSer.Slice());
		}
	}

	const TagName aggregationField = kProtoQueryResultsFields.at(kParamAggregations);
	for (auto& agg : res.GetAggregationResults()) {
		auto aggregation = protobufBuilder.Object(aggregationField);
		agg.GetProtobuf(ser);
		aggregation.End();
	}

	const TagName nsField = kProtoQueryResultsFields.at(kParamNamespaces);
	h_vector<std::string_view, 1> namespaces(res.GetNamespaces());
	for (auto ns : namespaces) {
		protobufBuilder.Put(nsField, ns);
	}

	protobufBuilder.Put(kProtoQueryResultsFields.at(kParamCacheEnabled), res.IsCacheEnabled() && !res.IsWALQuery());

	if (!res.GetExplainResults().empty()) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamExplain), res.GetExplainResults());
	}

	if (auto totalCount = qrOption.TotalCount(); totalCount.has_value()) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamTotalItems), int64_t(*totalCount));
	}
	if (auto queryTotalCount = qrOption.QueryTotalCount(); queryTotalCount.has_value()) {
		protobufBuilder.Put(kProtoQueryResultsFields.at(kParamQueryTotalItems), int64_t(*queryTotalCount));
	}

	if (withColumns) {
		reindexer::TableCalculator tableCalculator(std::move(jsonData), width);
		const auto& header = tableCalculator.GetHeader();
		auto& columnsSettings = tableCalculator.GetColumnsSettings();
		for (const auto& part : header) {
			ColumnData& data = columnsSettings[part];
			auto parametersObj = protobufBuilder.Object(kProtoQueryResultsFields.at(kParamColumns));
			parametersObj.Put(kProtoColumnsFields.at(kParamName), part);
			parametersObj.Put(kProtoColumnsFields.at(kParamWidthPercents), data.widthTerminalPercentage);
			parametersObj.Put(kProtoColumnsFields.at(kParamMaxChars), data.maxWidthCh);
			parametersObj.Put(kProtoColumnsFields.at(kParamWidthChars), data.widthCh);
			parametersObj.End();
		}
	}

	protobufBuilder.End();
	return ctx.Protobuf(http::StatusOK, ser.DetachChunk());
}

template <typename Builder>
void HTTPServer::queryResultParams(Builder& builder, reindexer::QueryResults& res, std::vector<std::string>&& jsonData,
								   const IQRSerializingOption& qrOption, bool withColumns, int width) {
	h_vector<std::string_view, 1> namespaces(res.GetNamespaces());
	auto namespacesArray = builder.Array(kParamNamespaces, namespaces.size());
	for (auto ns : namespaces) {
		namespacesArray.Put(TagName::Empty(), ns);
	}
	namespacesArray.End();

	builder.Put(kParamCacheEnabled, res.IsCacheEnabled() && !res.IsWALQuery());

	if (!res.GetExplainResults().empty()) {
		builder.Json(kParamExplain, res.GetExplainResults());
	}

	if (auto totalCount = qrOption.TotalCount(); totalCount.has_value()) {
		builder.Put(kParamTotalItems, int64_t(*totalCount));
	}
	if (auto queryTotalCount = qrOption.QueryTotalCount(); queryTotalCount.has_value()) {
		builder.Put(kParamQueryTotalItems, int64_t(*queryTotalCount));
	}

	if (withColumns) {
		reindexer::TableCalculator tableCalculator(std::move(jsonData), width);
		const auto& header = tableCalculator.GetHeader();
		auto& columnsSettings = tableCalculator.GetColumnsSettings();
		auto headerArray = builder.Array(kParamColumns, header.size());
		for (const auto& part : header) {
			ColumnData& data = columnsSettings[part];
			auto parametersObj = headerArray.Object(TagName::Empty(), 4);
			parametersObj.Put(kParamName, part);
			parametersObj.Put(kParamWidthPercents, data.widthTerminalPercentage);
			parametersObj.Put(kParamMaxChars, data.maxWidthCh);
			parametersObj.Put(kParamWidthChars, data.widthCh);
		}
	}
}

int HTTPServer::queryResults(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption) {
	const std::string_view widthParam = ctx.request->params.Get("width"sv);
	const int width = widthParam.empty() ? 0 : stoi(widthParam);

	const std::string_view withColumnsParam = ctx.request->params.Get("with_columns"sv);
	const bool withColumns = (isParameterSetOn(withColumnsParam) && (width > 0)) ? true : false;

	switch (getDataFormat(ctx)) {
		case DataFormat::JSON:
			return queryResultsJSON(ctx, res, qrOption, withColumns, width);
		case DataFormat::MsgPack:
			return queryResultsMsgPack(ctx, res, qrOption, withColumns, width);
		case DataFormat::Protobuf:
			return queryResultsProtobuf(ctx, res, qrOption, withColumns, width);
		case DataFormat::CSVFile: {
			CounterGuardAIRL32 cg(currentCsvDownloads_);
			if (currentCsvDownloads_.load() > kMaxConcurrentCsvDownloads) {
				throw Error(errForbidden, "Unable to start new CSV download. Limit of concurrent downloads is {}",
							kMaxConcurrentCsvDownloads);
			}
			return queryResultsCSV(ctx, res, qrOption);
		}
		default:
			throwUnsupportedOpFormat(ctx);
	}
}

int HTTPServer::statusOK(http::Context& ctx, chunk&& chunk) {
	switch (getDataFormat(ctx)) {
		case DataFormat::JSON:
			return ctx.JSON(http::StatusOK, std::move(chunk));
		case DataFormat::MsgPack:
			return ctx.MSGPACK(http::StatusOK, std::move(chunk));
		case DataFormat::Protobuf:
			return ctx.Protobuf(http::StatusOK, std::move(chunk));
		case DataFormat::CSVFile:
		default:
			throw Error(errLogic, "'HTTPServer::statusOK' is not implemented for '{}' format", ctx.request->params.Get("format"sv));
	}
}

int HTTPServer::status(http::Context& ctx, const http::HttpStatus& status) {
	switch (getDataFormat(ctx)) {
		case DataFormat::JSON:
			return jsonStatus(ctx, status);
		case DataFormat::MsgPack:
			return msgpackStatus(ctx, status);
		case DataFormat::Protobuf:
			return protobufStatus(ctx, status);
		case DataFormat::CSVFile:
		default:
			throw Error(errLogic, "'HTTPServer::status' is not implemented for '{}' format", ctx.request->params.Get("format"sv));
	}
}

int HTTPServer::msgpackStatus(http::Context& ctx, const http::HttpStatus& status) {
	WrSerializer ser(ctx.writer->GetChunk());
	MsgPackBuilder msgpackBuilder(ser, ObjType::TypeObject, 3);
	msgpackBuilder.Put(kParamSuccess, status.code == http::StatusOK);
	msgpackBuilder.Put(kParamResponseCode, status.code);
	msgpackBuilder.Put(kParamDescription, status.what);
	msgpackBuilder.End();
	return ctx.MSGPACK(status.code, ser.DetachChunk());
}

int HTTPServer::jsonStatus(http::Context& ctx, const http::HttpStatus& status) {
	WrSerializer ser(ctx.writer->GetChunk());
	JsonBuilder builder(ser);
	builder.Put(kParamSuccess, status.code == http::StatusOK);
	builder.Put(kParamResponseCode, int(status.code));
	builder.Put(kParamDescription, status.what);
	builder.End();
	return ctx.JSON(status.code, ser.DetachChunk());
}

int HTTPServer::protobufStatus(http::Context& ctx, const http::HttpStatus& status) {
	WrSerializer ser(ctx.writer->GetChunk());
	ProtobufBuilder builder(&ser);
	builder.Put(kProtoErrorResultsFields.at(kParamSuccess), status.code == http::StatusOK);
	builder.Put(kProtoErrorResultsFields.at(kParamResponseCode), int(status.code));
	builder.Put(kProtoErrorResultsFields.at(kParamDescription), status.what);
	builder.End();
	return ctx.Protobuf(status.code, ser.DetachChunk());
}

unsigned HTTPServer::prepareLimit(std::string_view limitParam, int limitDefault) {
	int limit = limitDefault;

	if (!limitParam.empty()) {
		limit = stoi(limitParam);
		if (limit < 0) {
			limit = 0;
		}
	}

	return static_cast<unsigned>(limit);
}

unsigned HTTPServer::prepareOffset(std::string_view offsetParam, int offsetDefault) {
	int offset = offsetDefault;

	if (!offsetParam.empty()) {
		offset = stoi(offsetParam);
		if (offset < 0) {
			offset = 0;
		}
	}

	return static_cast<unsigned>(offset);
}

int HTTPServer::modifyQueryTxImpl(http::Context& ctx, const std::string& dbName, std::string_view txId, Query& q) {
	reindexer::QueryResults res;
	auto tx = getTx(dbName, txId);
	if (!q.GetMergeQueries().empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Merged sub-queries are not allowed inside TX"));
	}
	if (!q.GetJoinQueries().empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Joined sub-queries are not allowed inside TX"));
	}
	auto err = tx->Modify(std::move(q));
	return status(ctx, http::HttpStatus(err));
}

template <UserRole role>
Reindexer HTTPServer::getDB(http::Context& ctx, std::string* dbNameOut) {
	std::string dbName = urldecode2(ctx.request->urlParams[0]);

	AuthContext dummyCtx;
	AuthContext* actx = &dummyCtx;
	if (!dbMgr_.IsNoSecurity()) {
		auto clientData = dynamic_cast<HTTPClientData*>(ctx.clientData.get());
		assertrx(clientData);
		actx = &clientData->auth;  // -V522
	}

	auto err = dbMgr_.OpenDatabase(dbName, *actx, false);
	if (!err.ok()) {
		throw http::HttpStatus(err);
	}
	if (dbNameOut) {
		*dbNameOut = std::move(dbName);
	}

	Reindexer* db = nullptr;
	err = actx->GetDB<AuthContext::CalledFrom::HTTPServer>(role, &db);
	if (!err.ok()) {
		throw http::HttpStatus(err);
	}

	assertrx(db);
	std::string_view timeoutHeader = ctx.request->headers.Get("Request-Timeout"sv);
	std::optional<int> timeoutSec;
	if (!timeoutHeader.empty()) {
		timeoutSec = try_stoi(timeoutHeader);
		if (!timeoutSec.has_value()) [[unlikely]] {
			logger_.warn("Unable to get integer value from 'Request-Timeout'-header('{}'). Using default value", timeoutHeader);
		}
	}
	std::chrono::seconds timeout;

	if constexpr (role == kUnauthorized || role == kRoleNone) {
		throw Error(errLogic, "Unexpected user's role");
	} else if constexpr (role == kRoleDataRead) {
		timeout = timeoutSec.has_value() ? std::chrono::seconds(timeoutSec.value()) : serverConfig_.HttpReadTimeout;
	} else if constexpr (role >= kRoleDataWrite) {
		timeout = timeoutSec.has_value() ? std::chrono::seconds(timeoutSec.value()) : serverConfig_.HttpWriteTimeout();
	}
	const auto needMaskDSN = NeedMaskingDSN(actx->UserRights() < kRoleDBAdmin);
	return db->NeedTraceActivity()
			   ? db->WithContextParams(timeout, needMaskDSN, ctx.request->clientAddr, std::string(ctx.request->headers.Get("User-Agent")))
			   : db->WithContextParams(timeout, needMaskDSN, std::string(), std::string());
}

std::string HTTPServer::getNameFromJson(std::string_view json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		return root["name"].As<std::string>();
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "getNameFromJson: {}", ex.what());
	}
}

std::shared_ptr<Transaction> HTTPServer::getTx(const std::string& dbName, std::string_view txId) {
	lock_guard lck(txMtx_);
	auto found = txMap_.find(txId);
	if (found == txMap_.end()) {
		throw http::HttpStatus(Error(errNotFound, "Invalid tx id"sv));
	}
	if (!iequals(found.value().dbName, dbName)) {
		throw http::HttpStatus(Error(errLogic, "Unexpected database name for this tx"sv));
	}
	found.value().txDeadline = TxDeadlineClock::now_coarse() + serverConfig_.TxIdleTimeout;
	return found.value().tx;
}

std::string HTTPServer::addTx(std::string dbName, Transaction&& tx) {
	const auto now = TxDeadlineClock::now_coarse();
	const auto ts = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
	std::string txId = randStringAlph(kTxIdLen) + "_" + std::to_string(ts.count());
	TxInfo txInfo;
	txInfo.tx = std::make_shared<Transaction>(std::move(tx));
	txInfo.dbName = std::move(dbName);
	txInfo.txDeadline = now + serverConfig_.TxIdleTimeout;

	lock_guard lck(txMtx_);
	auto result = txMap_.try_emplace(txId, std::move(txInfo));
	if (!result.second) {
		throw Error(errLogic, "Tx id conflict");
	}
	return txId;
}

void HTTPServer::removeTx(const std::string& dbName, std::string_view txId) {
	lock_guard lck(txMtx_);
	const auto found = txMap_.find(txId);
	if (found == txMap_.end() || !iequals(found.value().dbName, dbName)) {
		throw Error(errNotFound, "Invalid tx id");
	}
	txMap_.erase(found);
}

void HTTPServer::removeExpiredTx() {
	const auto now = TxDeadlineClock::now_coarse();

	lock_guard lck(txMtx_);
	for (auto it = txMap_.begin(); it != txMap_.end();) {
		if (it->second.txDeadline <= now) {
			auto ctx = MakeSystemAuthContext();
			auto err = dbMgr_.OpenDatabase(it->second.dbName, ctx, false);
			if (err.ok()) {
				reindexer::Reindexer* db = nullptr;
				err = ctx.GetDB<AuthContext::CalledFrom::HTTPServer>(kRoleSystem, &db);
				if (db && err.ok()) {
					logger_.info("Rollback tx {} on idle deadline", it->first);
					err = db->RollBackTransaction(*it->second.tx);
				}
			}
			it = txMap_.erase(it);
		} else {
			++it;
		}
	}
}

int HTTPServer::getAuth(http::Context& ctx, AuthContext& auth, const std::string& dbName) const {
	const std::string_view authHeader = ctx.request->headers.Get("authorization");

	if (authHeader.length() < 6) {
		ctx.writer->SetHeader({"WWW-Authenticate"sv, R"(Basic realm="reindexer")"});
		std::ignore = ctx.String(http::StatusUnauthorized, "Forbidden"sv);
		return -1;
	}

	h_vector<char, 128> credVec(authHeader.length());
	char* credBuf = &credVec.front();
	Base64decode(credBuf, authHeader.data() + 6);
	char* password = strchr(credBuf, ':');
	if (password != nullptr) {
		*password++ = 0;
	}

	auth = AuthContext(credBuf, password ? password : "");
	const auto err = dbMgr_.Login(dbName, auth);
	if (!err.ok()) {
		ctx.writer->SetHeader({"WWW-Authenticate"sv, R"(Basic realm="reindexer")"});
		std::ignore = ctx.String(http::StatusUnauthorized, err.whatStr());
		return -1;
	}

	return 0;
}

int HTTPServer::CheckAuth(http::Context& ctx) {
	if (dbMgr_.IsNoSecurity()) {
		return 0;
	}

	AuthContext auth;
	if (auto res = getAuth(ctx, auth, ""); res != 0) {
		return res;
	}

	std::unique_ptr<HTTPClientData> clientData(new HTTPClientData);
	clientData->auth = auth;
	ctx.clientData = std::move(clientData);
	return 0;
}

int HTTPServer::BeginTx(http::Context& ctx) {
	const std::string nsName = urldecode2(ctx.request->urlParams[1]);
	if (nsName.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Namespace is not specified"));
	}

	std::string dbName;
	auto tx = getDB<kRoleDataWrite>(ctx, &dbName).NewTransaction(nsName);
	if (!tx.Status().ok()) {
		return status(ctx, http::HttpStatus(tx.Status()));
	}
	const auto txId = addTx(std::move(dbName), std::move(tx));

	WrSerializer ser(ctx.writer->GetChunk());

	switch (getDataFormat(ctx)) {
		case DataFormat::JSON: {
			JsonBuilder builder(ser);
			builder.Put(kTxId, txId);
			builder.End();
			break;
		}
		case DataFormat::MsgPack: {
			MsgPackBuilder builder(ser, ObjType::TypeObject, 1);
			builder.Put(kTxId, txId);
			builder.End();
			break;
		}
		case DataFormat::Protobuf: {
			ProtobufBuilder builder(&ser);
			builder.Put(kProtoBeginTxResultsFields.at(kTxId), txId);
			builder.End();
			break;
		}
		case DataFormat::CSVFile:
		default:
			throwUnsupportedOpFormat(ctx);
	}
	return statusOK(ctx, ser.DetachChunk());
}

int HTTPServer::CommitTx(http::Context& ctx) {
	const std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	std::string dbName;
	auto db = getDB<kRoleDataWrite>(ctx, &dbName);
	auto tx = getTx(dbName, txId);
	QueryResults qr;
	const auto err = db.CommitTransaction(*tx, qr);
	if (!err.ok()) {
		return status(ctx, http::HttpStatus(err));
	}
	removeTx(dbName, txId);
	return queryResults(ctx, qr, TxCommitOption(qr));
}

int HTTPServer::RollbackTx(http::Context& ctx) {
	const std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	std::string dbName;
	auto db = getDB<kRoleDataWrite>(ctx, &dbName);
	auto tx = getTx(dbName, txId);
	QueryResults qr;
	const auto err = db.RollBackTransaction(*tx);
	removeTx(dbName, txId);
	return status(ctx, http::HttpStatus(err));
}

int HTTPServer::PostItemsTx(http::Context& ctx) { return modifyItemsTx(ctx, ModeInsert); }

int HTTPServer::PutItemsTx(http::Context& ctx) { return modifyItemsTx(ctx, ModeUpdate); }

int HTTPServer::PatchItemsTx(http::Context& ctx) { return modifyItemsTx(ctx, ModeUpsert); }

int HTTPServer::DeleteItemsTx(http::Context& ctx) { return modifyItemsTx(ctx, ModeDelete); }

int HTTPServer::GetSQLQueryTx(http::Context& ctx) {
	std::string dbName;
	auto db = getDB<kRoleDataRead>(ctx, &dbName);
	const std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}
	reindexer::QueryResults res;
	const std::string sqlQuery = urldecode2(ctx.request->params.Get("q"sv));
	if (sqlQuery.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Missing `q` parameter"));
	}

	try {
		auto q = Query::FromSQL(sqlQuery);
		switch (q.type_) {
			case QueryDelete:
			case QueryUpdate:
				return modifyQueryTxImpl(ctx, dbName, txId, q);
			case QuerySelect:
			case QueryTruncate:
				return status(ctx, http::HttpStatus(http::StatusInternalServerError, "Transactions support update/delete queries only"));
		}
		return status(ctx, http::HttpStatus(Error(errLogic, "Unexpected query type: {}", int(q.type_))));
	} catch (const Error& e) {
		return status(ctx, http::HttpStatus(e));
	}
}

int HTTPServer::DeleteQueryTx(http::Context& ctx) {
	std::string dbName;
	auto db = getDB<kRoleDataWrite>(ctx, &dbName);
	const std::string dsl = ctx.body->Read();

	reindexer::Query q;
	try {
		q = Query::FromJSON(dsl);
	} catch (Error& err) {
		return status(ctx, http::HttpStatus(err));
	}
	reindexer::QueryResults res;
	const std::string txId = urldecode2(ctx.request->urlParams[1]);
	if (txId.empty()) {
		return status(ctx, http::HttpStatus(http::StatusBadRequest, "Tx ID is not specified"));
	}

	q.type_ = QueryDelete;
	return modifyQueryTxImpl(ctx, dbName, txId, q);
}

int HTTPServer::PostMemReset(http::Context& ctx) {
#if REINDEX_WITH_GPERFTOOLS
	if (alloc_ext::TCMallocIsAvailable()) {
		alloc_ext::instance()->ReleaseFreeMemory();
		return status(ctx);
	}
	return ctx.String(http::StatusForbidden,
					  "Reindexer was compiled with tcmalloc, but tcmalloc shared library is not linked. Try LD_PRELOAD to link it");
#else
	return ctx.String(http::StatusForbidden, "Reindexer was compiled without tcmalloc");
#endif
}

int HTTPServer::GetMemInfo(http::Context& ctx) {
#if REINDEX_WITH_GPERFTOOLS
	if (alloc_ext::TCMallocIsAvailable()) {
		const int bufSize = 32000;
		char buf[bufSize];
		alloc_ext::instance()->GetStats(buf, bufSize);
		return ctx.String(http::StatusOK, std::string_view(buf, strlen(buf)));
	}
	return ctx.String(http::StatusForbidden,
					  "Reindexer was compiled with tcmalloc, but tcmalloc shared library is not linked. Try LD_PRELOAD to link it");
#else
	return ctx.String(http::StatusForbidden, "Reindexer was compiled without tcmalloc");
#endif
}

void HTTPServer::Logger(http::Context& ctx) {
	HandlerStat statDiff = HandlerStat() - ctx.stat.allocStat;
	auto clientData = reinterpret_cast<HTTPClientData*>(ctx.clientData.get());
	if (serverConfig_.DebugAllocs) {
		logger_.info("{} - {} {} {} {} {} {}us | allocs: {}, allocated: {} byte(s)", ctx.request->clientAddr,
					 clientData ? clientData->auth.Login() : "", ctx.request->method, ctx.request->uri, ctx.writer->RespCode(),
					 ctx.writer->Written(), statDiff.GetTimeElapsed(), statDiff.GetAllocsCnt(), statDiff.GetAllocsBytes());
	} else {
		logger_.info("{} - {} {} {} {} {} {}us", ctx.request->clientAddr, clientData ? clientData->auth.Login() : "", ctx.request->method,
					 ctx.request->uri, ctx.writer->RespCode(), ctx.writer->Written(), statDiff.GetTimeElapsed());
	}
}

void HTTPServer::OnResponse(http::Context& ctx) {
	if (statsWatcher_) {
		static const std::string kUnknownDBName = "<unknown>";
		std::string dbName;
		const std::string* dbNamePtr = &kUnknownDBName;
		if (nullptr != ctx.request && !ctx.request->urlParams.empty() && 0 == ctx.request->path.find("/api/v1/db/"sv)) {
			dbName = urldecode2(ctx.request->urlParams[0]);
			dbNamePtr = &dbName;
		}
		statsWatcher_->OnInputTraffic(*dbNamePtr, statsSourceName(), std::string_view(), ctx.stat.sizeStat.reqSizeBytes);
		statsWatcher_->OnOutputTraffic(*dbNamePtr, statsSourceName(), std::string_view(), ctx.stat.sizeStat.respSizeBytes);
	}
}

int HTTPServer::GetRole(http::Context& ctx) {
	auto response = [&](UserRole role) {
		WrSerializer ser(ctx.writer->GetChunk());
		JsonBuilder builder(ser);
		builder.Put("user_role", UserRoleName(role));
		builder.End();
		return ctx.JSON(http::StatusOK, ser.DetachChunk());
	};

	if (dbMgr_.IsNoSecurity()) {
		return response(kRoleOwner);
	}

	AuthContext auth;
	auto res = getAuth(ctx, auth, "*");
	return res != 0 ? res : response(auth.UserRights());
}

bool HTTPServer::isParameterSetOn(std::string_view val) const noexcept {
	if (val.empty()) {
		return false;
	}
	if (iequals(val, "on") || iequals(val, "true") || iequals(val, "1")) {
		return true;
	}
	return false;
}

int HTTPServer::GetDefaultConfigs(http::Context& ctx) {
	std::string_view configType = ctx.request->params.Get("type"sv);
	WrSerializer ser(ctx.writer->GetChunk());
	reindexer::JsonBuilder builder(ser);

	if (Error ret = reindexer::GetDefaultConfigs(configType, builder); !ret.ok()) {
		return status(ctx, http::HttpStatus(ret));
	}
	return ctx.JSON(http::StatusOK, ser.DetachChunk());
}

HTTPServer::DataFormat HTTPServer::dataFormatFromStr(std::string_view str) {
	if (str.empty() || iequals(str, kJSONFmt)) {
		return DataFormat::JSON;
	}
	if (iequals(str, kMsgPackFmt)) {
		return DataFormat::MsgPack;
	}
	if (iequals(str, kProtobufFmt)) {
		return DataFormat::Protobuf;
	}
	if (iequals(str, kCSVFileFmt)) {
		return DataFormat::CSVFile;
	}
	throw Error(errParams, "Unexpected format names:'{}'", str);
}

HTTPServer::DataFormat HTTPServer::getDataFormat(const http::Context& ctx) {
	return dataFormatFromStr(ctx.request->params.Get("format"sv));
}

void HTTPServer::throwUnsupportedOpFormat(const http::Context& ctx) {
	throw Error(errParams, "Unsupported format '{}' for '{}'", ctx.request->params.Get("format"sv), ctx.request->path);
}

}  // namespace reindexer_server
