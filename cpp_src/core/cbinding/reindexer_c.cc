#include "reindexer_c.h"

#include <string.h>

#include "cgocancelcontextpool.h"
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsonbuilder.h"
#include "debug/crashqueryreporter.h"
#include "estl/gift_str.h"
#include "estl/syncpool.h"
#include "events/subscriber_config.h"
#include "reindexer_version.h"
#include "reindexer_wrapper.h"
#include "resultserializer.h"
#include "tools/semversion.h"

using namespace reindexer;
constexpr int kQueryResultsPoolSize = 1024;
constexpr int kMaxConcurrentQueries = 65534;
constexpr size_t kCtxArrSize = 1024;
constexpr size_t kWarnLargeResultsLimit = 0x40000000;
constexpr size_t kMaxPooledResultsCap = 0x10000;

static const Error err_not_init(errNotValid, "Reindexer db has not initialized");
static const Error err_too_many_queries(errLogic, "Too many parallel queries");
static const Error err_unexpected_exception{errAssert, "Unexpected exception in Reindexer C-binding"};

// Using (void)1 here to force ';' usage after the macro
#define CATCH_AND_RETURN_C                        \
	catch (std::exception & err) {                \
		return error2c(err);                      \
	}                                             \
	catch (...) {                                 \
		return error2c(err_unexpected_exception); \
	}                                             \
	(void)1

// Using (void)1 here to force ';' usage after the macro
#define CATCH_AND_RETURN_OUT_C(out)                  \
	catch (std::exception & err) {                   \
		return ret2c(err, out);                      \
	}                                                \
	catch (...) {                                    \
		return ret2c(err_unexpected_exception, out); \
	}                                                \
	(void)1

static std::atomic<BindingCapabilities> bindingCaps;
static_assert(std::atomic<BindingCapabilities>::is_always_lock_free, "Expected lockfree structure here");

static reindexer_error error2c(const Error& err_) noexcept {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.whatStr().length() ? strdup(err_.what()) : nullptr;
	return err;
}

static reindexer_ret ret2c(const Error& err_, const reindexer_resbuffer& out) noexcept {
	reindexer_ret ret;
	ret.err_code = err_.code();
	if (ret.err_code) {
		ret.out.results_ptr = 0;
		ret.out.data = uintptr_t(err_.whatStr().length() ? strdup(err_.what()) : nullptr);
	} else {
		ret.out = out;
	}
	return ret;
}

static reindexer_array_ret arr_ret2c(const Error& err_, reindexer_buffer* out, uint32_t out_size) noexcept {
	reindexer_array_ret ret;
	ret.err_code = err_.code();
	if (ret.err_code) {
		ret.out_buffers = 0;
		ret.out_size = 0;
		ret.data = uintptr_t(err_.whatStr().length() ? strdup(err_.what()) : nullptr);
	} else {
		ret.out_buffers = out;
		ret.out_size = out_size;
		assertrx_dbg(ret.out_buffers);
	}
	return ret;
}

static uint32_t span2arr(std::span<chunk> d, reindexer_buffer* out, uint32_t out_size) noexcept {
	const auto sz = std::min(d.size(), size_t(out_size));
	for (uint32_t i = 0; i < sz; ++i) {
		out[i].data = d[i].data();
		out[i].len = d[i].len();
	}
	return sz;
}

static std::string str2c(reindexer_string gs) { return std::string(reinterpret_cast<const char*>(gs.p), gs.n); }
static std::string_view str2cv(reindexer_string gs) noexcept { return std::string_view(reinterpret_cast<const char*>(gs.p), gs.n); }

struct [[nodiscard]] QueryResultsWrapper : QueryResults {
	WrResultSerializer ser;
	QueryResults::ProxiedRefsStorage proxiedRefsStorage;
};
struct [[nodiscard]] TransactionWrapper {
	TransactionWrapper(Transaction&& tr) : tr_(std::move(tr)) {}
	WrResultSerializer ser_;
	Transaction tr_;
};

static std::atomic<int> serializedResultsCount{0};
static sync_pool<QueryResultsWrapper, kQueryResultsPoolSize, kMaxConcurrentQueries> res_pool;
static CGOCtxPool ctx_pool(kCtxArrSize);

struct [[nodiscard]] put_results_to_pool {
	void operator()(QueryResultsWrapper* res) const {
		std::unique_ptr<QueryResultsWrapper> results{res};
		results->Clear();
		results->proxiedRefsStorage = std::vector<QueryResults::ItemRefCache>();
		if (results->ser.Cap() > kMaxPooledResultsCap) {
			results->ser = WrResultSerializer();
		} else {
			results->ser.Reset();
		}
		res_pool.put(std::move(results));
	}
};

struct [[nodiscard]] query_results_ptr : public std::unique_ptr<QueryResultsWrapper, put_results_to_pool> {
	query_results_ptr() noexcept = default;
	query_results_ptr(std::unique_ptr<QueryResultsWrapper>&& ptr) noexcept
		: std::unique_ptr<QueryResultsWrapper, put_results_to_pool>{ptr.release()} {}
	operator std::unique_ptr<QueryResultsWrapper>() && noexcept { return std::unique_ptr<QueryResultsWrapper>{release()}; }
};

static query_results_ptr new_results(bool as_json) {
	auto res = res_pool.get(serializedResultsCount.load(std::memory_order_relaxed));
	if (res) {
		res->SetFlags(as_json ? kResultsJson : (kResultsCJson | kResultsWithItemID | kResultsWithPayloadTypes));
	}
	return query_results_ptr(std::move(res));
}

static void results2c(std::unique_ptr<QueryResultsWrapper> result, struct reindexer_resbuffer* out, int as_json = 0,
					  int32_t* pt_versions = nullptr, int pt_versions_count = 0) {
	int flags = 0;
	if (as_json) {
		flags = kResultsJson;
	} else {
		flags = pt_versions ? (kResultsCJson | kResultsWithItemID | kResultsWithPayloadTypes) : (kResultsCJson | kResultsWithItemID);
	}
	const bool rawResProxying =
		result->IsRawProxiedBufferAvailable(flags) && WrResultSerializer::IsRawResultsSupported(bindingCaps.load(), *result);
	std::string_view rawBufOut;
	if (rawResProxying) {
		result->ser.SetOpts({.flags = flags,
							 .ptVersions = std::span<int32_t>(pt_versions, pt_versions_count),
							 .fetchOffset = 0,
							 .fetchLimit = INT_MAX,
							 .withAggregations = true});
		std::ignore = result->ser.PutResultsRaw(*result, &rawBufOut);
		out->len = rawBufOut.size() ? rawBufOut.size() : result->ser.Len();
		out->data = rawBufOut.size() ? uintptr_t(rawBufOut.data()) : uintptr_t(result->ser.Buf());
	} else {
		flags = as_json ? kResultsJson : (kResultsPtrs | kResultsWithItemID);
		if (pt_versions && as_json == 0) {
			flags |= kResultsWithPayloadTypes;
		}
		result->ser.SetOpts({.flags = flags,
							 .ptVersions = std::span<int32_t>(pt_versions, pt_versions_count),
							 .fetchOffset = 0,
							 .fetchLimit = INT_MAX,
							 .withAggregations = true});
		std::ignore = result->ser.PutResults(*result, bindingCaps.load(std::memory_order_relaxed), &result->proxiedRefsStorage);
		out->len = result->ser.Len();
		out->data = uintptr_t(result->ser.Buf());
	}

	out->results_ptr = uintptr_t(result.release());
	if (const auto count{serializedResultsCount.fetch_add(1, std::memory_order_relaxed)}; count > kMaxConcurrentQueries) {
		logFmt(LogWarning, "Too many serialized results: count={}, alloced={}", count, res_pool.Alloced());
	}
}

uintptr_t init_reindexer() {
	reindexer_init_locale();
	static std::atomic<int64_t> dbsCounter = {0};
	auto db = new ReindexerWrapper(std::move(ReindexerConfig().WithDBName(fmt::format("builtin_db_{}", dbsCounter++))));
	return reinterpret_cast<uintptr_t>(db);
}

uintptr_t init_reindexer_with_config(reindexer_config config) {
	reindexer_init_locale();
	auto db = new ReindexerWrapper(std::move(ReindexerConfig()
												 .WithAllocatorCacheLimits(config.allocator_cache_limit, config.allocator_max_cache_part)
												 .WithDBName(str2c(config.sub_db_name))
												 .WithUpdatesSize(config.max_updates_size)));
	return reinterpret_cast<uintptr_t>(db);
}

void destroy_reindexer(uintptr_t rx) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	delete db;
}

reindexer_error reindexer_ping(uintptr_t rx) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	return error2c(db ? Error() : err_not_init);
}

static void proccess_packed_item(Item& item, int /*mode*/, int state_token, reindexer_buffer data, int format, Error& err) noexcept {
	if (item.Status().ok()) {
		switch (format) {
			case FormatJson:
				err = item.FromJSON(std::string_view(reinterpret_cast<const char*>(data.data), data.len), 0,
									false);	 // TODO: for mode == ModeDelete deserialize PK and sharding key only
				break;
			case FormatCJson:
				if (item.GetStateToken() != state_token) {
					err = Error(errStateInvalidated, "stateToken mismatch: {:#08x}, need {:#08x}. Can't process item", state_token,
								item.GetStateToken());
				} else {
					err = item.FromCJSON(std::string_view(reinterpret_cast<const char*>(data.data), data.len),
										 false);  // TODO: for mode == ModeDelete deserialize PK and sharding key only
				}
				break;
			default:
				err = Error(errNotValid, "Invalid source item format {}", format);
		}
	} else {
		err = item.Status();
	}
}

reindexer_error reindexer_modify_item_packed_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer args, reindexer_buffer data) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	auto trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(Error());
	}
	Error err = err_not_init;

	try {
		Serializer ser(args.data, args.len);
		int format = ser.GetVarUInt();
		int mode = ser.GetVarUInt();
		int state_token = ser.GetVarUInt();
		auto item = trw->tr_.NewItem();
		proccess_packed_item(item, mode, state_token, data, format, err);
		if (err.code() == errTagsMissmatch) {
			item = db->rx.NewItem(trw->tr_.GetNsName());
			err = item.Status();
			if (err.ok()) {
				proccess_packed_item(item, mode, state_token, data, format, err);
			}
		}
		if (err.ok()) {
			unsigned preceptsCount = ser.GetVarUInt();
			std::vector<std::string> precepts;
			precepts.reserve(preceptsCount);
			while (preceptsCount--) {
				precepts.emplace_back(ser.GetVString());
			}
			item.SetPrecepts(std::move(precepts));
			err = trw->tr_.Modify(std::move(item), ItemModifyMode(mode));
		}
	}
	CATCH_AND_RETURN_C;

	return error2c(err);
}

reindexer_ret reindexer_modify_item_packed(uintptr_t rx, reindexer_buffer args, reindexer_buffer data, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};
	try {
		Serializer ser(args.data, args.len);
		std::string_view ns = ser.GetVString();
		int format = ser.GetVarUInt();
		int mode = ser.GetVarUInt();
		int state_token = ser.GetVarUInt();

		Error err = err_not_init;
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

			Item item = rdxKeeper.db().NewItem(ns);

			proccess_packed_item(item, mode, state_token, data, format, err);

			query_results_ptr res;
			if (err.ok()) {
				unsigned preceptsCount = ser.GetVarUInt();
				const bool needSaveItemValueInQR = preceptsCount;
				std::vector<std::string> precepts;
				precepts.reserve(preceptsCount);
				while (preceptsCount--) {
					precepts.emplace_back(ser.GetVString());
				}
				item.SetPrecepts(std::move(precepts));

				res = new_results(false);
				if (!res) {
					return ret2c(err_too_many_queries, out);
				}
				if (needSaveItemValueInQR) {
					switch (mode) {
						case ModeUpsert:
							err = rdxKeeper.db().Upsert(ns, item, *res);
							break;
						case ModeInsert:
							err = rdxKeeper.db().Insert(ns, item, *res);
							break;
						case ModeUpdate:
							err = rdxKeeper.db().Update(ns, item, *res);
							break;
						case ModeDelete:
							err = rdxKeeper.db().Delete(ns, item, *res);
							break;
						default:
							err = Error(errParams, "Unexpected ItemModifyMode: {}", mode);
							break;
					}
				} else {
					switch (mode) {
						case ModeUpsert:
							err = rdxKeeper.db().Upsert(ns, item);
							break;
						case ModeInsert:
							err = rdxKeeper.db().Insert(ns, item);
							break;
						case ModeUpdate:
							err = rdxKeeper.db().Update(ns, item);
							break;
						case ModeDelete:
							err = rdxKeeper.db().Delete(ns, item);
							break;
						default:
							err = Error(errParams, "Unexpected ItemModifyMode: {}", mode);
							break;
					}
					if (err.ok()) {
						LocalQueryResults lqr;
						lqr.AddItemNoHold(item, lsn_t());
						res->AddQr(std::move(lqr));
					}
				}
			}

			if (err.ok()) {
				int32_t ptVers = -1;
				bool tmUpdated = item.IsTagsUpdated();
				results2c(std::move(res), &out, 0, tmUpdated ? &ptVers : nullptr, tmUpdated ? 1 : 0);
			}
		}
		return ret2c(err, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_tx_ret reindexer_start_transaction(uintptr_t rx, reindexer_string nsName) {
	reindexer_tx_ret ret{0, {nullptr, 0}};
	try {
		auto db = reinterpret_cast<ReindexerWrapper*>(rx);
		if (!db) {
			ret.err = error2c(err_not_init);
			return ret;
		}
		Transaction tr = db->rx.NewTransaction(str2cv(nsName));
		if (tr.Status().ok()) {
			auto trw = new TransactionWrapper(std::move(tr));
			ret.tx_id = reinterpret_cast<uintptr_t>(trw);
		} else {
			ret.err = error2c(tr.Status());
		}
	} catch (std::exception& e) {
		ret.err = error2c(e);
	} catch (...) {
		ret.err = error2c(err_unexpected_exception);
	}

	return ret;
}

reindexer_error reindexer_rollback_transaction(uintptr_t rx, uintptr_t tr) {
	Error err;
	try {
		auto db = reinterpret_cast<ReindexerWrapper*>(rx);
		if (!db) {
			return error2c(err_not_init);
		}
		auto trw = std::unique_ptr<TransactionWrapper>(reinterpret_cast<TransactionWrapper*>(tr));
		if (!trw) {
			return error2c(Error());
		}
		err = db->rx.RollBackTransaction(trw->tr_);
	}
	CATCH_AND_RETURN_C;
	return error2c(err);
}

reindexer_ret reindexer_commit_transaction(uintptr_t rx, uintptr_t tr, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};
	try {
		if (!rx) {
			return ret2c(err_not_init, out);
		}
		std::unique_ptr<TransactionWrapper> trw(reinterpret_cast<TransactionWrapper*>(tr));
		if (!trw) {
			return ret2c(errOK, out);
		}

		auto res(new_results(false));
		if (!res) {
			return ret2c(err_too_many_queries, out);
		}

		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

		auto err = rdxKeeper.db().CommitTransaction(trw->tr_, *res);

		if (err.ok()) {
			int32_t ptVers = -1;
			results2c(std::move(res), &out, 0, trw->tr_.IsTagsUpdated() ? &ptVers : nullptr, trw->tr_.IsTagsUpdated() ? 1 : 0);
		}

		return ret2c(err, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_error reindexer_open_namespace(uintptr_t rx, reindexer_string nsName, StorageOpts opts, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().OpenNamespace(str2cv(nsName), opts);
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_drop_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().DropNamespace(str2cv(nsName));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_truncate_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().TruncateNamespace(str2cv(nsName));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_rename_namespace(uintptr_t rx, reindexer_string srcNsName, reindexer_string dstNsName,
										   reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().RenameNamespace(str2cv(srcNsName), str2c(dstNsName));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_close_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().CloseNamespace(str2cv(nsName));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_add_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			std::string json(str2cv(indexDefJson));
			const auto indexDef = IndexDef::FromJSON(giftStr(json));
			if (!indexDef) {
				return error2c(indexDef.error());
			}

			res = rdxKeeper.db().AddIndex(str2cv(nsName), *indexDef);
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_update_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			std::string json(str2cv(indexDefJson));

			const auto indexDef = IndexDef::FromJSON(giftStr(json));
			if (!indexDef) {
				return error2c(indexDef.error());
			}

			res = rdxKeeper.db().UpdateIndex(str2cv(nsName), *indexDef);
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_error reindexer_drop_index(uintptr_t rx, reindexer_string nsName, reindexer_string index, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().DropIndex(str2cv(nsName), IndexDef(str2c(index)));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

// Special function fot tsan's false positive suppression
static std::string copy_ns_schema(reindexer_string gs) { return std::string(reinterpret_cast<const char*>(gs.p), gs.n); }

reindexer_error reindexer_set_schema(uintptr_t rx, reindexer_string nsName, reindexer_string schemaJson, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().SetSchema(str2cv(nsName), copy_ns_schema(schemaJson));
	}
	return error2c(res);
}

reindexer_error reindexer_connect(uintptr_t rx, reindexer_string dsn, ConnectOpts opts, reindexer_string client_vers,
								  BindingCapabilities caps) {
	try {
		SemVersion cliVersion(str2cv(client_vers));
		if (opts.options & kConnectOptWarnVersion) {
			SemVersion libVersion(REINDEX_VERSION);
			if (cliVersion != libVersion) {
				std::cerr << "Warning: Used Reindexer client version: " << str2cv(client_vers)
						  << " with library version: " << REINDEX_VERSION
						  << ". It is strongly recommended to sync client & library versions" << std::endl;
			}
		}

		auto db = reinterpret_cast<ReindexerWrapper*>(rx);
		if (!db) {
			return error2c(err_not_init);
		}
		Error err = db->rx.Connect(str2c(dsn), opts);
		if (err.ok() && db->rx.NeedTraceActivity()) {
			db->rx.SetActivityTracer("builtin", "");
		}
		bindingCaps.store(caps, std::memory_order_relaxed);
		return error2c(err);
	}
	CATCH_AND_RETURN_C;
}

reindexer_ret reindexer_select(uintptr_t rx, reindexer_string query, int as_json, int32_t* pt_versions, int pt_versions_count,
							   reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};
	try {
		Error err = err_not_init;
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			auto result{new_results(as_json)};
			if (!result) {
				return ret2c(err_too_many_queries, out);
			}

			auto querySV = str2cv(query);
			ActiveQueryScope scope(querySV);
			err = rdxKeeper.db().ExecSQL(querySV, *result);
			if (err.ok()) {
				const auto count = result->Count(), len = result->ser.Len(), cap = result->ser.Cap();
				results2c(std::move(result), &out, as_json, pt_versions, pt_versions_count);
				if (cap >= kWarnLargeResultsLimit) {
					logFmt(LogWarning, "Query too large results: count={}, size={}, cap={}, q={}", count, len, cap, str2cv(query));
				}
			}
		}
		return ret2c(err, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_ret reindexer_select_query(uintptr_t rx, struct reindexer_buffer in, int as_json, int32_t* pt_versions, int pt_versions_count,
									 reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};
	try {
		Error err = err_not_init;
		if (rx) {
			err = Error(errOK);
			Serializer ser(in.data, in.len);
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

			Query q = Query::Deserialize(ser);
			while (!ser.Eof()) {
				const auto joinType = JoinType(ser.GetVarUInt());
				JoinedQuery q1{joinType, Query::Deserialize(ser)};
				if (q1.joinType == JoinType::Merge) {
					q.Merge(std::move(q1));
				} else {
					q.AddJoinQuery(std::move(q1));
				}
			}

			auto result{new_results(as_json)};
			if (!result) {
				return ret2c(err_too_many_queries, out);
			}

			ActiveQueryScope scope(q, QuerySelect);
			err = rdxKeeper.db().Select(q, *result);
			if (q.GetDebugLevel() >= LogError && err.code() != errOK) {
				logFmt(LogError, "Query error {}", err.what());
			}
			if (err.ok()) {
				results2c(std::move(result), &out, as_json, pt_versions, pt_versions_count);
			} else {
				if (result->ser.Cap() >= kWarnLargeResultsLimit) {
					logFmt(LogWarning, "Query too large results: count={} size={},cap={}, q={}", result->Count(), result->ser.Len(),
						   result->ser.Cap(), q.GetSQL());
				}
			}
		}
		return ret2c(err, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_ret reindexer_delete_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	try {
		Error res = err_not_init;
		if (rx) {
			res = Error(errOK);
			Serializer ser(in.data, in.len);
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

			Query q = Query::Deserialize(ser);
			q.type_ = QueryDelete;

			auto result{new_results(false)};
			if (!result) {
				return ret2c(err_too_many_queries, out);
			}

			ActiveQueryScope scope(q, QueryDelete);
			res = rdxKeeper.db().Delete(q, *result);
			if (q.GetDebugLevel() >= LogError && res.code() != errOK) {
				logFmt(LogError, "Query error {}", res.what());
			}
			if (res.ok()) {
				results2c(std::move(result), &out);
			}
		}
		return ret2c(res, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_ret reindexer_update_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	try {
		Error res = err_not_init;
		if (rx) {
			res = Error(errOK);
			Serializer ser(in.data, in.len);
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

			Query q = Query::Deserialize(ser);
			q.type_ = QueryUpdate;
			auto result{new_results(false)};
			if (!result) {
				return ret2c(err_too_many_queries, out);
			}

			ActiveQueryScope scope(q, QueryUpdate);
			res = rdxKeeper.db().Update(q, *result);
			if (q.GetDebugLevel() >= LogError && res.code() != errOK) {
				logFmt(LogError, "Query error {}", res.what());
			}
			if (res.ok()) {
				int32_t ptVers = -1;
				results2c(std::move(result), &out, 0, &ptVers, 1);
			}
		}
		return ret2c(res, out);
	}
	CATCH_AND_RETURN_OUT_C(out);
}

reindexer_error reindexer_delete_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	auto trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(errOK);
	}
	Serializer ser(in.data, in.len);
	try {
		Query q = Query::Deserialize(ser);
		q.type_ = QueryDelete;

		Error err = trw->tr_.Modify(std::move(q));
		return error2c(err);
	}
	CATCH_AND_RETURN_C;
}

reindexer_error reindexer_update_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	auto trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(errOK);
	}
	Serializer ser(in.data, in.len);
	try {
		Query q = Query::Deserialize(ser);
		q.type_ = QueryUpdate;

		Error err = trw->tr_.Modify(std::move(q));
		return error2c(err);
	}
	CATCH_AND_RETURN_C;
}

// This method is required for builtin modes of java-connector
reindexer_buffer reindexer_cptr2cjson(uintptr_t results_ptr, uintptr_t cptr, int ns_id) {
	QueryResults* qr = reinterpret_cast<QueryResults*>(results_ptr);
	cptr -= sizeof(PayloadValue::dataHeader);

	PayloadValue* pv = reinterpret_cast<PayloadValue*>(&cptr);
	const auto tagsMatcher = qr->GetTagsMatcher(ns_id);
	const auto payloadType = qr->GetPayloadType(ns_id);

	WrSerializer ser;
	ConstPayload pl(payloadType, *pv);
	CJsonBuilder builder(ser, ObjType::TypePlain);
	CJsonEncoder cjsonEncoder(&tagsMatcher, &qr->GetFieldsFilter(ns_id));

	cjsonEncoder.Encode(pl, builder);
	const int n = ser.Len();
	uint8_t* p = ser.DetachBuf().release();
	return reindexer_buffer{p, n};
}

void reindexer_free_cjson(reindexer_buffer b) { delete[] b.data; }

reindexer_ret reindexer_enum_meta(uintptr_t rx, reindexer_string ns, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			auto results{new_results(false)};
			if (!results) {
				return ret2c(err_too_many_queries, out);
			}

			std::vector<std::string> keys;
			res = rdxKeeper.db().EnumMeta(str2c(ns), keys);

			auto& ser = results->ser;
			ser.PutVarUint(keys.size());
			for (const auto& key : keys) {
				ser.PutVString(key);
			}

			out.len = ser.Len();
			out.data = uintptr_t(ser.Buf());
			out.results_ptr = uintptr_t(results.release());
			if (const auto count{serializedResultsCount.fetch_add(1, std::memory_order_relaxed)}; count > kMaxConcurrentQueries) {
				logFmt(LogWarning, "Too many serialized results: count={}, alloced={}", count, res_pool.Alloced());
			}
		}
	}
	CATCH_AND_RETURN_OUT_C(out);
	return ret2c(res, out);
}

reindexer_error reindexer_put_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_string data,
								   reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().PutMeta(str2c(ns), str2c(key), str2c(data));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

reindexer_ret reindexer_get_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			auto results{new_results(false)};
			if (!results) {
				return ret2c(err_too_many_queries, out);
			}

			std::string data;
			res = rdxKeeper.db().GetMeta(str2c(ns), str2c(key), data);
			results->ser.Write(data);
			out.len = results->ser.Len();
			out.data = uintptr_t(results->ser.Buf());
			out.results_ptr = uintptr_t(results.release());
			if (const auto count{serializedResultsCount.fetch_add(1, std::memory_order_relaxed)}; count > kMaxConcurrentQueries) {
				logFmt(LogWarning, "Too many serialized results: count={}, alloced={}", count, res_pool.Alloced());
			}
		}
	}
	CATCH_AND_RETURN_OUT_C(out);
	return ret2c(res, out);
}

reindexer_error reindexer_delete_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	try {
		if (rx) {
			CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
			res = rdxKeeper.db().DeleteMeta(str2c(ns), str2c(key));
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(res);
}

void reindexer_enable_logger(void (*logWriter)(int, char*)) {
	try {
		logInstallWriter(logWriter, LoggerPolicy::WithLocks, int(LogTrace));
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unable to enable logger in Reindexer C-binding: %s", e.what());
	} catch (...) {
		fprintf(stderr, "reindexer error: unable to enable logger in Reindexer C-binding: <no description>");
	}
}

void reindexer_disable_logger() {
	try {
		logInstallWriter(nullptr, LoggerPolicy::WithLocks, int(LogNone));
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unable to disable logger in Reindexer C-binding: %s", e.what());
	} catch (...) {
		fprintf(stderr, "reindexer error: unable to disable logger in Reindexer C-binding: <no description>");
	}
}

reindexer_error reindexer_free_buffer(reindexer_resbuffer in) {
	try {
		constexpr static put_results_to_pool putResultsToPool;
		putResultsToPool(reinterpret_cast<QueryResultsWrapper*>(in.results_ptr));
		if (const auto count{serializedResultsCount.fetch_sub(1, std::memory_order_relaxed)}; count < 1) {
			logFmt(LogWarning, "Too many deserialized results: count={}, alloced={}", count, res_pool.Alloced());
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(Error());
}

reindexer_error reindexer_free_buffers(reindexer_resbuffer* in, int count) {
	try {
		for (int i = 0; i < count; i++) {  // NOLINT(*.Malloc) Memory will be deallocated by Go
			reindexer_free_buffer(in[i]);
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(Error());
}

reindexer_error reindexer_cancel_context(reindexer_ctx_info ctx_info, ctx_cancel_type how) {
	auto howCPP = CancelType::None;
	switch (how) {
		case cancel_expilicitly:
			howCPP = CancelType::Explicit;
			break;
		case cancel_on_timeout:
			howCPP = CancelType::Timeout;
			break;
		default:
			assertrx(false);
	}
	if (ctx_pool.cancelContext(ctx_info, howCPP)) {
		return error2c(Error());
	}
	return error2c(Error(errParams, "Unable to cancle context"));
}

void reindexer_init_locale() {
	static std::once_flag flag;
	std::call_once(flag, [] {
		setvbuf(stdout, nullptr, _IONBF, 0);
		setvbuf(stderr, nullptr, _IONBF, 0);
		setlocale(LC_CTYPE, "");
		setlocale(LC_NUMERIC, "C");
	});
}

reindexer_error reindexer_subscribe(uintptr_t rx, reindexer_string optsJSON) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	Error err = err_not_init;
	try {
		if (db) {
			EventSubscriberConfig cfg;
			std::string json(str2cv(optsJSON));
			err = cfg.FromJSON(giftStr(json));
			if (err.ok()) {
				err = db->rx.SubscribeUpdates(db->builtinUpdatesObs, std::move(cfg));
			}
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(err);
}

reindexer_error reindexer_unsubscribe(uintptr_t rx) {
	auto db = reinterpret_cast<ReindexerWrapper*>(rx);
	Error err = err_not_init;
	try {
		if (db) {
			err = db->rx.UnsubscribeUpdates(db->builtinUpdatesObs);
		}
	}
	CATCH_AND_RETURN_C;
	return error2c(err);
}

reindexer_array_ret reindexer_read_events(uintptr_t rx, reindexer_buffer* out_buffers, uint32_t buffers_count) {
	Error err = err_not_init;
	reindexer_buffer* res = nullptr;
	uint32_t resCount = 0;
	if (out_buffers == nullptr) {
		return arr_ret2c(Error(errParams, "reindexer_await_events: out_buffers can not be null"), res, resCount);
	}
	if (buffers_count == 0) {
		return arr_ret2c(Error(errParams, "reindexer_await_events: buffers_count can not be 0"), res, resCount);
	}
	if (auto db = reinterpret_cast<ReindexerWrapper*>(rx); db) {
		auto updates = db->builtinUpdatesObs.TryReadUpdates();
		resCount = span2arr(updates, out_buffers, buffers_count);
		res = out_buffers;
		err = Error();
		// FIXME: Debug this logic in case of error
	}
	return arr_ret2c(err, res, resCount);
}

reindexer_error reindexer_erase_events(uintptr_t rx, uint32_t events_count) {
	Error err = err_not_init;
	if (auto db = reinterpret_cast<ReindexerWrapper*>(rx); db) {
		if (events_count) {
			db->builtinUpdatesObs.EraseUpdates(events_count);
		}
		err = Error();
	}
	return error2c(err);
}

const char* reindexer_version() {
	const char* version_str = REINDEX_VERSION;
	return version_str;
}
