#include "reindexer_c.h"

#include <stdlib.h>
#include <string.h>
#include <locale>
#include <mutex>

#include "cgocancelcontextpool.h"
#include "core/selectfunc/selectfuncparser.h"
#include "core/transactionimpl.h"
#include "debug/allocdebug.h"
#include "estl/syncpool.h"
#include "resultserializer.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace reindexer;
using std::move;
const int kQueryResultsPoolSize = 1024;
const int kMaxConcurentQueries = 65534;
const size_t kCtxArrSize = 1024;

static Error err_not_init(-1, "Reindexer db has not initialized");
static Error err_too_many_queries(errLogic, "Too many paralell queries");

static reindexer_error error2c(const Error& err_) {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	return err;
}

static reindexer_ret ret2c(const Error& err_, const reindexer_resbuffer& out) {
	reindexer_ret ret;
	ret.err_code = err_.code();
	if (ret.err_code) {
		ret.out.results_ptr = 0;
		ret.out.data = uintptr_t(err_.what().length() ? strdup(err_.what().c_str()) : nullptr);
	} else {
		ret.out = out;
	}
	return ret;
}

static string str2c(reindexer_string gs) { return string(reinterpret_cast<const char*>(gs.p), gs.n); }
static string_view str2cv(reindexer_string gs) { return string_view(reinterpret_cast<const char*>(gs.p), gs.n); }

struct QueryResultsWrapper : QueryResults {
	WrResultSerializer ser;
};
struct TransactionWrapper {
	TransactionWrapper(Transaction&& tr) : tr_(std::move(tr)) {}
	WrResultSerializer ser_;
	Transaction tr_;
};

static sync_pool<QueryResultsWrapper, kQueryResultsPoolSize, kMaxConcurentQueries> res_pool;
static CGOCtxPool ctx_pool(kCtxArrSize);

static void put_results_to_pool(QueryResultsWrapper* res) {
	res->Clear();
	res->ser.Reset();
	res_pool.put(res);
}

static QueryResultsWrapper* new_results() { return res_pool.get(); }

static void results2c(QueryResultsWrapper* result, struct reindexer_resbuffer* out, int as_json = 0, int32_t* pt_versions = nullptr,
					  int pt_versions_count = 0) {
	int flags = as_json ? kResultsJson : (kResultsPtrs | kResultsWithItemID);

	flags |= (pt_versions && as_json == 0) ? kResultsWithPayloadTypes : 0;

	result->ser.SetOpts({flags, span<int32_t>(pt_versions, pt_versions_count), 0, INT_MAX});

	result->ser.PutResults(result);

	out->len = result->ser.Len();
	out->data = uintptr_t(result->ser.Buf());
	out->results_ptr = uintptr_t(result);
}

uintptr_t init_reindexer() {
	Reindexer* db = new Reindexer();
	setvbuf(stdout, 0, _IONBF, 0);
	setvbuf(stderr, 0, _IONBF, 0);
	setlocale(LC_CTYPE, "");
	setlocale(LC_NUMERIC, "C");
	return reinterpret_cast<uintptr_t>(db);
}

void destroy_reindexer(uintptr_t rx) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	delete db;
	db = nullptr;
}

reindexer_error reindexer_ping(uintptr_t rx) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	return error2c(db ? Error(errOK) : err_not_init);
}

static void procces_packed_item(Item& item, int mode, int state_token, reindexer_buffer data, const vector<string>& precepts, int format,
								Error& err) {
	if (item.Status().ok()) {
		switch (format) {
			case FormatJson:
				err = item.FromJSON(string_view(reinterpret_cast<const char*>(data.data), data.len), 0, mode == ModeDelete);
				break;
			case FormatCJson:
				if (item.GetStateToken() != state_token) {
					err = Error(errStateInvalidated, "stateToken mismatch:  %08X, need %08X. Can't process item", state_token,
								item.GetStateToken());
				} else {
					err = item.FromCJSON(string_view(reinterpret_cast<const char*>(data.data), data.len), mode == ModeDelete);
				}
				break;
			default:
				err = Error(-1, "Invalid source item format %d", format);
		}
		if (err.ok()) {
			item.SetPrecepts(precepts);
		}
	} else {
		err = item.Status();
	}
}

reindexer_error reindexer_modify_item_packed_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer args, reindexer_buffer data) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	TransactionWrapper* trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(errOK);
	}

	Serializer ser(args.data, args.len);
	int format = ser.GetVarUint();
	int mode = ser.GetVarUint();
	int state_token = ser.GetVarUint();
	unsigned preceptsCount = ser.GetVarUint();
	vector<string> precepts;
	while (preceptsCount--) {
		precepts.push_back(string(ser.GetVString()));
	}
	Error err = err_not_init;
	auto item = trw->tr_.NewItem();
	procces_packed_item(item, mode, state_token, data, precepts, format, err);
	if (err.code() == errTagsMissmatch) {
		item = db->NewItem(trw->tr_.GetName());
		err = item.Status();
		if (err.ok()) {
			procces_packed_item(item, mode, state_token, data, precepts, format, err);
		}
	}
	if (err.ok()) {
		trw->tr_.Modify(std::move(item), ItemModifyMode(mode));
	}

	return error2c(err);
}

reindexer_ret reindexer_modify_item_packed(uintptr_t rx, reindexer_buffer args, reindexer_buffer data, reindexer_ctx_info ctx_info) {
	Serializer ser(args.data, args.len);
	string_view ns = ser.GetVString();
	int format = ser.GetVarUint();
	int mode = ser.GetVarUint();
	int state_token = ser.GetVarUint();
	unsigned preceptsCount = ser.GetVarUint();
	vector<string> precepts;
	while (preceptsCount--) {
		precepts.push_back(string(ser.GetVString()));
	}

	reindexer_resbuffer out = {0, 0, 0};
	Error err = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

		Item item = rdxKeeper.db().NewItem(ns);

		procces_packed_item(item, mode, state_token, data, precepts, format, err);

		if (err.ok()) {
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
			}
		}

		if (err.ok()) {
			QueryResultsWrapper* res = new_results();
			if (!res) {
				return ret2c(err_too_many_queries, out);
			}
			res->AddItem(item, !precepts.empty());
			int32_t ptVers = -1;
			bool tmUpdated = item.IsTagsUpdated();
			results2c(res, &out, 0, tmUpdated ? &ptVers : nullptr, tmUpdated ? 1 : 0);
		}
	}

	return ret2c(err, out);
}

reindexer_tx_ret reindexer_start_transaction(uintptr_t rx, reindexer_string nsName) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	reindexer_tx_ret ret{0, {nullptr, 0}};
	if (!db) {
		ret.err = error2c(err_not_init);
		return ret;
	}
	Transaction tr = db->NewTransaction(str2cv(nsName));
	if (tr.Status().ok()) {
		auto trw = new TransactionWrapper(move(tr));
		ret.tx_id = reinterpret_cast<uintptr_t>(trw);
	} else {
		ret.err = error2c(tr.Status());
	}
	return ret;
}

reindexer_error reindexer_rollback_transaction(uintptr_t rx, uintptr_t tr) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	if (!db) {
		return error2c(err_not_init);
	}
	auto trw = std::unique_ptr<TransactionWrapper>(reinterpret_cast<TransactionWrapper*>(tr));
	if (!trw) {
		return error2c(errOK);
	}
	auto err = db->RollBackTransaction(trw->tr_);
	return error2c(err);
}

reindexer_ret reindexer_commit_transaction(uintptr_t rx, uintptr_t tr, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};

	if (!rx) {
		return ret2c(err_not_init, out);
	}
	std::unique_ptr<TransactionWrapper> trw(reinterpret_cast<TransactionWrapper*>(tr));
	if (!trw) {
		return ret2c(errOK, out);
	}

	std::unique_ptr<QueryResultsWrapper> res(new_results());
	if (!res) {
		return ret2c(err_too_many_queries, out);
	}

	CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

	auto err = rdxKeeper.db().CommitTransaction(trw->tr_, *res);

	if (err.ok()) {
		int32_t ptVers = -1;
		results2c(res.release(), &out, 0, trw->tr_.IsTagsUpdated() ? &ptVers : nullptr, trw->tr_.IsTagsUpdated() ? 1 : 0);
	}

	return ret2c(err, out);
}

reindexer_error reindexer_open_namespace(uintptr_t rx, reindexer_string nsName, StorageOpts opts, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().OpenNamespace(str2cv(nsName), opts);
	}
	return error2c(res);
}

reindexer_error reindexer_drop_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().DropNamespace(str2cv(nsName));
	}
	return error2c(res);
}

reindexer_error reindexer_truncate_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().TruncateNamespace(str2cv(nsName));
	}
	return error2c(res);
}

reindexer_error reindexer_rename_namespace(uintptr_t rx, reindexer_string srcNsName, reindexer_string dstNsName,
										   reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().RenameNamespace(str2cv(srcNsName), str2c(dstNsName));
	}
	return error2c(res);
}

reindexer_error reindexer_close_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().CloseNamespace(str2cv(nsName));
	}
	return error2c(res);
}

reindexer_error reindexer_add_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		string json(str2cv(indexDefJson));
		IndexDef indexDef;

		auto err = indexDef.FromJSON(giftStr(json));
		if (!err.ok()) {
			return error2c(err);
		}

		res = rdxKeeper.db().AddIndex(str2cv(nsName), indexDef);
	}
	return error2c(res);
}

reindexer_error reindexer_update_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		string json(str2cv(indexDefJson));
		IndexDef indexDef;

		auto err = indexDef.FromJSON(giftStr(json));
		if (!err.ok()) {
			return error2c(err);
		}

		res = rdxKeeper.db().UpdateIndex(str2cv(nsName), indexDef);
	}
	return error2c(res);
}

reindexer_error reindexer_drop_index(uintptr_t rx, reindexer_string nsName, reindexer_string index, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().DropIndex(str2cv(nsName), IndexDef(str2c(index)));
	}
	return error2c(res);
}

reindexer_error reindexer_enable_storage(uintptr_t rx, reindexer_string path, reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().EnableStorage(str2c(path));
	}
	return error2c(res);
}

reindexer_error reindexer_connect(uintptr_t rx, reindexer_string dsn, ConnectOpts opts) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (!db) return error2c(err_not_init);
	Error err = db->Connect(str2c(dsn), opts);
	if (err.ok() && db->NeedTraceActivity()) db->SetActivityTracer("builtin", "");
	return error2c(err);
}

reindexer_error reindexer_init_system_namespaces(uintptr_t rx) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (!db) return error2c(err_not_init);
	Error err = db->InitSystemNamespaces();
	if (err.ok() && db->NeedTraceActivity()) db->SetActivityTracer("builtin", "");
	return error2c(err);
}

reindexer_ret reindexer_select(uintptr_t rx, reindexer_string query, int as_json, int32_t* pt_versions, int pt_versions_count,
							   reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out = {0, 0, 0};
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		auto result = new_results();
		if (!result) {
			return ret2c(err_too_many_queries, out);
		}
		res = rdxKeeper.db().Select(str2cv(query), *result);
		if (res.ok()) {
			results2c(result, &out, as_json, pt_versions, pt_versions_count);
		} else {
			put_results_to_pool(result);
		}
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_select_query(uintptr_t rx, struct reindexer_buffer in, int as_json, int32_t* pt_versions, int pt_versions_count,
									 reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	reindexer_resbuffer out = {0, 0, 0};
	if (rx) {
		res = Error(errOK);
		Serializer ser(in.data, in.len);
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

		Query q;
		q.Deserialize(ser);
		while (!ser.Eof()) {
			JoinedQuery q1;
			q1.joinType = JoinType(ser.GetVarUint());
			q1.Deserialize(ser);
			q1.debugLevel = q.debugLevel;
			if (q1.joinType == JoinType::Merge) {
				q.mergeQueries_.emplace_back(std::move(q1));
			} else {
				q.joinQueries_.emplace_back(std::move(q1));
			}
		}

		QueryResultsWrapper* result = new_results();
		if (!result) {
			return ret2c(err_too_many_queries, out);
		}
		res = rdxKeeper.db().Select(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what());
		if (res.ok()) {
			results2c(result, &out, as_json, pt_versions, pt_versions_count);
		} else {
			put_results_to_pool(result);
		}
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_delete_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	if (rx) {
		res = Error(errOK);
		Serializer ser(in.data, in.len);
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

		Query q;
		q.type_ = QueryDelete;
		q.Deserialize(ser);
		QueryResultsWrapper* result = new_results();
		if (!result) {
			return ret2c(err_too_many_queries, out);
		}
		res = rdxKeeper.db().Delete(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what());
		if (res.ok()) {
			results2c(result, &out);
		} else {
			put_results_to_pool(result);
		}
		ctx_pool.removeContext(ctx_info);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_update_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	if (rx) {
		res = Error(errOK);
		Serializer ser(in.data, in.len);
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);

		Query q;
		q.Deserialize(ser);
		q.type_ = QueryUpdate;
		QueryResultsWrapper* result = new_results();
		if (!result) {
			ctx_pool.removeContext(ctx_info);
			return ret2c(err_too_many_queries, out);
		}
		res = rdxKeeper.db().Update(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what());
		if (res.ok()) {
			int32_t ptVers = -1;
			results2c(result, &out, 0, &ptVers, 1);
		} else {
			put_results_to_pool(result);
		}
	}
	return ret2c(res, out);
}

reindexer_error reindexer_delete_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	TransactionWrapper* trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(errOK);
	}
	Serializer ser(in.data, in.len);
	Query q;
	q.Deserialize(ser);
	q.type_ = QueryDelete;

	trw->tr_.Modify(std::move(q));

	return error2c(errOK);
}

reindexer_error reindexer_update_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	TransactionWrapper* trw = reinterpret_cast<TransactionWrapper*>(tr);
	if (!db) {
		return error2c(err_not_init);
	}
	if (!tr) {
		return error2c(errOK);
	}
	Serializer ser(in.data, in.len);
	Query q;
	q.Deserialize(ser);
	q.type_ = QueryUpdate;

	trw->tr_.Modify(std::move(q));

	return error2c(errOK);
}

reindexer_error reindexer_put_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_string data,
								   reindexer_ctx_info ctx_info) {
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		res = rdxKeeper.db().PutMeta(str2c(ns), str2c(key), str2c(data));
	}
	return error2c(res);
}

reindexer_ret reindexer_get_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_ctx_info ctx_info) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	if (rx) {
		CGORdxCtxKeeper rdxKeeper(rx, ctx_info, ctx_pool);
		QueryResultsWrapper* results = new_results();
		if (!results) {
			return ret2c(err_too_many_queries, out);
		}

		string data;
		res = rdxKeeper.db().GetMeta(str2c(ns), str2c(key), data);
		results->ser.Write(data);
		out.len = results->ser.Len();
		out.data = uintptr_t(results->ser.Buf());
		out.results_ptr = uintptr_t(results);
	}
	return ret2c(res, out);
}

reindexer_error reindexer_commit(uintptr_t rx, reindexer_string nsName) {
	auto db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->Commit(str2cv(nsName)));
}

void reindexer_enable_logger(void (*logWriter)(int, char*)) { logInstallWriter(logWriter); }

void reindexer_disable_logger() { logInstallWriter(nullptr); }

reindexer_error reindexer_free_buffer(reindexer_resbuffer in) {
	put_results_to_pool(reinterpret_cast<QueryResultsWrapper*>(in.results_ptr));
	return error2c(Error(errOK));
}

reindexer_error reindexer_free_buffers(reindexer_resbuffer* in, int count) {
	for (int i = 0; i < count; i++) {
		reindexer_free_buffer(in[i]);
	}
	return error2c(Error(errOK));
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
			assert(false);
	}
	if (ctx_pool.cancelContext(ctx_info, howCPP)) {
		return error2c(Error(errOK));
	}
	return error2c(Error(errParams));
}
