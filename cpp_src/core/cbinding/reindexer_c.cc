#include "reindexer_c.h"

#include <stdlib.h>
#include <string.h>
#include <locale>
#include <mutex>

#include "core/reindexer.h"
#include "core/selectfunc/selectfuncparser.h"
#include "debug/allocdebug.h"
#include "resultserializer.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace reindexer;

const int kQueryResultsPoolSize = 1024;
const int kMaxConcurentQueries = 65534;

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

struct QueryResultsWrapper : public QueryResults {
public:
	WrResultSerializer ser;
};

static std::mutex res_pool_lck;
static h_vector<std::unique_ptr<QueryResultsWrapper>, 2> res_pool;
static int alloced_res_count;

void put_results_to_pool(QueryResultsWrapper* res) {
	res->Clear();
	res->ser.Reset();
	std::unique_lock<std::mutex> lck(res_pool_lck);
	alloced_res_count--;
	if (res_pool.size() < kQueryResultsPoolSize)
		res_pool.push_back(std::unique_ptr<QueryResultsWrapper>(res));
	else
		delete res;
}

QueryResultsWrapper* new_results() {
	std::unique_lock<std::mutex> lck(res_pool_lck);
	if (alloced_res_count > kMaxConcurentQueries) {
		return nullptr;
	}
	alloced_res_count++;
	if (res_pool.empty()) {
		return new QueryResultsWrapper;
	} else {
		auto res = res_pool.back().release();
		res_pool.pop_back();
		return res;
	}
}

static void results2c(QueryResultsWrapper* result, struct reindexer_resbuffer* out, int with_items = 0, int32_t* pt_versions = nullptr,
					  int pt_versions_count = 0) {
	int flags = with_items ? kResultsJson : (kResultsPtrs | kResultsWithItemID);

	flags |= (pt_versions && with_items == 0) ? kResultsWithPayloadTypes : 0;

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
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	delete db;
	db = nullptr;
}

reindexer_error reindexer_ping(uintptr_t rx) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(db ? Error(errOK) : err_not_init);
}

reindexer_ret reindexer_modify_item_packed(uintptr_t rx, reindexer_buffer args, reindexer_buffer data) {
	Serializer ser(args.data, args.len);
	string ns = ser.GetVString().ToString();
	int format = ser.GetVarUint();
	int mode = ser.GetVarUint();
	int state_token = ser.GetVarUint();
	int tx_id = ser.GetVarUint();
	(void)tx_id;
	unsigned preceptsCount = ser.GetVarUint();
	vector<string> precepts;
	while (preceptsCount--) {
		precepts.push_back(ser.GetVString().ToString());
	}

	reindexer_resbuffer out = {0, 0, 0};
	Error err = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		Item item = db->NewItem(ns);

		if (item.Status().ok()) {
			switch (format) {
				case FormatJson:
					err = item.Unsafe().FromJSON(string_view(reinterpret_cast<const char*>(data.data), data.len), 0, mode == ModeDelete);
					break;
				case FormatCJson:
					if (item.GetStateToken() != state_token)
						err = Error(errStateInvalidated, "stateToken mismatch:  %08X, need %08X. Can't process item", state_token,
									item.GetStateToken());
					else
						err = item.Unsafe().FromCJSON(string_view(reinterpret_cast<const char*>(data.data), data.len), mode == ModeDelete);
					break;
				default:
					err = Error(-1, "Invalid source item format %d", format);
			}
			if (err.ok()) {
				item.SetPrecepts(precepts);
				switch (mode) {
					case ModeUpsert:
						err = db->Upsert(ns, item);
						break;
					case ModeInsert:
						err = db->Insert(ns, item);
						break;
					case ModeUpdate:
						err = db->Update(ns, item);
						break;
					case ModeDelete:
						err = db->Delete(ns, item);
						break;
				}
				if (err.ok()) {
					QueryResultsWrapper* res = new_results();
					if (!res) return ret2c(err_too_many_queries, out);
					res->AddItem(item);
					int32_t ptVers = -1;
					bool tmUpdated = item.IsTagsUpdated();
					results2c(res, &out, 0, tmUpdated ? &ptVers : nullptr, tmUpdated ? 1 : 0);
				}
			}
		} else {
			err = item.Status();
		}
	}
	return ret2c(err, out);
}

reindexer_error reindexer_open_namespace(uintptr_t rx, reindexer_string _namespace, StorageOpts opts, uint8_t cacheMode) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->OpenNamespace(str2c(_namespace), opts, static_cast<CacheMode>(cacheMode)));
}

reindexer_error reindexer_drop_namespace(uintptr_t rx, reindexer_string _namespace) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->DropNamespace(str2c(_namespace)));
}

reindexer_error reindexer_close_namespace(uintptr_t rx, reindexer_string _namespace) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->CloseNamespace(str2c(_namespace)));
}

reindexer_error reindexer_add_index(uintptr_t rx, reindexer_string _namespace, reindexer_string indexDefJson) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	string json = str2c(indexDefJson);
	IndexDef indexDef;

	try {
		indexDef.FromJSON(&json[0]);
	} catch (const Error& err) {
		return error2c(err);
	}

	return error2c(!db ? err_not_init : db->AddIndex(str2c(_namespace), indexDef));
}

reindexer_error reindexer_update_index(uintptr_t rx, reindexer_string _namespace, reindexer_string indexDefJson) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	string json = str2c(indexDefJson);
	IndexDef indexDef;

	try {
		indexDef.FromJSON(&json[0]);
	} catch (const Error& err) {
		return error2c(err);
	}

	return error2c(!db ? err_not_init : db->UpdateIndex(str2c(_namespace), indexDef));
}

reindexer_error reindexer_drop_index(uintptr_t rx, reindexer_string _namespace, reindexer_string index) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->DropIndex(str2c(_namespace), str2c(index)));
}

reindexer_error reindexer_enable_storage(uintptr_t rx, reindexer_string path) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->EnableStorage(str2c(path)));
}

reindexer_error reindexer_init_system_namespaces(uintptr_t rx) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->InitSystemNamespaces());
}

reindexer_ret reindexer_select(uintptr_t rx, reindexer_string query, int with_items, int32_t* pt_versions, int pt_versions_count) {
	reindexer_resbuffer out = {0, 0, 0};
	Error res = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		QueryResultsWrapper* result = new_results();
		if (!result) return ret2c(err_too_many_queries, out);
		res = db->Select(str2cv(query), *result);
		if (res.ok())
			results2c(result, &out, with_items, pt_versions, pt_versions_count);
		else
			put_results_to_pool(result);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_select_query(uintptr_t rx, struct reindexer_buffer in, int with_items, int32_t* pt_versions,
									 int pt_versions_count) {
	Error res = err_not_init;
	reindexer_resbuffer out = {0, 0, 0};
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		res = Error(errOK);
		Serializer ser(in.data, in.len);

		Query q;
		q.Deserialize(ser);
		while (!ser.Eof()) {
			Query q1;
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
		if (!result) return ret2c(err_too_many_queries, out);
		res = db->Select(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		if (res.ok())
			results2c(result, &out, with_items, pt_versions, pt_versions_count);
		else
			put_results_to_pool(result);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_delete_query(uintptr_t rx, reindexer_buffer in) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		res = Error(errOK);
		Serializer ser(in.data, in.len);

		Query q;
		q.Deserialize(ser);
		QueryResultsWrapper* result = new_results();
		if (!result) return ret2c(err_too_many_queries, out);
		res = db->Delete(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		if (res.ok())
			results2c(result, &out);
		else
			put_results_to_pool(result);
	}
	return ret2c(res, out);
}

reindexer_error reindexer_put_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_string data) {
	Error res = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		res = db->PutMeta(str2c(ns), str2c(key), str2c(data));
	}
	return error2c(res);
}

reindexer_ret reindexer_get_meta(uintptr_t rx, reindexer_string ns, reindexer_string key) {
	reindexer_resbuffer out{0, 0, 0};
	Error res = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		QueryResultsWrapper* results = new_results();
		if (!results) return ret2c(err_too_many_queries, out);

		string data;
		res = db->GetMeta(str2c(ns), str2c(key), data);
		results->ser.Write(data);
		out.len = results->ser.Len();
		out.data = uintptr_t(results->ser.Buf());
		out.results_ptr = uintptr_t(results);
	}
	return ret2c(res, out);
}

reindexer_error reindexer_commit(uintptr_t rx, reindexer_string _namespace) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->Commit(str2c(_namespace)));
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
