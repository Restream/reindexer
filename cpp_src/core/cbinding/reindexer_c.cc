#include "reindexer_c.h"

#include <stdlib.h>
#include <string.h>
#include <locale>

#include "core/reindexer.h"
#include "core/selectfunc/selectfuncparser.h"
#include "debug/allocdebug.h"
#include "resultserializer.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace reindexer;

static reindexer_error error2c(const Error& err_) {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	return err;
}

static reindexer_ret ret2c(const Error& err_, const reindexer_resbuffer& out) {
	reindexer_ret ret;
	ret.err.code = err_.code();
	ret.err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	ret.out = out;
	return ret;
}

static string str2c(reindexer_string gs) { return string(reinterpret_cast<const char*>(gs.p), gs.n); }

struct QueryResultsWrapper : public QueryResults {
public:
	WrResultSerializer ser;
};

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

static Error err_not_init(-1, "Reindexer db has not initialized");

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

reindexer_ret reindexer_modify_item(uintptr_t rx, reindexer_string _namespace, int format, reindexer_buffer data, int mode,
									reindexer_buffer percepts_pack, int state_token, int /*tx_id*/) {
	reindexer_resbuffer out = {0, 0, 0};
	Error err = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		string ns = str2c(_namespace);
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
				if (percepts_pack.len) {
					Serializer ser(percepts_pack.data, percepts_pack.len);
					unsigned preceptsCount = ser.GetVarUint();
					vector<string> precepts;
					for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
						string precept = ser.GetVString().ToString();
						precepts.push_back(precept);
					}
					item.SetPrecepts(precepts);
				}
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
				QueryResultsWrapper* res = new QueryResultsWrapper();
				res->AddItem(item);
				int32_t ptVers = -1;
				bool tmUpdated = item.IsTagsUpdated();
				results2c(res, &out, 0, tmUpdated ? &ptVers : nullptr, tmUpdated ? 1 : 0);
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
		QueryResultsWrapper* result = new QueryResultsWrapper();
		res = db->Select(str2c(query), *result);
		if (res.code() == errOK) results2c(result, &out, with_items, pt_versions, pt_versions_count);
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

		QueryResultsWrapper* result = new QueryResultsWrapper();
		res = db->Select(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		if (res.code() == errOK) results2c(result, &out, with_items, pt_versions, pt_versions_count);
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
		QueryResultsWrapper* result = new QueryResultsWrapper();
		res = db->Delete(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		if (res.code() == errOK) results2c(result, &out);
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
		QueryResultsWrapper* results = new QueryResultsWrapper;
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
	delete reinterpret_cast<QueryResultsWrapper*>(in.results_ptr);
	return error2c(Error(errOK));
}

reindexer_error reindexer_free_buffers(reindexer_resbuffer* in, int count) {
	for (int i = 0; i < count; i++) {
		reindexer_free_buffer(in[i]);
	}
	return error2c(Error(errOK));
}
