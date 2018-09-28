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

static void results2c(const QueryResults* result, struct reindexer_resbuffer* results, int with_items = 0, int32_t* pt_versions = nullptr,
					  int pt_versions_count = 0) {
	int flags = with_items ? kResultsWithJson : kResultsWithPtrs;

	flags |= (pt_versions && with_items == 0) ? kResultsWithPayloadTypes : 0;

	ResultFetchOpts opts{flags, pt_versions, pt_versions_count, 0, INT_MAX, -1};
	WrResultSerializer ser(false, opts);
	ser.PutResults(result);

	results->len = ser.Len();
	results->data = uintptr_t(ser.DetachBuffer());
	results->results_flag = 1;
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

reindexer_ret reindexer_modify_item(uintptr_t rx, reindexer_buffer in, int mode) {
	reindexer_resbuffer out = {0, 0, 0};
	Error err = err_not_init;
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	if (db) {
		Serializer ser(in.data, in.len);
		string ns = ser.GetVString().ToString();
		int format = ser.GetVarUint();
		Item item = db->NewItem(ns);
		if (item.Status().ok()) {
			switch (format) {
				case FormatJson:
					err = item.Unsafe().FromJSON(ser.GetSlice(), 0, mode == ModeDelete);
					break;
				case FormatCJson: {
					err = item.Unsafe().FromCJSON(ser.GetSlice(), mode == ModeDelete);
					break;
				}
				default:
					err = Error(-1, "Invalid source item format %d", format);
			}
			if (err.ok()) {
				unsigned preceptsCount = ser.GetVarUint();
				vector<string> precepts;
				for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
					string precept = ser.GetVString().ToString();
					precepts.push_back(precept);
				}
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
				QueryResults* res = new QueryResults();
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

reindexer_error reindexer_configure_index(uintptr_t rx, reindexer_string _namespace, reindexer_string index, reindexer_string config) {
	Reindexer* db = reinterpret_cast<Reindexer*>(rx);
	return error2c(!db ? err_not_init : db->ConfigureIndex(str2c(_namespace), str2c(index), str2c(config)));
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
		auto result = new QueryResults;
		res = db->Select(str2c(query), *result);
		results2c(result, &out, with_items, pt_versions, pt_versions_count);
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

		auto result = new QueryResults;
		res = db->Select(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		results2c(result, &out, with_items, pt_versions, pt_versions_count);
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
		auto result = new QueryResults;
		res = db->Delete(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		results2c(result, &out);
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
		WrSerializer wrSer(false);
		string data;
		res = db->GetMeta(str2c(ns), str2c(key), data);
		wrSer.PutVString(data);
		out.len = wrSer.Len();
		out.data = uintptr_t(wrSer.DetachBuffer());
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
	if (in.results_flag) {
		auto addr = *reinterpret_cast<uint64_t*>(in.data);
		auto results = reinterpret_cast<QueryResults*>(addr);
		delete results;
	}
	free(reinterpret_cast<void*>(in.data));
	return error2c(Error(errOK));
}

reindexer_error reindexer_free_buffers(reindexer_resbuffer* in, int count) {
	for (int i = 0; i < count; i++) {
		reindexer_free_buffer(in[i]);
	}
	return error2c(Error(errOK));
}
