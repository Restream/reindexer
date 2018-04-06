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

static Reindexer *db = nullptr;

void init_reindexer() {
	if (db) {
		abort();
	}
	db = new Reindexer();
	setvbuf(stdout, 0, _IONBF, 0);
	setvbuf(stderr, 0, _IONBF, 0);
	setlocale(LC_CTYPE, "");
	setlocale(LC_NUMERIC, "C");
}

void destroy_reindexer() {
	delete db;
	db = nullptr;
}

static reindexer_error error2c(const Error &err_) {
	reindexer_error err;
	err.code = err_.code();
	err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	return err;
}
static reindexer_ret ret2c(const Error &err_, const reindexer_buffer &out) {
	reindexer_ret ret;
	ret.err.code = err_.code();
	ret.err.what = err_.what().length() ? strdup(err_.what().c_str()) : nullptr;
	ret.out = out;
	return ret;
}

static string str2c(reindexer_string gs) { return string(reinterpret_cast<const char *>(gs.p), gs.n); }

static void results2c(const QueryResults *result, struct reindexer_buffer *results, int with_items = 0, int32_t *pt_versions = nullptr) {
	int flags = with_items ? kResultsWithJson : kResultsWithPtrs;

	flags |= (pt_versions && with_items == 0) ? kResultsWithPayloadTypes : 0;

	ResultFetchOpts opts{flags, pt_versions, 0, INT_MAX, -1};
	ResultSerializer ser(false, opts);
	ser.PutResults(result);

	results->len = ser.Len();
	results->data = ser.DetachBuffer();
	results->results_flag = 1;
}

static Error err_not_init(-1, "Reindexer db has not initialized");

reindexer_ret reindexer_modify_item(reindexer_buffer in, int mode) {
	reindexer_buffer out = {0, 0, nullptr};
	Error err = err_not_init;
	if (db) {
		Serializer ser(in.data, in.len);
		string ns = ser.GetVString().ToString();
		int format = ser.GetVarUint();
		Item item = db->NewItem(ns);
		if (item.Status().ok()) {
			switch (format) {
				case FormatJson:
					err = item.Unsafe().FromJSON(ser.GetSlice());
					break;
				case FormatCJson: {
					err = item.Unsafe().FromCJSON(ser.GetSlice());
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
				QueryResults *res = new QueryResults();
				res->AddItem(item);
				int32_t ptVers = -1;
				bool tmUpdated = item.IsTagsUpdated();
				results2c(res, &out, 0, tmUpdated ? &ptVers : nullptr);
			}
		} else {
			err = item.Status();
		}
	}
	return ret2c(err, out);
}

reindexer_error reindexer_open_namespace(reindexer_string _namespace, StorageOpts opts) {
	return error2c(!db ? err_not_init : db->OpenNamespace(str2c(_namespace), opts));
}

reindexer_error reindexer_drop_namespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->DropNamespace(str2c(_namespace)));
}

reindexer_error reindexer_close_namespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->CloseNamespace(str2c(_namespace)));
}

reindexer_error reindexer_add_index(reindexer_string _namespace, reindexer_string index, reindexer_string json_path,
									reindexer_string index_type, reindexer_string field_type, IndexOpts indexOpts) {
	return error2c(
		!db ? err_not_init
			: db->AddIndex(str2c(_namespace), IndexDef{str2c(index), str2c(json_path), str2c(index_type), str2c(field_type), indexOpts}));
}

reindexer_error reindexer_configure_index(reindexer_string _namespace, reindexer_string index, reindexer_string config) {
	return error2c(!db ? err_not_init : db->ConfigureIndex(str2c(_namespace), str2c(index), str2c(config)));
}

reindexer_error reindexer_enable_storage(reindexer_string path) {
	// Enable storage
	// TODO: Do not skip placeholder check
	return error2c(!db ? err_not_init : db->EnableStorage(str2c(path), true));
}

reindexer_ret reindexer_select(reindexer_string query, int with_items, int32_t *pt_versions) {
	reindexer_buffer out = {0, 0, nullptr};
	Error res = err_not_init;
	if (db) {
		auto result = new QueryResults;
		res = db->Select(str2c(query), *result);
		results2c(result, &out, with_items, pt_versions);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_select_query(struct reindexer_buffer in, int with_items, int32_t *pt_versions) {
	Error res = err_not_init;
	reindexer_buffer out = {0, 0, nullptr};
	if (db) {
		res = Error(errOK, "");
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
		results2c(result, &out, with_items, pt_versions);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_delete_query(reindexer_buffer in) {
	reindexer_buffer out{0, 0, nullptr};
	Error res = err_not_init;
	if (db) {
		res = Error(errOK, "");
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

reindexer_error reindexer_put_meta(reindexer_string ns, reindexer_string key, reindexer_string data) {
	Error res = err_not_init;
	if (db) {
		res = db->PutMeta(str2c(ns), str2c(key), str2c(data));
	}
	return error2c(res);
}

reindexer_ret reindexer_get_meta(reindexer_string ns, reindexer_string key) {
	reindexer_buffer out{0, 0, nullptr};
	Error res = err_not_init;
	if (db) {
		WrSerializer wrSer(false);
		string data;
		res = db->GetMeta(str2c(ns), str2c(key), data);
		wrSer.PutVString(data);
		out.len = wrSer.Len();
		out.data = wrSer.DetachBuffer();
	}
	return ret2c(res, out);
}

reindexer_error reindexer_commit(reindexer_string _namespace) { return error2c(!db ? err_not_init : db->Commit(str2c(_namespace))); }

void reindexer_enable_logger(void (*logWriter)(int, char *)) { logInstallWriter(logWriter); }

void reindexer_disable_logger() { logInstallWriter(nullptr); }

reindexer_error reindexer_reset_stats() { return error2c(!db ? err_not_init : db->ResetStats()); }
reindexer_stat reindexer_get_stats() {
	reindexer_stat stat;
	db->GetStats(stat);
	return stat;
}

reindexer_error reindexer_free_buffer(reindexer_buffer in) {
	if (in.results_flag) {
		auto addr = *reinterpret_cast<uint64_t *>(in.data);
		auto results = reinterpret_cast<QueryResults *>(addr);
		delete results;
	}
	free(in.data);
	return error2c(Error(errOK));
}

reindexer_error reindexer_free_buffers(reindexer_buffer *in, int count) {
	for (int i = 0; i < count; i++) {
		reindexer_free_buffer(in[i]);
	}
	return error2c(Error(errOK));
}
