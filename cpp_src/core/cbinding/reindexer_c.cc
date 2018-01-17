#include "reindexer_c.h"
#include <stdlib.h>
#include <string.h>
#include <locale>
#include "core/reindexer.h"
#include "core/sqlfunc/sqlfunc.h"
#include "debug/allocdebug.h"
#include "resultserializer.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace reindexer;

static Reindexer *db;

void init_reindexer() {
	db = new Reindexer();
	setvbuf(stdout, 0, 0, _IONBF);
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

static void results2c(const QueryResults *result, struct reindexer_buffer *results, int with_items = 0) {
	ResultSerializer ser(false);

	ser.PutQueryParams(result);

	for (unsigned i = 0; i < result->size(); i++) {
		// Put Item ID and version
		auto &it = result->at(i);
		ser.PutItemParams(it);
		if (with_items) {
			if (i == 10) ser.Reserve(16ULL + result->size() * ser.Len() * 11 / 100ULL);
			result->GetJSON(i, ser);
		}

		auto jres = result->joined_.find(it.id);
		if (jres == result->joined_.end()) {
			ser.PutInt(0);
			continue;
		}
		// Put count of joined subqueires for item ID
		ser.PutInt(jres->second.size());
		for (auto &jfres : jres->second) {
			// Put count of returned items from joined namespace
			ser.PutInt(jfres.size());
			for (unsigned j = 0; j < jfres.size(); j++) {
				auto &iit = jfres[j];
				ser.PutItemParams(iit);
				if (with_items) jfres.GetJSON(j, ser);
			}
		}
	}

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
		string ns = ser.GetString();
		int format = ser.GetInt();
		auto item = unique_ptr<Item>(db->NewItem(ns));
		if (item->Status().ok()) {
			switch (format) {
				case FormatJson:
					err = item->FromJSON(ser.GetSlice());
					break;
				case FormatCJson:
					err = item->FromCJSON(ser.GetSlice());
					break;
				default:
					err = Error(-1, "Invalid source item format %d", format);
			}
			if (err.ok()) {
				unsigned preceptsCount = ser.GetInt();
				vector<string> precepts;
				for (unsigned prIndex = 0; prIndex < preceptsCount; prIndex++) {
					string precept = ser.GetString();
					precepts.push_back(precept);
				}
				item->SetPrecepts(precepts);

				switch (mode) {
					case ModeUpsert:
						err = db->Upsert(ns, item.get());
						break;
					case ModeInsert:
						err = db->Insert(ns, item.get());
						break;
					case ModeUpdate:
						err = db->Update(ns, item.get());
						break;
					case ModeDelete:
						err = db->Delete(ns, item.get());
						break;
				}
				results2c(item->GetRef().id == -1 ? new QueryResults() : new QueryResults({item->GetRef()}), &out);
			}
		} else {
			err = item->Status();
		}
	}
	return ret2c(err, out);
}

reindexer_error reindexer_open_namespace(reindexer_string _namespace, StorageOpts opts) {
	NamespaceDef nsDef(str2c(_namespace), opts);

	return error2c(!db ? err_not_init : db->OpenNamespace(nsDef));
}

reindexer_error reindexer_drop_namespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->DropNamespace(str2c(_namespace)));
}

reindexer_error reindexer_close_namespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->CloseNamespace(str2c(_namespace)));
}

reindexer_error reindexer_clone_namespace(reindexer_string src, reindexer_string dst) {
	return error2c(!db ? err_not_init : db->CloneNamespace(str2c(src), str2c(dst)));
}

reindexer_error reindexer_rename_namespace(reindexer_string src, reindexer_string dst) {
	return error2c(!db ? err_not_init : db->RenameNamespace(str2c(src), str2c(dst)));
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
	//
	return error2c(!db ? err_not_init : db->EnableStorage(str2c(path)));
}

reindexer_ret reindexer_select(reindexer_string query, int with_items) {
	reindexer_buffer out = {0, 0, nullptr};
	Error res = err_not_init;
	if (db) {
		auto result = new QueryResults;
		res = db->Select(str2c(query), *result);
		results2c(result, &out, with_items);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_select_query(int with_items, struct reindexer_buffer in) {
	Error res = err_not_init;
	reindexer_buffer out = {0, 0, nullptr};
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);

		Query q;
		q.Deserialize(ser);
		while (!ser.Eof()) {
			Query q1;
			q1.joinType = JoinType(ser.GetInt());
			q1.Deserialize(ser);
			q1.debugLevel = q.debugLevel;
			if (q1.joinType == JoinType::Merge) {
				q.mergeQueries_.push_back(q1);
			} else {
				q.joinQueries_.push_back(q1);
			}
		}

		auto result = new QueryResults;
		res = db->Select(q, *result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		results2c(result, &out, with_items);
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

reindexer_error reindexer_put_meta(reindexer_buffer in) {
	Error res = err_not_init;
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);
		auto ns = ser.GetString();
		auto key = ser.GetString();
		auto data = ser.GetSlice();
		res = db->PutMeta(ns, key, data);
	}
	return error2c(res);
}

reindexer_ret reindexer_get_meta(reindexer_buffer in) {
	reindexer_buffer out{0, 0, nullptr};
	Error res = err_not_init;
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);
		WrSerializer wrSer(false);
		auto ns = ser.GetString();
		auto key = ser.GetString();
		string data;
		res = db->GetMeta(ns, key, data);
		wrSer.PutSlice(Slice(data));
		out.len = wrSer.Len();
		out.data = wrSer.DetachBuffer();
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_get_payload_type(reindexer_buffer in, int nsid) {
	reindexer_buffer out{0, 0, nullptr};

	assert(in.results_flag);

	// in MUST be a buffer with query results
	auto addr = *reinterpret_cast<uint64_t *>(in.data);
	auto results = reinterpret_cast<QueryResults *>(addr);

	assert(nsid < int(results->ctxs.size()));
	const PayloadType &t = *results->ctxs[nsid].type_;
	const TagsMatcher &m = results->ctxs[nsid].tagsMatcher_;

	WrSerializer wrSer(false);

	// Serialize tags matcher
	wrSer.PutInt(m.version());
	wrSer.PutInt(m.size());
	for (unsigned i = 0; i < m.size(); i++) {
		wrSer.PutString(m.tag2name(i + 1));
	}

	// Serialize payload type
	wrSer.PutInt(base_key_string::export_hdr_offset());
	wrSer.PutInt(t.NumFields());
	for (int i = 0; i < t.NumFields(); i++) {
		wrSer.PutInt(t.Field(i).Type());
		wrSer.PutString(t.Field(i).Name());
		wrSer.PutInt(t.Field(i).Offset());
		wrSer.PutInt(t.Field(i).ElemSizeof());
		wrSer.PutInt(t.Field(i).IsArray());
	}

	out.len = wrSer.Len();
	out.data = wrSer.DetachBuffer();

	return ret2c(0, out);
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
