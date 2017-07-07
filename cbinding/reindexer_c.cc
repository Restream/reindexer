#include "cbinding/reindexer_c.h"
#include "cbinding/serializer.h"
#include "core/reindexer.h"
#include "tools/allocdebug.h"
#include "tools/logger.h"

#include <stdlib.h>
#include <string.h>
#include <locale>

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

static string str2c(reindexer_string gs) { return string((const char *)gs.p, gs.n); }

static void results2c(const QueryResults &result, struct reindexer_buffer *results, int with_items = 0) {
	WrSerializer ser(false);

	// Total
	ser.PutInt(result.totalCount);
	// Count of returned items
	ser.PutInt(result.size());
	for (unsigned i = 0; i < result.size(); i++) {
		// Put Item ID and version
		auto &it = result[i];
		ser.PutInt(it.id);
		ser.PutInt(it.version);
		if (with_items) result.GetJSON(i, ser);

		auto jres = result.joined_.find(it.id);
		if (jres == result.joined_.end()) {
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
				// Put items from joined name  space ID
				ser.PutInt(iit.id);
				// Put version
				ser.PutInt(iit.version);
				if (with_items) jfres.GetJSON(j, ser);
			}
		}
	}

	results->len = ser.Len();
	results->data = ser.DetachBuffer();
}

static Error reindex_deserialize_item(Serializer &ser, Item *item) {
	Error err(errOK);
	while (err.ok() && !ser.Eof()) {
		string idxName = ser.GetString();
		int count = ser.GetInt();
		KeyRefs kvs;
		kvs.reserve(count);
		while (count--) kvs.push_back(ser.GetRef());
		err = item->SetField(idxName, kvs);
	}
	return err;
}

static void reindexer_parse_query_buf(Serializer &ser, Query &q) {
	while (!ser.Eof()) {
		QueryEntry qe;
		QueryJoinEntry qje;

		int qtype = ser.GetInt();
		switch (qtype) {
			case QueryCondition: {
				qe.index = ser.GetString();
				qe.op = (OpType)ser.GetInt();
				qe.condition = (CondType)ser.GetInt();
				int count = ser.GetInt();
				qe.values.reserve(count);
				while (count--) qe.values.push_back(ser.GetValue());
				q.entries.push_back(qe);
				break;
			}
			case QueryDistinct:
				qe.index = ser.GetString();
				qe.distinct = true;
				qe.condition = CondAny;
				q.entries.push_back(qe);
				break;
			case QuerySortIndex:
				q.sortBy = ser.GetString();
				q.sortDirDesc = bool(ser.GetInt());
				break;
			case QueryJoinOn:
				qje.condition_ = (CondType)ser.GetInt();
				qje.index_ = ser.GetString();
				qje.joinIndex_ = ser.GetString();
				q.joinEntries_.push_back(move(qje));
				break;
			case QueryEnd:
				return;
		}
	}
}

static Error err_not_init(-1, "Reindexer db has not initialized");

reindexer_ret reindexer_upsert(reindexer_buffer in) {
	reindexer_buffer out;
	Error err = err_not_init;
	if (db) {
		Serializer ser(in.data, in.len);
		string ns = ser.GetString();
		auto item = unique_ptr<Item>(db->NewItem(ns));
		if (item->Status().ok()) {
			err = item->FromJSON(ser.GetSlice());
			if (err.ok()) {
				err = db->Upsert(ns, item.get());
				results2c(QueryResults(item->GetRef()), &out);
			}
		} else
			err = item->Status();
	}
	return ret2c(err, out);
}

reindexer_ret reindexer_delete(reindexer_buffer in) {
	reindexer_buffer out;
	Error err = err_not_init;
	if (db) {
		Serializer ser(in.data, in.len);
		string ns = ser.GetString();
		int format = ser.GetInt();
		auto item = unique_ptr<Item>(db->NewItem(ns));
		if (item->Status().ok()) {
			err = (format) ? reindex_deserialize_item(ser, item.get()) : item->FromJSON(ser.GetSlice());
			if (err.ok()) {
				err = db->Delete(ns, item.get());
				results2c(QueryResults(item->GetRef()), &out);
			}
		} else
			err = item->Status();
	}
	return ret2c(err, out);
}

reindexer_error reindexer_addnamespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->AddNamespace(str2c(_namespace)));
}

reindexer_error reindexer_delete_namespace(reindexer_string _namespace) {
	return error2c(!db ? err_not_init : db->DeleteNamespace(str2c(_namespace)));
}
reindexer_error reindexer_clone_namespace(reindexer_string src, reindexer_string dst) {
	return error2c(!db ? err_not_init : db->CloneNamespace(str2c(src), str2c(dst)));
}

reindexer_error reindexer_rename_namespace(reindexer_string src, reindexer_string dst) {
	return error2c(!db ? err_not_init : db->RenameNamespace(str2c(src), str2c(dst)));
}

reindexer_error reindexer_addindex(reindexer_string _namespace, reindexer_string index, reindexer_string json_path, IndexType type,
								   IndexOpts indexOpts) {
	return error2c(!db ? err_not_init : db->AddIndex(str2c(_namespace), str2c(index), str2c(json_path), type, &indexOpts));
}

reindexer_error reindexer_enable_storage(reindexer_string _namespace, reindexer_string path) {
	StorageOpts opts;
	opts.createIfMissing = true;
	opts.dropIfCorrupted = true;
	opts.dropIfFieldsMismatch = true;
	return error2c(!db ? err_not_init : db->EnableStorage(str2c(_namespace), str2c(path)));
}

reindexer_ret reindexer_select(reindexer_string query, int with_items) {
	reindexer_buffer out;
	Error res = err_not_init;
	if (db) {
		QueryResults result;
		res = db->Select(str2c(query), result);
		results2c(result, &out, with_items);
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_select_query(reindexer_query query, int with_items, struct reindexer_buffer in) {
	Error res = err_not_init;
	reindexer_buffer out;
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);

		Query q(ser.GetString(), query.offset, query.limit, query.calc_total);
		reindexer_parse_query_buf(ser, q);
		q.debugLevel = query.debug_level;
		while (!ser.Eof()) {
			auto joinType = (JoinType)ser.GetInt();
			Query q1(ser.GetString(), 0, 0, 0);
			q1.joinType = joinType;
			reindexer_parse_query_buf(ser, q1);
			q1.debugLevel = q.debugLevel;
			q.joinQueries_.push_back(q1);
		}

		QueryResults result;
		res = db->Select(q, result);
		if (q.debugLevel >= LogError && res.code() != errOK) logPrintf(LogError, "Query error %s", res.what().c_str());
		results2c(result, &out, with_items);
		allocdebug_show();
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_get_items(reindexer_buffer in) {
	reindexer_buffer out;
	Error res = err_not_init;
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);
		WrSerializer wrSer(false);
		auto ns = ser.GetString();
		auto cnt = ser.GetInt();
		for (int i = 0; i < cnt; i++) {
			auto id = ser.GetInt();
			auto item = unique_ptr<Item>(db->GetItem(ns, id));
			if (item->Status().ok()) {
				wrSer.PutInt(item->GetRef().id);
				wrSer.PutInt(item->GetRef().version);
				wrSer.PutSlice(item->GetJSON());
			}
		}
		out.len = wrSer.Len();
		out.data = wrSer.DetachBuffer();
	}
	return ret2c(res, out);
}

reindexer_ret reindexer_delete_query(reindexer_query query, reindexer_buffer in) {
	reindexer_buffer out;
	Error res = err_not_init;
	if (db) {
		res = Error(errOK, "");
		Serializer ser(in.data, in.len);

		Query q(ser.GetString(), query.offset, query.limit, query.calc_total);
		reindexer_parse_query_buf(ser, q);
		q.debugLevel = query.debug_level;
		QueryResults result;
		res = db->Delete(q, result);
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
	reindexer_buffer out;
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

reindexer_error reindexer_commit(reindexer_string _namespace) { return error2c(!db ? err_not_init : db->Commit(str2c(_namespace))); }

void reindexer_enable_logger(void (*logWriter)(int, char *)) { logInstallWriter(logWriter); }

void reindexer_disable_logger() { logInstallWriter(nullptr); }

reindexer_error reindexer_reset_stats() { return error2c(!db ? err_not_init : db->ResetStats()); }
reindexer_stat reindexer_get_stats() {
	reindexer_stat stat;
	db->GetStats(stat);
	return stat;
}
