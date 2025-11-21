#include "reindexertestapi.h"
#include <thread>
#include "core/cjson/tagsmatcher.h"
#include "core/system_ns_names.h"
#include "gtests/tests/gtest_cout.h"
#include "utf8cpp/utf8/checked.h"
#include "vendor/gason/gason.h"

static constexpr auto kBasicTimeout = std::chrono::seconds(200);

template <typename DB>
ReindexerTestApi<DB>::ReindexerTestApi() : reindexer(std::make_shared<DB>()) {
	if constexpr (std::is_same_v<DB, reindexer::Reindexer>) {
		auto err = reindexer->Connect("builtin://");
		EXPECT_TRUE(err.ok()) << err.what();
		assertrx(err.ok());
	}
}

template <typename DB>
ReindexerTestApi<DB>::ReindexerTestApi(const typename DB::ConfigT& cfg) : reindexer(std::make_shared<DB>(cfg)) {
	if constexpr (std::is_same_v<DB, reindexer::Reindexer>) {
		auto err = reindexer->Connect("builtin://");
		EXPECT_TRUE(err.ok()) << err.what();
		assertrx(err.ok());
	}
}

template <typename DB>
void ReindexerTestApi<DB>::DefineNamespaceDataset(DB& rx, std::string_view ns, std::span<const IndexDeclaration> fields) {
	auto err = reindexer::Error();
	for (const auto& field : fields) {
		if (field.indexType != "composite") {
			err = rx.AddIndex(ns, {std::string{field.indexName},
								   {std::string{field.indexName}},
								   std::string{field.fieldType},
								   std::string{field.indexType},
								   field.indexOpts});
		} else {
			std::string indexName{field.indexName};
			std::string idxContents{field.indexName};
			auto eqPos = indexName.find_first_of('=');
			if (eqPos != std::string::npos) {
				idxContents = indexName.substr(0, eqPos);
				indexName = indexName.substr(eqPos + 1);
			}
			reindexer::JsonPaths jsonPaths;
			jsonPaths = reindexer::split(idxContents, "+", true, jsonPaths);

			err = rx.AddIndex(
				ns, {indexName, jsonPaths, std::string{field.fieldType}, std::string{field.indexType}, field.indexOpts, field.expireAfter});
		}
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

template <typename DB>
void ReindexerTestApi<DB>::Connect(const std::string& dsn, const ConnectOptsType& opts) {
	auto err = reindexer->Connect(dsn, opts);
	ASSERT_TRUE(err.ok()) << err.what() << "; dsn: " << dsn;
}

template <typename DB>
typename ReindexerTestApi<DB>::ItemType ReindexerTestApi<DB>::NewItem(std::string_view ns) {
	ItemType item = reindexer->WithTimeout(kBasicTimeout).NewItem(ns);
	EXPECT_TRUE(item.Status().ok()) << item.Status().what() << "; namespace: " << ns;
	return item;
}

template <typename DB>
void ReindexerTestApi<DB>::OpenNamespace(std::string_view ns, const StorageOpts& storage) {
	auto err = reindexer->WithTimeout(kBasicTimeout).OpenNamespace(ns, storage);
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns;
}

template <typename DB>
void ReindexerTestApi<DB>::CloseNamespace(std::string_view ns) {
	auto err = reindexer->WithTimeout(kBasicTimeout).CloseNamespace(ns);
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns;
}

template <typename DB>
void ReindexerTestApi<DB>::DropNamespace(std::string_view ns) {
	auto err = reindexer->WithTimeout(kBasicTimeout).DropNamespace(ns);
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns;
}

template <typename DB>
void ReindexerTestApi<DB>::AddNamespace(const reindexer::NamespaceDef& def) {
	auto err = reindexer->WithTimeout(kBasicTimeout).AddNamespace(def);
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << def.name;
}

template <typename DB>
void ReindexerTestApi<DB>::TruncateNamespace(std::string_view ns) {
	auto err = reindexer->WithTimeout(kBasicTimeout).TruncateNamespace(ns);
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns;
}

template <typename DB>
void ReindexerTestApi<DB>::AddIndex(std::string_view ns, const reindexer::IndexDef& idef) {
	auto err = reindexer->WithTimeout(kBasicTimeout).AddIndex(ns, idef);
	if (!err.ok()) {
		reindexer::WrSerializer ser;
		idef.GetJSON(ser);
		ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns << "; def: " << ser.Slice();
	}
}

template <typename DB>
void ReindexerTestApi<DB>::UpdateIndex(std::string_view ns, const reindexer::IndexDef& idef) {
	auto err = reindexer->WithTimeout(kBasicTimeout).UpdateIndex(ns, idef);
	if (!err.ok()) {
		reindexer::WrSerializer ser;
		idef.GetJSON(ser);
		ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns << "; def: " << ser.Slice();
	}
}

template <typename DB>
void ReindexerTestApi<DB>::DropIndex(std::string_view ns, std::string_view name) {
	auto err = reindexer->DropIndex(ns, reindexer::IndexDef(std::string(name)));
	ASSERT_TRUE(err.ok()) << err.what() << "; namespace: " << ns << "; name: " << name;
}

template <typename DB>
void ReindexerTestApi<DB>::Insert(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Insert(ns, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Insert(std::string_view ns, ItemType& item, QueryResultsType& qr) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Insert(ns, item, qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Upsert(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Upsert(std::string_view ns, ItemType& item, QueryResultsType& qr) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item, qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Update(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Update(ns, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Update(std::string_view ns, ItemType& item, QueryResultsType& qr) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Update(ns, item, qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::UpsertJSON(std::string_view ns, std::string_view json) {
	auto item = NewItem(ns);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what() << "; " << json;
	auto err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
	err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
}

template <typename DB>
void ReindexerTestApi<DB>::InsertJSON(std::string_view ns, std::string_view json) {
	auto item = NewItem(ns);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what() << "; " << json;
	auto err = item.FromJSON(json);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
	err = reindexer->WithTimeout(kBasicTimeout).Insert(ns, item);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << json;
}

template <typename DB>
void ReindexerTestApi<DB>::Update(const reindexer::Query& q, QueryResultsType& qr) {
	auto err = reindexer->WithTimeout(kBasicTimeout).Update(q, qr);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL(QueryUpdate);
}

template <typename DB>
size_t ReindexerTestApi<DB>::Update(const reindexer::Query& q) {
	QueryResultsType qr;
	Update(q, qr);
	return qr.Count();
}

template <typename DB>
typename ReindexerTestApi<DB>::QueryResultsType ReindexerTestApi<DB>::UpdateQR(const reindexer::Query& q) {
	QueryResultsType qr;
	Update(q, qr);
	return qr;
}

template <typename DB>
void ReindexerTestApi<DB>::Select(const reindexer::Query& q, QueryResultsType& qr) const {
	auto err = reindexer->WithTimeout(kBasicTimeout).Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL();
}

template <typename DB>
typename ReindexerTestApi<DB>::QueryResultsType ReindexerTestApi<DB>::Select(const reindexer::Query& q) const {
	QueryResultsType qr;
	Select(q, qr);
	return qr;
}

template <typename DB>
typename ReindexerTestApi<DB>::QueryResultsType ReindexerTestApi<DB>::ExecSQL(std::string_view sql) const {
	QueryResultsType qr;
	auto err = reindexer->WithTimeout(kBasicTimeout).ExecSQL(sql, qr);
	EXPECT_TRUE(err.ok()) << err.what() << "; " << sql;
	return qr;
}

template <typename DB>
void ReindexerTestApi<DB>::Delete(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Delete(ns, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
void ReindexerTestApi<DB>::Delete(std::string_view ns, ItemType& item, QueryResultsType& qr) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Delete(ns, item, qr);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
size_t ReindexerTestApi<DB>::Delete(const reindexer::Query& q) {
	QueryResultsType qr;
	auto err = reindexer->WithTimeout(kBasicTimeout).Delete(q, qr);
	EXPECT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL(QueryDelete);
	return qr.Count();
}

template <typename DB>
void ReindexerTestApi<DB>::Delete(const reindexer::Query& q, QueryResultsType& qr) {
	auto err = reindexer->WithTimeout(kBasicTimeout).Delete(q, qr);
	EXPECT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL(QueryDelete);
}

template <typename DB>
std::vector<reindexer::NamespaceDef> ReindexerTestApi<DB>::EnumNamespaces(reindexer::EnumNamespacesOpts opts) {
	std::vector<reindexer::NamespaceDef> defs;
	auto err = reindexer->WithTimeout(kBasicTimeout).EnumNamespaces(defs, opts);
	EXPECT_TRUE(err.ok()) << err.what();
	return defs;
}

template <typename DB>
void ReindexerTestApi<DB>::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName) {
	auto err = reindexer->WithTimeout(kBasicTimeout).RenameNamespace(srcNsName, dstNsName);
	EXPECT_TRUE(err.ok()) << err.what();
}

template <typename DB>
typename ReindexerTestApi<DB>::TransactionType ReindexerTestApi<DB>::NewTransaction(std::string_view ns) {
	auto tx = reindexer->WithTimeout(kBasicTimeout).NewTransaction(ns);
	EXPECT_TRUE(tx.Status().ok()) << tx.Status().what();
	return tx;
}

template <typename DB>
typename ReindexerTestApi<DB>::QueryResultsType ReindexerTestApi<DB>::CommitTransaction(TransactionType& tx) {
	QueryResultsType qr;
	auto err = reindexer->WithTimeout(kBasicTimeout).CommitTransaction(tx, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	return qr;
}

template <typename DB>
ReplicationTestState ReindexerTestApi<DB>::GetReplicationState(std::string_view ns) {
	using namespace reindexer;
	ReplicationTestState state;
	{
		Query qr = Query(reindexer::kMemStatsNamespace).Where("name", CondEq, ns);
		QueryResultsType res;
		auto err = reindexer->WithTimeout(kBasicTimeout).Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		for (auto it : res) {
			WrSerializer ser;
			err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			auto root = parser.Parse(ser.Slice());
			state.lsn.FromJSON(root["replication"]["last_lsn_v2"]);

			state.dataCount = root["replication"]["data_count"].As<int64_t>();
			state.dataHash = root["replication"]["data_hash"].As<uint64_t>();
			state.nsVersion.FromJSON(root["replication"]["ns_version"]);
			state.updateUnixNano = root["replication"]["updated_unix_nano"].As<uint64_t>();
			try {
				reindexer::ClusterOperationStatus clStatus;
				clStatus.FromJSON(root["replication"]["clusterization_status"]);
				state.role = clStatus.role;
			} catch (...) {
				EXPECT_TRUE(false) << "Unable to parse cluster status: " << ser.Slice();
			}
			state.tmVersion = root["tags_matcher"]["version"].As<int>();
			state.tmStatetoken = root["tags_matcher"]["state_token"].As<uint32_t>();

#if 0
			std::ostringstream os;
			os << "\n"
			   << "lsn = " << int64_t(state.lsn) << std::dec << " dataCount = " << state.dataCount << " dataHash = " << state.dataHash
			   << " [" << ser.Slice() << "]\n"
			   << std::endl;
			TestCout() << os.str();
#endif
		}
	}
	return state;
}

template <typename DB>
void ReindexerTestApi<DB>::SetSchema(std::string_view ns, std::string_view schema) {
	auto err = reindexer->WithTimeout(kBasicTimeout).SetSchema(ns, schema);
	ASSERT_TRUE(err.ok()) << err.what() << "; ns: " << ns << "; schema: " << schema;
}

template <typename DB>
std::string ReindexerTestApi<DB>::GetSchema(std::string_view ns, int format) {
	std::string out;
	auto err = reindexer->WithTimeout(kBasicTimeout).GetSchema(ns, format, out);
	EXPECT_TRUE(err.ok()) << err.what() << "; ns: " << ns << "; format: " << format;
	return out;
}

template <typename DB>
reindexer::Error ReindexerTestApi<DB>::DumpIndex(std::ostream& os, std::string_view ns, std::string_view index) {
	if constexpr (std::is_same_v<DB, reindexer::Reindexer>) {
		return reindexer->WithTimeout(kBasicTimeout).DumpIndex(os, ns, index);
	} else {
		(void)os;
		(void)ns;
		(void)index;
		std::abort();
	}
}

template <typename DB>
void ReindexerTestApi<DB>::PrintQueryResults(const std::string& ns, const QueryResultsType& res) {
	if constexpr (std::is_same_v<DB, reindexer::Reindexer>) {
		if (!verbose_) {
			return;
		}
		{
			ItemType rdummy(reindexer->WithTimeout(kBasicTimeout).NewItem(ns));
			std::string outBuf;
			for (auto idx = 1; idx < rdummy.NumFields(); idx++) {
				outBuf += "\t";
				auto sv = rdummy[idx].Name();
				outBuf.append(sv.begin(), sv.end());
			}
			TestCout() << outBuf << std::endl;
		}

		for (auto it : res) {
			ItemType ritem(it.GetItem(false));
			std::string outBuf = "";
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				outBuf += "\t";
				outBuf += ritem[idx].template As<std::string>();
			}
			TestCout() << outBuf << std::endl;
		}
		TestCout() << std::endl;
	} else {
		// Not implemented;
		(void)ns;
		(void)res;
		return;
	}
}

template <typename DB>
std::string ReindexerTestApi<DB>::RandString(unsigned int minLen, unsigned int maxRandLen) {
	std::string res;
	uint8_t len = maxRandLen ? (rand() % maxRandLen + minLen) : minLen;
	res.resize(len);
	for (int i = 0; i < len; ++i) {
		int f = rand() % kLetters.size();
		res[i] = kLetters[f];
	}
	return res;
}

template <typename DB>
std::string ReindexerTestApi<DB>::RandLikePattern() {
	std::string res;
	const uint8_t len = rand() % 4 + 4;
	res.reserve(len);
	for (uint8_t i = 0; i < len;) {
		if (rand() % 3 == 0) {
			res += '%';
			const uint8_t skipLen = rand() % (len - i + 1);
			i += skipLen;
		} else {
			if (rand() % 3 == 0) {
				res += '_';
			} else {
				int f = rand() % kLetters.size();
				res += kLetters[f];
			}
			++i;
		}
	}
	return res;
}

template <typename DB>
std::string ReindexerTestApi<DB>::RuRandString() {
	std::string res;
	uint8_t len = rand() % 20 + 4;
	res.resize(len * 3);
	auto it = res.begin();
	for (int i = 0; i < len; ++i) {
		int f = rand() % kRuLetters.size();
		it = utf8::append(kRuLetters[f], it);
	}
	res.erase(it, res.end());
	return res;
}

template <typename DB>
std::vector<int> ReindexerTestApi<DB>::RandIntVector(size_t size, int start, int range) {
	std::vector<int> vec;
	vec.reserve(size);
	for (size_t i = 0; i < size; ++i) {
		vec.push_back(start + rand() % range);
	}
	return vec;
}

template <typename DB>
std::vector<std::string> ReindexerTestApi<DB>::RandStrVector(size_t size) {
	std::vector<std::string> vec;
	vec.reserve(size);
	for (size_t i = 0; i < size; ++i) {
		vec.push_back(RandString());
	}
	return vec;
}

template <typename DB>
void ReindexerTestApi<DB>::EnablePerfStats(DB& rx) {
	auto item = rx.NewItem(reindexer::kConfigNamespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(
		R"json({"type":"profiling","profiling":{"queriesperfstats":true,"queries_threshold_us":100,"perfstats":true,"memstats":true,"activitystats":true}})json");
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.Upsert(reindexer::kConfigNamespace, item);
	ASSERT_TRUE(err.ok()) << err.what();
}

template <typename DB>
std::vector<std::string> ReindexerTestApi<DB>::GetSerializedQrItems(reindexer::QueryResults& qr) {
	std::vector<std::string> items;
	items.reserve(qr.Count());
	reindexer::WrSerializer wrser;
	for (auto it : qr) {
		EXPECT_TRUE(it.Status().ok()) << it.Status().what();
		wrser.Reset();
		auto err = it.GetJSON(wrser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		items.emplace_back(wrser.Slice());
	}
	return items;
}

template <>
void ReindexerTestApi<reindexer::Reindexer>::AwaitIndexOptimization(std::string_view nsName) {
	bool optimization_completed = false;
	unsigned waitForIndexOptimizationCompleteIterations = 0;
	while (!optimization_completed) {
		ASSERT_LT(waitForIndexOptimizationCompleteIterations++, 200) << "Too long index optimization";
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		auto qr = Select(reindexer::Query(reindexer::kMemStatsNamespace).Where("name", CondEq, nsName));
		ASSERT_EQ(1, qr.Count());
		optimization_completed = qr.begin().GetItem(false)["optimization_completed"].Get<bool>();
	}
}

template class ReindexerTestApi<reindexer::Reindexer>;
template class ReindexerTestApi<reindexer::client::Reindexer>;
