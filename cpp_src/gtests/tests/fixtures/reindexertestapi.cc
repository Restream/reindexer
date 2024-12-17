#include "reindexertestapi.h"
#include "core/cjson/tagsmatcher.h"
#include "gtests/tests/gtest_cout.h"
#include "utf8cpp/utf8/checked.h"
#include "vendor/gason/gason.h"

static constexpr auto kBasicTimeout = std::chrono::seconds(200);

template <typename DB>
ReindexerTestApi<DB>::ReindexerTestApi() : reindexer(std::make_shared<DB>()) {}

template <typename DB>
ReindexerTestApi<DB>::ReindexerTestApi(const typename DB::ConfigT& cfg) : reindexer(std::make_shared<DB>(cfg)) {}

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
void ReindexerTestApi<DB>::Upsert(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Upsert(ns, item);
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
void ReindexerTestApi<DB>::Select(const reindexer::Query& q, QueryResultsType& qr) {
	auto err = reindexer->WithTimeout(kBasicTimeout).Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what() << "; " << q.GetSQL();
}

template <typename DB>
typename ReindexerTestApi<DB>::QueryResultsType ReindexerTestApi<DB>::Select(const reindexer::Query& q) {
	QueryResultsType qr;
	Select(q, qr);
	return qr;
}

template <typename DB>
void ReindexerTestApi<DB>::Delete(std::string_view ns, ItemType& item) {
	assertrx(!!item);
	auto err = reindexer->WithTimeout(kBasicTimeout).Delete(ns, item);
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
ReplicationTestState ReindexerTestApi<DB>::GetReplicationState(std::string_view ns) {
	using namespace reindexer;
	ReplicationTestState state;
	{
		Query qr = Query("#memstats").Where("name", CondEq, ns);
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
				reindexer::ClusterizationStatus clStatus;
				clStatus.FromJSON(root["replication"]["clusterization_status"]);
				state.role = clStatus.role;
			} catch (...) {
				EXPECT_TRUE(false) << "Unable to parse cluster status: " << ser.Slice();
			}

			/*		TestCout() << "\n"
						  << std::hex << "lsn = " << int64_t(state.lsn) << std::dec << " dataCount = " << state.dataCount
						  << " dataHash = " << state.dataHash << " [" << ser.c_str() << "]\n"
						  << std::endl;
			*/
		}
	}
	{
		Query qr = Query(ns).Limit(0);
		QueryResultsType res;
		auto err = reindexer->WithTimeout(kBasicTimeout).Select(qr, res);
		if (err.ok()) {
			auto tm = res.GetTagsMatcher(0);
			state.tmVersion = tm.version();
			state.tmStatetoken = tm.stateToken();
		}
	}
	return state;
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
		if (!verbose) {
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
		int f = rand() % letters.size();
		res[i] = letters[f];
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
				int f = rand() % letters.size();
				res += letters[f];
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
		int f = rand() % ru_letters.size();
		it = utf8::append(ru_letters[f], it);
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

template class ReindexerTestApi<reindexer::Reindexer>;
template class ReindexerTestApi<reindexer::client::Reindexer>;
