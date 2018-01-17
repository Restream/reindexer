#include <ftw.h>
#include <unistd.h>
#include <limits>
#include <type_traits>

#include "benchmark/benchmark.h"
#include "reindexer_fixture.h"

#include "debug/allocdebug.h"
#include "debug/backtrace.h"

using std::unique_ptr;

using benchmark::State;

using reindexer::KeyValue;
using reindexer::QueryResults;

shared_ptr<Reindexer> DB = make_shared<Reindexer>();

namespace aux {
string keyRefs2string(KeyRefs& pkgs) {
	string result;
	auto it = pkgs.begin();
	while (it != pkgs.end()) {
		KeyValue kv = *it;
		result += std::to_string(kv.toInt());
		result += (it != pkgs.end() - 1) ? "," : "";
		it++;
	}
	return result;
}

template <typename T>
typename std::enable_if<std::is_integral<T>::value, string>::type printRange(T min, T max) {
	string result;
	for (T i = min; i <= max; i++) {
		result += std::to_string(i);
		result += i < max ? "," : "";
	}
	return result;
}

template <typename... Args>
string string_format(const std::string& format, Args... args) {
	size_t size = snprintf(nullptr, 0, format.c_str(), args...) + 1;
	unique_ptr<char[]> buf(new char[size]);
	snprintf(buf.get(), size, format.c_str(), args...);
	return string(buf.get(), buf.get() + size - 1);
}

vector<KeyRef> keyArray2vector(KeyRefs& krs) {
	vector<KeyRef> result;
	for (auto const& v : krs) result.emplace_back(v);

	return result;
}
}  // namespace aux

struct AllocsTracker {
	AllocsTracker(State& state) : total_sz(get_alloc_size_total()), total_cnt(get_alloc_cnt_total()), state(state) {
		if (!inited) {
			allocdebug_init();
			backtrace_init();
		}
		inited = true;
	}
	~AllocsTracker() {
		state.counters.insert({{"Bytes/Op", (get_alloc_size_total() - total_sz) / state.iterations()},
							   {"Allocs/Op", (get_alloc_cnt_total() - total_cnt) / state.iterations()}});
	}

protected:
	size_t total_sz, total_cnt;
	State& state;
	static bool inited;
};
bool AllocsTracker::inited = false;

BENCHMARK_F(Rndxr, Prepare)(State& state) {
	Error err;

	AllocsTracker allocsTracker(state);
	while (state.KeepRunning()) {
		if ((err = FillTestItemsBench(0, 500000, 10))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		if ((err = FillTestJoinItem(7000, 500))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		state.PauseTiming();
		string query = aux::string_format("select * from %s where %s = %s order by %s", defaultNamespace.c_str(), "year", "1", "year");
		state.ResumeTiming();

		{
			QueryResults qres;
			if ((err = GetDB()->Select(query, qres))) {
				state.SkipWithError(err.what().c_str());
				return;
			};
		}

		for (size_t i = 0; i < pkgs_.size() * 3; i++) {
			state.PauseTiming();
			string query1 = aux::string_format("select * from %s where packages in (%s) order by start_time limit 20",
											   defaultNamespace.c_str(), aux::keyRefs2string(pkgs_[i % pkgs_.size()]).c_str());
			string query2 = aux::string_format("select * from %s where packages in (%s) order by year limit 20", defaultNamespace.c_str(),
											   aux::keyRefs2string(pkgs_[i % pkgs_.size()]).c_str());
			string query3 = aux::string_format("select * from %s where year in (%s) limit 20", defaultNamespace.c_str(),
											   aux::printRange(2010, 2016).c_str());
			state.ResumeTiming();

			{
				QueryResults qres;
				if ((err = GetDB()->Select(query1, qres))) {
					state.SkipWithError(err.what().c_str());
					return;
				}
			}

			{
				QueryResults qres;
				if ((err = GetDB()->Select(query2, qres))) {
					state.SkipWithError(err.what().c_str());
					return;
				}
			}

			{
				QueryResults qres;
				if ((err = GetDB()->Select(query3, qres))) {
					state.SkipWithError(err.what().c_str());
					return;
				}
			}
		}

		for (size_t i = 0; i < priceIDs_.size() * 3; i++) {
			state.PauseTiming();
			query = aux::string_format("select * from %s where id in (%s) limit 20", defaultJoinNamespace.c_str(),
									   aux::keyRefs2string(priceIDs_[i % priceIDs_.size()]).c_str());
			state.ResumeTiming();

			{
				QueryResults qres;
				if ((err = GetDB()->Select(query, qres))) {
					state.SkipWithError(err.what().c_str());
					return;
				}
			}
		}
	}
}

constexpr inline int mkID(int i) { return i * 17 + 8000000; }

BENCHMARK_F(Rndxr, GetByID)(State& state) {
	AllocsTracker allocsTracker(state);
	while (state.KeepRunning()) {
		auto q = reindexer::Query(defaultNamespace).Where("id", CondEq, mkID(rand() % 500000)).Limit(1);
		reindexer::QueryResults res;

		auto err = GetDB()->Select(q, res);
		if (!err.ok() || !res.size()) {
			printf("!!%s\n", err.what().c_str());
			abort();
		}
		res.GetItem(0);
	}
}

BENCHMARK_F(Rndxr, Query1Cond)(State& state) {
	AllocsTracker allocsTracker(state);
	while (state.KeepRunning()) {
		auto q = reindexer::Query(defaultNamespace).Where("year", CondGt, 2020).Limit(20);
		reindexer::QueryResults res;
		auto err = GetDB()->Select(q, res);
	}
}

BENCHMARK_F(Rndxr, SimpleInsert)(State& state) {
	Error err;
	ItemPtr item;

	AllocsTracker allocsTracker(state);

	while (state.KeepRunning()) {
		item.reset();

		if ((err = newTestSimpleItem(static_cast<int>(state.iterations()), item))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		err = GetDB()->Insert(defaultSimpleNamespace, item.get());
		//		err = err ? err : item->Status();

		if (err) {
			state.SkipWithError(err.what().c_str());
			return;
		}
	}
}

BENCHMARK_F(Rndxr, SimpleCmplxPKUpsert)(State& state) {
	Error err;
	ItemPtr item;

	AllocsTracker allocsTracker(state);

	while (state.KeepRunning()) {
		item.reset();

		if ((err = newTestSimpleCmplxPKItem(static_cast<int>(state.iterations()), item))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		err = GetDB()->Upsert(defaultSimpleCmplxPKNamespace, item.get());
		err = err ? err : item->Status();

		if (err) {
			state.SkipWithError(err.what().c_str());
			return;
		}
	}
}

BENCHMARK_F(Rndxr, SimpleUpdate)(State& state) {
	Error err;
	ItemPtr item;

	AllocsTracker allocsTracker(state);

	while (state.KeepRunning()) {
		item.reset();

		if ((err = newTestSimpleItem(static_cast<int>(state.iterations()), item))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		err = GetDB()->Update(defaultSimpleNamespace, item.get());
		err = err ? err : item->Status();

		if (err) {
			state.SkipWithError(err.what().c_str());
			return;
		}
	}
}

BENCHMARK_F(Rndxr, Insert)(State& state) {
	Error err;
	ItemPtr item;

	AllocsTracker allocsTracker(state);

	while (state.KeepRunning()) {
		item.reset();

		if ((err = newTestInsertItem(static_cast<int>(state.iterations()), item))) {
			state.SkipWithError(err.what().c_str());
			return;
		}

		err = GetDB()->Insert(defaultInsertNamespace, item.get());
		//		err = err ? err : item->Status();

		if (err) {
			state.SkipWithError(err.what().c_str());
			return;
		}
	}
}

BENCHMARK_F(Rndxr, Query4Cond)(State& state) {
	AllocsTracker allocsTracker(state);
	while (state.KeepRunning()) {
		auto q = reindexer::Query(defaultInsertNamespace)
					 .Where("genre", CondGt, 5)
					 .Where("age", CondEq, "2")
					 .Where("year", CondRange, {2010, 2016})
					 .Where("packages", CondSet, aux::keyArray2vector(pkgs_[static_cast<size_t>(rand()) % pkgs_.size()]))
					 .Limit(20);
		reindexer::QueryResults res;
		auto err = GetDB()->Select(q, res);
	}
}

// BENCHMARK_REGISTER_F(Rndxr, GetByID);
// BENCHMARK_REGISTER_F(Rndxr, Query1Cond);
// BENCHMARK_REGISTER_F(Rndxr, SimpleInsert);
// BENCHMARK_REGISTER_F(Rndxr, SimpleUpdate);
// BENCHMARK_REGISTER_F(Rndxr, SimpleCmplxPKUpsert)->Iterations(100000);

BENCHMARK_MAIN();
