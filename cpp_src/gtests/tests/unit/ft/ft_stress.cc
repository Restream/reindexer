#include <gtest/gtest-param-test.h>
#include <thread>
#include "core/system_ns_names.h"
#include "estl/condition_variable.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "ft_api.h"
#include "tools/fsops.h"

using namespace std::string_view_literals;

class [[nodiscard]] FTStressApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_stress_default_namespace"; }
};

TEST_P(FTStressApi, BasicStress) {
	const std::string kStorage = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_FTApi/BasicStress");
	std::ignore = reindexer::fs::RmDirAll(kStorage);
	Init(GetDefaultConfig(), NS1, kStorage);

	std::vector<std::string> data;
	std::vector<std::string> phrase;

	data.reserve(100000);
	for (size_t i = 0; i < 100000; ++i) {
		data.push_back(rt.RandString());
	}

	phrase.reserve(7000);
	for (size_t i = 0; i < 7000; ++i) {
		phrase.push_back(data[rand() % data.size()] + "  " + data[rand() % data.size()] + " " + data[rand() % data.size()]);
	}

	std::atomic<bool> terminate = false;
	std::thread statsThread([&] {
		while (!terminate) {
			std::ignore = rt.Select(reindexer::Query(reindexer::kMemStatsNamespace));
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	});

	for (size_t i = 0; i < phrase.size(); i++) {
		Add(phrase[i], phrase[rand() % phrase.size()]);
		if (i % 500 == 0) {
			for (size_t j = 0; j < i; j++) {
				auto res = StressSelect(phrase[j]);
				bool found = false;
				if (!res.Count()) {
					abort();
				}

				for (auto it : res) {
					auto ritem(it.GetItem(false));
					if (ritem["ft1"].As<std::string>() == phrase[j]) {
						found = true;
					}
				}
				if (!found) {
					abort();
				}
			}
		}
	}
	terminate = true;
	statsThread.join();
}

TEST_P(FTStressApi, ConcurrencyCheck) {
	const std::string kStorage = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_FTApi/ConcurrencyCheck");
	std::ignore = reindexer::fs::RmDirAll(kStorage);
	Init(GetDefaultConfig(), NS1, kStorage);

	Add("Her nose was very very long"sv);
	Add("Her nose was exceptionally long"sv);
	Add("Her nose was long"sv);

	rt.reindexer.reset();
	Init(GetDefaultConfig(), NS1, kStorage);  // Restart rx to drop all the caches

	reindexer::condition_variable cv;
	reindexer::mutex mtx;
	bool ready = false;
	std::vector<std::thread> threads;
	std::atomic<unsigned> runningThreads = {0};
	constexpr unsigned kTotalThreads = 11;
	std::thread statsThread;
	std::atomic<bool> terminate = false;
	for (unsigned i = 0; i < kTotalThreads; ++i) {
		if (i == 0) {
			statsThread = std::thread([&] {
				reindexer::unique_lock lck(mtx);
				++runningThreads;
				cv.wait(lck, [&] { return ready; });
				lck.unlock();
				while (!terminate) {
					std::ignore = rt.Select(reindexer::Query(reindexer::kMemStatsNamespace));
				}
			});
		} else {
			threads.emplace_back(std::thread([&] {
				reindexer::unique_lock lck(mtx);
				++runningThreads;
				cv.wait(lck, [&] { return ready; });
				lck.unlock();
				CheckResults("'nose long'~3", {{"Her !nose was long!", ""}, {"Her !nose was exceptionally long!", ""}}, true);
			}));
		}
	}
	while (runningThreads.load() < kTotalThreads) {
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	{
		reindexer::lock_guard lck(mtx);
		ready = true;
		cv.notify_all();
	}
	for (auto& th : threads) {
		th.join();
	}
	terminate = true;
	statsThread.join();
}

TEST_P(FTStressApi, LargeMergeLimit) {
	// Check if results are bounded by merge limit
	auto ftCfg = GetDefaultConfig();
#if RX_WITH_STDLIB_DEBUG
	if (ftCfg.optimization != reindexer::FtFastConfig::Optimization::CPU) {
		GTEST_SKIP_("Test is too large. Do not run in twice in debug build");
	}
#endif	// RX_WITH_STDLIB_DEBUG
	ftCfg.mergeLimit = 100'000;
	Init(ftCfg);
	const std::string kBase1 = "aaaa";
	const std::string kBase2 = "bbbb";

	reindexer::fast_hash_set<std::string> strings1;

	constexpr unsigned kPartLen = 160000;
	for (unsigned i = 0; i < kPartLen; ++i) {
		while (true) {
			std::string val = kBase2 + rt.RandString(10, 10);
			if (strings1.emplace(val).second) {
				Add("nm1"sv, val);
				break;
			}
		}
	}
	reindexer::fast_hash_set<std::string> strings2;
	auto fit = strings1.begin();
	for (unsigned i = 0; i < kPartLen; ++i, ++fit) {
		while (true) {
			std::string val = kBase2 + rt.RandString(10, 10);
			if (strings2.emplace(val).second) {
				if (fit == strings1.end()) {
					fit = strings1.begin();
				}
				Add("nm1"sv, val, fit.key());
				break;
			}
		}
	}
	{
		auto qr = SimpleSelect(fmt::format("{}* {}*", kBase1, kBase2));
		ASSERT_EQ(qr.Count(), ftCfg.mergeLimit);
	}
	ftCfg.mergeLimit = 60'000;
	SetFTConfig(ftCfg);
	{
		auto qr = SimpleSelect(fmt::format("{}* {}*", kBase1, kBase2));
		ASSERT_EQ(qr.Count(), ftCfg.mergeLimit);
	}
}

INSTANTIATE_TEST_SUITE_P(, FTStressApi,
						 ::testing::Values(reindexer::FtFastConfig::Optimization::Memory, reindexer::FtFastConfig::Optimization::CPU),
						 [](const auto& info) {
							 switch (info.param) {
								 case reindexer::FtFastConfig::Optimization::Memory:
									 return "OptimizationByMemory";
								 case reindexer::FtFastConfig::Optimization::CPU:
									 return "OptimizationByCPU";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });
