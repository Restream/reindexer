#include <gtest/gtest.h>
#include <chrono>

#include <coroutine/channel.h>
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "net/ev/ev.h"
#include "tools/clock.h"

using reindexer::net::ev::dynamic_loop;
using std::chrono::duration_cast;
using std::chrono::seconds;
using std::chrono::milliseconds;
using reindexer::coroutine::channel;

template <typename T>
static void OutputVector(const std::vector<T>& vec) {
	std::cerr << "{ ";
	for (auto it = vec.begin(); it != vec.end(); ++it) {
		std::cerr << *it;
		if (it + 1 != vec.end()) {
			std::cerr << ", ";
		}
	}
	std::cerr << " }" << std::endl;
}

TEST(Coroutines, Timers) {
	// Should be able to handle multiple timers in concurrent coroutines
	constexpr auto kSleepTime = milliseconds(500);
#ifdef REINDEX_WITH_TSAN
	constexpr auto kCoroCount = 100;
#else
	constexpr auto kCoroCount = 500;
#endif
	dynamic_loop loop;
	size_t counter = 0;
	for (size_t i = 0; i < kCoroCount; ++i) {
		loop.spawn([&loop, &counter, kSleepTime] {
			std::vector<int> v = {1, 2, 3};	 // Check if destructor was called
			(void)v;
			loop.sleep(kSleepTime);
			++counter;
		});
	}
	auto beg = reindexer::system_clock_w::now();
	loop.run();
	auto diff = reindexer::system_clock_w::now() - beg;
	ASSERT_TRUE(diff > kSleepTime) << "Diff: " << duration_cast<milliseconds>(diff).count() << " milliseconds";
	ASSERT_TRUE(diff < 6 * kSleepTime) << "Diff: " << duration_cast<milliseconds>(diff).count() << " milliseconds";
	ASSERT_EQ(counter, kCoroCount);
}

TEST(Coroutines, LoopDestructor) {
	// Loop should await coroutines's completions on dectrution
	constexpr auto kSleepTime = milliseconds(100);
	constexpr auto kCoroCount = 1000;
	size_t counter = 0;
	{
		dynamic_loop loop;
		for (size_t i = 0; i < kCoroCount; ++i) {
			loop.spawn([&loop, &counter, kSleepTime] {
				std::vector<int> v = {1, 2, 3};	 // Check if destructor was called
				(void)v;
				loop.sleep(kSleepTime);
				++counter;
			});
		}
	}
	ASSERT_EQ(counter, kCoroCount);
}

TEST(Coroutines, StressTest) {
	// Any number of concurrent coroutines and channels should work properly with sanitizers
	size_t counter = 0;
	dynamic_loop loop;
	std::vector<std::unique_ptr<channel<int>>> vec;
	auto storage_size = reindexer::coroutine::shrink_storage();
	ASSERT_EQ(storage_size, 0);
	size_t finishedCoroutines = 0;
	int64_t userCallbackId =
		reindexer::coroutine::add_completion_callback([&finishedCoroutines](reindexer::coroutine::routine_t) { ++finishedCoroutines; });
	for (size_t i = 0; i < 50; ++i) {
		loop.spawn([&loop, &counter, &vec] {
			for (size_t i = 0; i < 100; ++i) {
				constexpr size_t kCnt = 5;
				auto chPtr = std::unique_ptr<channel<int>>(new channel<int>(kCnt));
				auto& ch = *chPtr;
				vec.emplace_back(std::move(chPtr));
				loop.spawn([&ch, &counter] {
					for (size_t i = 0; i < 2 * kCnt; ++i) {
						auto res = ch.pop();
						ASSERT_TRUE(res.second);
						(void)res;
					}
					++counter;
				});
				for (size_t i = 0; i < kCnt; ++i) {
					loop.spawn([&ch, &counter] {
						for (size_t i = 0; i < 2; ++i) {
							ch.push(int(i));
						}
						++counter;
					});
				}
			}
			++counter;
		});
	}
	loop.run();
	constexpr size_t kExpectedTotal = 30050;
	ASSERT_EQ(counter, kExpectedTotal);
	ASSERT_EQ(finishedCoroutines, kExpectedTotal);

	int res = reindexer::coroutine::remove_completion_callback(userCallbackId);
	ASSERT_EQ(res, 0);
	res = reindexer::coroutine::remove_completion_callback(userCallbackId);
	ASSERT_NE(res, 0);
}

TEST(Coroutines, ClosedChannelWriting) {
	// Closed channel should throw exception on write
	dynamic_loop loop;
	channel<int> ch(10);
	size_t exceptions = 0;
	loop.spawn([&ch, &exceptions] {
		for (size_t i = 0; i < ch.capacity(); ++i) {
			try {
				if (i == ch.capacity() / 2) {
					ch.close();
				}
				ch.push(int(i));
			} catch (std::exception&) {
				++exceptions;
			}
		}
	});
	loop.run();
	ASSERT_EQ(exceptions, ch.capacity() / 2);
}

TEST(Coroutines, ClosedChannelReading) {
	// Closed channel should allow to read data on read (if there are any) and return error (if there are none)
	dynamic_loop loop;
	std::vector<int> wData = {5, 3, 7, 15, 99, 22, 53, 44};
	std::vector<int> rData;
	rData.reserve(wData.size());
	channel<int> ch(wData.size());
	loop.spawn([&ch, &wData] {
		for (auto d : wData) {
			try {
				ch.push(d);
				// We will get ASAN warning on exception, but this doesn't matter
			} catch (std::exception&) {
				ASSERT_TRUE(false);
			}
		}
		ASSERT_EQ(ch.size(), wData.size());
		ch.close();
	});
	loop.spawn([&ch, &rData] {
		auto dp = ch.pop();
		while (dp.second) {
			rData.emplace_back(dp.first);
			dp = ch.pop();
		}
	});
	loop.run();
	if (wData != rData) {
		std::cerr << "Expected data is:\n{ ";
		OutputVector(wData);
		std::cerr << "Actual data is:\n{ ";
		OutputVector(rData);
		ASSERT_TRUE(false);
	}
}

TEST(Coroutines, SchedulingOrder) {
	// Coroutines should be scheduled in specified order
	using reindexer::coroutine::create;
	using reindexer::coroutine::current;
	using reindexer::coroutine::resume;
	using reindexer::coroutine::suspend;
	using reindexer::coroutine::routine_t;

	TestCout() << "Expecting unhandled exception (and non-critical ASAN warning) for coroutine \"10\" here..." << std::endl;

	auto storage_size = reindexer::coroutine::shrink_storage();
	ASSERT_EQ(storage_size, 0);
	std::vector<routine_t> order;
	const std::vector<routine_t> kExpectedOrder = {0, 0, 1, 1, 2, 1, 1, 3, 3, 4, 4,	 5,	 5, 6, 6, 7, 7, 8,	7, 6, 5, 4, 3, 8,
												   3, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2,	 7,	 2, 9, 2, 3, 7, 3,	2, 8, 9, 8, 2, 4,
												   4, 2, 5, 5, 2, 6, 6, 2, 2, 2, 10, 10, 2, 2, 2, 2, 2, 10, 2, 2, 1, 1, 0};

	auto testFn = [&order] {
		order.emplace_back(current());
		auto fn1 = [&order] {
			order.emplace_back(current());
			suspend();
			order.emplace_back(current());
			channel<int> ch(5);
			auto wrFn = [&order, &ch] {
				order.emplace_back(current());
				ch.push(1);
				order.emplace_back(current());
			};
			std::vector<routine_t> wVec;
			for (size_t i = 0; i < 5; ++i) {
				auto wId = create(wrFn);
				ASSERT_TRUE(wId > 0) << size_t(wId);
				order.emplace_back(current());
				wVec.emplace_back(wId);
			}
			auto rdFn = [&order, &ch] {
				order.emplace_back(current());
				auto dp = ch.pop();
				ASSERT_TRUE(dp.second);
				order.emplace_back(current());
			};
			std::vector<routine_t> rVec;
			for (size_t i = 0; i < 3; ++i) {
				auto rId = create(rdFn);
				ASSERT_TRUE(rId > 0) << size_t(rId);
				order.emplace_back(current());
				rVec.emplace_back(rId);
			}

			int res = resume(rVec[0]);
			ASSERT_EQ(res, 0);
			order.emplace_back(current());
			res = resume(rVec[1]);
			ASSERT_EQ(res, 0);
			order.emplace_back(current());
			for (auto wId : wVec) {
				res = resume(wId);
				ASSERT_EQ(res, 0);
				order.emplace_back(current());
			}
			res = resume(rVec[0]);
			ASSERT_TRUE(res < 0) << res;
			order.emplace_back(current());

			res = resume(rVec[1]);
			ASSERT_TRUE(res < 0) << res;
			order.emplace_back(current());

			res = resume(rVec[2]);
			ASSERT_EQ(res, 0);
			order.emplace_back(current());

			while (ch.size() < ch.capacity()) {
				ch.push(0);
				order.emplace_back(current());
			}
			auto wId = create(wrFn);
			ASSERT_TRUE(wId > 0) << size_t(wId);
			order.emplace_back(current());
			res = resume(wId);
			ASSERT_EQ(res, 0);
			order.emplace_back(current());
			ch.close();	 // We will get an unhandled exception in writing routine
			// We will also get ASAN warning on exception, but this doesn't matter
			order.emplace_back(current());
			ASSERT_EQ(ch.size(), ch.capacity());
		};
		auto coId1 = create(fn1);
		ASSERT_TRUE(coId1 > 0) << size_t(coId1);
		order.emplace_back(current());
		int res = resume(coId1);
		ASSERT_EQ(res, 0);
		order.emplace_back(current());

		auto fn2 = [&order] {
			order.emplace_back(current());
			channel<int> ch(2);
			auto coId = create([&order, &ch] {
				order.emplace_back(current());
				auto coId = create([&order, &ch] {
					order.emplace_back(current());
					auto coId = create([&order, &ch] {
						order.emplace_back(current());
						auto coId = create([&order, &ch] {
							order.emplace_back(current());
							auto coId = create([&order, &ch] {
								order.emplace_back(current());
								auto dp = ch.pop();
								ASSERT_FALSE(dp.second);
								order.emplace_back(current());
							});

							ASSERT_TRUE(coId > 0) << size_t(coId);
							order.emplace_back(current());
							int res = resume(coId);
							ASSERT_EQ(res, 0);
							order.emplace_back(current());
						});

						ASSERT_TRUE(coId > 0) << size_t(coId);
						order.emplace_back(current());
						int res = resume(coId);
						ASSERT_EQ(res, 0);
						order.emplace_back(current());
					});

					ASSERT_TRUE(coId > 0) << size_t(coId);
					order.emplace_back(current());
					int res = resume(coId);
					ASSERT_EQ(res, 0);
					order.emplace_back(current());
				});

				ASSERT_TRUE(coId > 0) << coId;
				order.emplace_back(current());
				int res = resume(coId);
				ASSERT_EQ(res, 0);
				order.emplace_back(current());
			});

			ASSERT_TRUE(coId > 0) << size_t(coId);
			order.emplace_back(current());
			int res = resume(coId);
			ASSERT_EQ(res, 0);
			order.emplace_back(current());
			ch.close();
			order.emplace_back(current());
		};
		auto coId2 = create(fn2);
		ASSERT_TRUE(coId2 > 0) << size_t(coId2);
		order.emplace_back(current());
		res = resume(coId2);
		ASSERT_EQ(res, 0);
		order.emplace_back(current());

		res = resume(coId1);
		ASSERT_EQ(res, 0);
		order.emplace_back(current());

		res = resume(coId1);
		ASSERT_TRUE(res < 0) << res;
		order.emplace_back(current());
	};

	order.emplace_back(current());
	auto coId = create(testFn);
	ASSERT_TRUE(coId > 0) << size_t(coId);
	order.emplace_back(current());
	int res = resume(coId);
	ASSERT_EQ(res, 0);
	order.emplace_back(current());

	if (order != kExpectedOrder) {
		std::cerr << "Expected order is:\n";
		OutputVector(kExpectedOrder);
		std::cerr << "Actual order is:\n";
		OutputVector(order);
		ASSERT_TRUE(false);
	}
}

TEST(Coroutines, FIFOChannels) {
	// Check FIFO coroutines ordering for channels

	using reindexer::coroutine::create;
	using reindexer::coroutine::current;
	using reindexer::coroutine::channel;
	using reindexer::coroutine::routine_t;

	auto storage_size = reindexer::coroutine::shrink_storage();
	ASSERT_EQ(storage_size, 0);

	std::vector<routine_t> launchOrder, terminationOrder;
	std::vector<int> read;
	const std::vector<routine_t> kExpectedOrder = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
	dynamic_loop loop;
	channel<int> ch(3), chEnd;
	constexpr int kValue = -999;
	auto pushRoutine = exceptionWrapper([&] {
		launchOrder.emplace_back(current());
		ch.push(kValue);
		terminationOrder.emplace_back(current());
		std::ignore = chEnd.pop();
	});
	constexpr unsigned kPushRountinesBatch = 5;
	constexpr unsigned kIntermediateReadSize = 2;
	ASSERT_GT(ch.capacity(), kIntermediateReadSize);  // Particular read is essential test condition
	ASSERT_LT(ch.capacity(), kPushRountinesBatch);	  // Channel overflow is essential test condition

	loop.spawn(exceptionWrapper([&] {
		read.reserve(kPushRountinesBatch * 2);
		for (unsigned i = 0; i < kPushRountinesBatch; ++i) {
			loop.spawn(pushRoutine);
		}
		loop.yield();
		loop.spawn(exceptionWrapper([&] {
			EXPECT_EQ(ch.size(), ch.capacity());
			for (unsigned i = 0; i < kIntermediateReadSize; ++i) {
				auto res = ch.pop();
				EXPECT_EQ(res.first, kValue);
				EXPECT_TRUE(res.second);
				read.emplace_back(res.first);
			}
		}));
		loop.yield();
		for (unsigned i = 0; i < kPushRountinesBatch; ++i) {
			loop.spawn(pushRoutine);
		}
		loop.yield();
		EXPECT_EQ(ch.size(), ch.capacity());
		EXPECT_EQ(ch.writers(), kPushRountinesBatch * 2 - kIntermediateReadSize - ch.size());
		while (read.size() != kPushRountinesBatch * 2) {
			auto res = ch.pop();
			EXPECT_EQ(res.first, kValue);
			EXPECT_TRUE(res.second);
			read.emplace_back(res.first);
		}
		chEnd.close();
	}));

	loop.run();

	if (launchOrder != kExpectedOrder || terminationOrder != kExpectedOrder) {
		std::cerr << "Expected order (both launch and termination) is:\n";
		OutputVector(kExpectedOrder);
		std::cerr << "Actual launch order is:\n";
		OutputVector(launchOrder);
		std::cerr << "Actual termination order is:\n";
		OutputVector(launchOrder);
		ASSERT_TRUE(false);
	}
}
