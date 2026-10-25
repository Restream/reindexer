#include <gtest/gtest.h>
#include <chrono>
#include <stdexcept>

#include <coroutine/channel.h>
#include <coroutine/coroutine.h>
#include <coroutine/mutex.h>
#include <coroutine/waitgroup.h>
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "net/ev/ev.h"
#include "tools/clock.h"

namespace reindexer_tests {

using reindexer::net::ev::dynamic_loop;
using std::chrono::duration_cast;
using std::chrono::seconds;
using std::chrono::milliseconds;
using reindexer::coroutine::channel;
using reindexer::coroutine::mutex;
using reindexer::coroutine::wait_group;

// NOLINTBEGIN(rx-perf-lambda-to-std-function-allocation)

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
	auto pushRoutine = reindexer_tests_tools::exceptionWrapper([&] {
		launchOrder.emplace_back(current());
		ch.push(kValue);
		terminationOrder.emplace_back(current());
		std::ignore = chEnd.pop();
	});
	constexpr unsigned kPushRountinesBatch = 5;
	constexpr unsigned kIntermediateReadSize = 2;
	ASSERT_GT(ch.capacity(), kIntermediateReadSize);  // Particular read is essential test condition
	ASSERT_LT(ch.capacity(), kPushRountinesBatch);	  // Channel overflow is essential test condition

	loop.spawn(reindexer_tests_tools::exceptionWrapper([&] {
		read.reserve(kPushRountinesBatch * 2);
		for (unsigned i = 0; i < kPushRountinesBatch; ++i) {
			loop.spawn(pushRoutine);
		}
		loop.yield();
		loop.spawn(reindexer_tests_tools::exceptionWrapper([&] {
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

TEST(Coroutines, WaitGroupGuardHappyPath) {
	// Happy path: add(N) via spawn(wg, ...) + N children using wait_group_guard. wait() must resume exactly once after all
	// children are done, wait_count() must be 0, and the deferral path must NOT be taken (no exceptions in flight).
	dynamic_loop loop;
	constexpr size_t kCnt = 16;
	size_t counter = 0;
	size_t counterAtResume = 0;
	bool waiterResumed = false;
	loop.spawn([&] {
		wait_group wg;
		for (size_t i = 0; i < kCnt; ++i) {
			loop.spawn(wg, [&counter] { ++counter; });
		}
		EXPECT_EQ(wg.wait_count(), kCnt);
		wg.wait();
		counterAtResume = counter;
		waiterResumed = true;
		EXPECT_EQ(wg.wait_count(), 0);
	});
	loop.run();
	EXPECT_TRUE(waiterResumed);
	EXPECT_EQ(counter, kCnt);
	EXPECT_EQ(counterAtResume, kCnt);
}

TEST(Coroutines, MutexUnlockDefersResumeDuringUnwind) {
	// Check that mutex unlock defers resume during unwind

	TestCout() << "Expecting unhandled exception (and non-critical ASAN warning) for the mutex owner coroutine here..." << std::endl;
	dynamic_loop loop;
	mutex mtx;
	std::vector<int> order;
	bool waiterAcquired = false;
	loop.spawn([&] {
		std::lock_guard<mutex> guard(mtx);
		order.emplace_back(1);
		loop.spawn([&] {
			order.emplace_back(2);
			std::lock_guard<mutex> waiterGuard(mtx);
			order.emplace_back(4);
			waiterAcquired = true;
		});
		loop.yield();
		EXPECT_EQ(order, (std::vector<int>{1, 2}));
		EXPECT_FALSE(waiterAcquired);
		order.emplace_back(3);
		throw std::runtime_error("boom from mutex owner");
	});
	loop.run();
	EXPECT_TRUE(waiterAcquired);
	EXPECT_EQ(order, (std::vector<int>{1, 2, 3, 4}));
}

TEST(Coroutines, WaitGroupWaitAfterChildrenFinished) {
	// Children finish before wait() is called -> wait() must return immediately.
	dynamic_loop loop;
	constexpr size_t kCnt = 8;
	size_t counter = 0;
	bool waiterResumed = false;
	loop.spawn([&] {
		wait_group wg;
		for (size_t i = 0; i < kCnt; ++i) {
			loop.spawn(wg, [&counter] { ++counter; });
		}
		// Let all children run to completion before waiting.
		while (wg.wait_count() != 0) {
			loop.yield();
		}
		EXPECT_EQ(counter, kCnt);
		wg.wait();	// Should return immediately: wait_cnt_ is already 0.
		waiterResumed = true;
	});
	loop.run();
	EXPECT_TRUE(waiterResumed);
	EXPECT_EQ(counter, kCnt);
}

TEST(Coroutines, WaitGroupGuardLastDoneDuringUnwind) {
	// A spawned coroutine throws and is the LAST to finish while the parent is suspended in
	// wait(). ~wait_group_guard runs done() during unwinding (uncaught_exceptions() > 0), which must defer the resume
	// instead of switching fibers with an in-flight exception. Assert: no crash, suspend()'s debug assert never trips,
	// the waiter resumes after the exception is handled, and final state is correct.
	TestCout() << "Expecting unhandled exception (and non-critical ASAN warning) for the last child coroutine here..." << std::endl;
	dynamic_loop loop;
	constexpr size_t kCnt = 4;
	size_t counter = 0;
	size_t counterAtResume = 0;
	bool waiterResumed = false;
	loop.spawn([&] {
		wait_group wg;
		for (size_t i = 0; i < kCnt; ++i) {
			const bool isLast = (i + 1 == kCnt);
			loop.spawn(wg, [&counter, isLast] {
				++counter;
				if (isLast) {
					throw std::runtime_error("boom from last child");
				}
			});
		}
		wg.wait();
		counterAtResume = counter;
		waiterResumed = true;
		EXPECT_EQ(wg.wait_count(), 0);
	});
	loop.run();
	EXPECT_TRUE(waiterResumed);
	EXPECT_EQ(counter, kCnt);
	EXPECT_EQ(counterAtResume, kCnt);
}

TEST(Coroutines, WaitGroupGuardThrowOnNonLastChild) {
	// Throw on a non-last child -> no premature wakeup. The throwing child's done() during unwinding only decrements
	// wait_cnt_ (still > 0), so neither resume nor defer happens. The waiter must only wake once all children are done.
	TestCout() << "Expecting unhandled exception (and non-critical ASAN warning) for the first child coroutine here..." << std::endl;
	dynamic_loop loop;
	constexpr size_t kCnt = 4;
	size_t counter = 0;
	size_t counterAtResume = 0;
	bool waiterResumed = false;
	loop.spawn([&] {
		wait_group wg;
		for (size_t i = 0; i < kCnt; ++i) {
			const bool isFirst = (i == 0);
			loop.spawn(wg, [&counter, isFirst] {
				++counter;
				if (isFirst) {
					throw std::runtime_error("boom from first child");
				}
			});
		}
		wg.wait();
		counterAtResume = counter;
		waiterResumed = true;
		EXPECT_EQ(wg.wait_count(), 0);
	});
	loop.run();
	EXPECT_TRUE(waiterResumed);
	EXPECT_EQ(counter, kCnt);
	// The waiter must not be woken before every child has finished.
	EXPECT_EQ(counterAtResume, kCnt);
}

TEST(Coroutines, WaitGroupGuardNestedDeferReentrancy) {
	// Nested defer / re-entrancy: after the first wait_group resumes the parent (via a deferred resume triggered by a
	// throwing child), the parent spawns a second batch of children, one of which also throws, and waits again. Both
	// wait()s must complete and all children must run.
	TestCout() << "Expecting two unhandled exceptions (and non-critical ASAN warnings) for child coroutines here..." << std::endl;
	dynamic_loop loop;
	size_t counter = 0;
	bool firstWaitDone = false;
	bool secondWaitDone = false;
	loop.spawn([&] {
		{
			wait_group wg;
			constexpr size_t kCnt = 3;
			for (size_t i = 0; i < kCnt; ++i) {
				const bool isLast = (i + 1 == kCnt);
				loop.spawn(wg, [&counter, isLast] {
					++counter;
					if (isLast) {
						throw std::runtime_error("boom1");
					}
				});
			}
			wg.wait();
			firstWaitDone = true;
			EXPECT_EQ(wg.wait_count(), 0);
		}
		{
			wait_group wg2;
			constexpr size_t kCnt2 = 3;
			for (size_t i = 0; i < kCnt2; ++i) {
				const bool isLast = (i + 1 == kCnt2);
				loop.spawn(wg2, [&counter, isLast] {
					++counter;
					if (isLast) {
						throw std::runtime_error("boom2");
					}
				});
			}
			wg2.wait();
			secondWaitDone = true;
			EXPECT_EQ(wg2.wait_count(), 0);
		}
	});
	loop.run();
	EXPECT_TRUE(firstWaitDone);
	EXPECT_TRUE(secondWaitDone);
	EXPECT_EQ(counter, 6u);
}

TEST(Coroutines, DeferredResumeIntraFlushReentrancy) {
	// True intra-flush re-entrancy: a coroutine that is resumed *from inside* flush_deferred_resumes() itself enqueues a
	// new deferred resume and then suspends instead of finishing. Had it finished, its own entry() would drain the list.
	using namespace reindexer::coroutine;
	std::vector<int> order;
	routine_t idB = create([&order] { order.push_back(2); });  // B: just records when resumed, then finishes.
	routine_t idA = create([&order, &idB] {
		order.push_back(1);
		defer_resume(idB);	 // Enqueue B while A is being resumed from within the flush loop...
		suspend();			 // ...and hand control back to that loop WITHOUT finishing (so A's entry() does not drain it).
		order.push_back(3);	 // Reached only on the later, explicit resume below.
	});

	// Kick off the drain: defer A, then flush. Inside this single flush() call A runs, defers B and suspends; the same
	// flush loop must then resume B. So afterwards order == {1, 2} and A is still parked.
	defer_resume(idA);
	flush_deferred_resumes();
	EXPECT_EQ(order, (std::vector<int>{1, 2}));

	// Finish A so no coroutine is left dangling in the shared (thread_local) ordinator.
	EXPECT_EQ(resume(idA), 0);
	EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
}

TEST(Coroutines, DeferManyDistinctResumesAllInLifoOrder) {
	// Defer a large number of DISTINCT coroutines and verify flush_deferred_resumes() resumes them ALL
	// exactly once, in LIFO order, without crashing or hitting any limit. Each deferred
	// coroutine runs to completion, so no unfinalized coroutine is left dangling in the shared thread_local ordinator.
	using namespace reindexer::coroutine;
	constexpr routine_t kCnt = 64;
	std::vector<routine_t> ids;
	ids.reserve(kCnt);
	std::vector<routine_t> resumeOrder;
	resumeOrder.reserve(kCnt);

	for (routine_t i = 0; i < kCnt; ++i) {
		routine_t id = create([&resumeOrder] { resumeOrder.push_back(current()); });
		ASSERT_GT(id, 0u) << "iteration " << i;
		ids.push_back(id);
	}

	for (auto id : ids) {
		defer_resume(id);
		defer_resume(id);  // duplicate -> must be deduplicated (O(1) flag), resumed only once
	}

	flush_deferred_resumes();

	ASSERT_EQ(resumeOrder.size(), size_t(kCnt));
	// LIFO: defer pushes onto the head, flush drains the head first -> resume order is the reverse of the defer order.
	std::vector<routine_t> expected(ids.rbegin(), ids.rend());
	EXPECT_EQ(resumeOrder, expected);

	// The list must be empty now: a second flush is a no-op and leaves nothing behind.
	flush_deferred_resumes();
	EXPECT_EQ(resumeOrder.size(), size_t(kCnt));
}

#if REINDEX_WITH_DEBUG_ASSERT && !defined(NDEBUG) && !defined(REINDEX_WITH_ASAN)
TEST(CoroutinesDeathTest, SuspendDuringUnwindAsserts) {
	// Suspending while an exception is in flight (e.g. wait()/suspend() reached from a destructor during stack
	// unwinding) would migrate the in-flight exception across a fiber switch. suspend() guards against this with a debug
	// assert; verify it fires.
	GTEST_FLAG_SET(death_test_style, "threadsafe");
	EXPECT_DEATH(
		{
			dynamic_loop loop;
			loop.spawn([] {
				struct SuspendOnUnwind {
					~SuspendOnUnwind() { reindexer::coroutine::suspend(); }
				} guard;
				(void)guard;
				throw std::runtime_error("boom");
			});
			loop.run();
		},
		"Assertion failed");
}

TEST(CoroutinesDeathTest, ResumeDuringUnwindAsserts) {
	// Directly resuming another coroutine while an exception is in flight is the exact corruption this fix prevents.
	// resume() guards against it with a debug assert; verify it fires when resume() is invoked from a destructor running
	// during stack unwinding.
	GTEST_FLAG_SET(death_test_style, "threadsafe");
	using namespace reindexer::coroutine;
	EXPECT_DEATH(
		{
			routine_t victim = create([] { suspend(); });  // Park a coroutine so it is a valid resume target.
			resume(victim);
			struct ResumeOnUnwind {
				routine_t id;
				~ResumeOnUnwind() { resume(id); }
			} guard{victim};
			(void)guard;
			throw std::runtime_error("boom");
		},
		"Assertion failed");
}
#endif	// REINDEX_WITH_DEBUG_ASSERT && !defined(NDEBUG) && !defined(REINDEX_WITH_ASAN)

// NOLINTEND(rx-perf-lambda-to-std-function-allocation)

}  // namespace reindexer_tests
