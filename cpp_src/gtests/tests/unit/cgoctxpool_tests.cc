#include "cgoctxpool_api.h"
#include "estl/condition_variable.h"
#include "estl/lock.h"
#include "estl/mutex.h"

namespace CGOCtxPoolTests {

using reindexer::CancelType;

static const size_t kCtxPoolSize = 4096;
static const size_t kCtxPoolTestsRepeatCount = 40;

TEST_F(CGOCtxPoolApi, SingleThread) {
	static const size_t kFirstIterCount = kCtxPoolSize + 1;
	static const size_t kSecondIterCount = kCtxPoolSize * 3 / 2 + 1;
	static const size_t kThirdIterCount = kCtxPoolSize * 5 / 2 + 1;

	auto pool = createCtxPool(kCtxPoolSize);
	std::vector<IRdxCancelContext*> ctxPtrs(kCtxPoolSize);

	EXPECT_TRUE(getAndValidateCtx(0, *pool) == nullptr);

	for (uint64_t i = 1; i < kFirstIterCount; ++i) {
		ctxPtrs[i - 1] = getAndValidateCtx(i, *pool);
		ASSERT_TRUE(ctxPtrs[i - 1] != nullptr);
	}

	// Trying to get the same contexts
	for (uint64_t i = 1; i < kFirstIterCount; ++i) {
		ASSERT_TRUE(getAndValidateCtx(i, *pool) == nullptr);
	}

	// Get few more
	for (uint64_t i = kFirstIterCount; i < kSecondIterCount; ++i) {
		ASSERT_TRUE(getAndValidateCtx(i, *pool) != nullptr);
	}

	auto& contexts = pool->contexts();
	ASSERT_EQ(contexts.size(), kCtxPoolSize);
	for (size_t i = 0; i < kCtxPoolSize; ++i) {
		EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].ctxID));
		EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].ctxID));
		EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].ctxID));
		if (i < kCtxPoolSize / 2 + 1 && i != 0) {
			ASSERT_TRUE(contexts[i].next != nullptr);
			EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->ctxID));
			ASSERT_TRUE(contexts[i].next->next == nullptr);
		} else {
			ASSERT_TRUE(contexts[i].next == nullptr);
		}
	}

	// Cancel some of the ctx
	EXPECT_FALSE(pool->cancelContext(0, CancelType::Explicit));
	for (uint64_t i = 1; i < kCtxPoolSize / 2 + 1; ++i) {
		ASSERT_TRUE(pool->cancelContext(i, CancelType::Explicit));
		EXPECT_EQ(ctxPtrs[i - 1]->GetCancelType(), CancelType::Explicit);
	}

	auto validateAfterCancel = [&]() {
		for (size_t i = 0; i < kCtxPoolSize; ++i) {
			if (i < kCtxPoolSize / 2 + 1 && i != 0) {
				EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].ctxID));
				EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].ctxID));

				ASSERT_TRUE(contexts[i].next != nullptr);
				EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->ctxID));
				ASSERT_TRUE(contexts[i].next->next == nullptr);
			} else {
				EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].ctxID));
				ASSERT_TRUE(contexts[i].next == nullptr);
			}
		}
	};
	validateAfterCancel();

	// Cancel the same contexts
	for (uint64_t i = 1; i < kCtxPoolSize / 2 + 1; ++i) {
		ASSERT_TRUE(pool->cancelContext(i, CancelType::Explicit));
	}
	validateAfterCancel();

	// Get even more contexts
	for (uint64_t i = kSecondIterCount; i < kThirdIterCount; ++i) {
		ASSERT_TRUE(getAndValidateCtx(i, *pool) != nullptr);
	}

	for (size_t i = 0; i < kCtxPoolSize; ++i) {
		EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].ctxID));
		EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].ctxID));
		ASSERT_TRUE(contexts[i].next != nullptr);
		EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->ctxID));
		EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->ctxID));
		EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->ctxID));
		if (i < kCtxPoolSize / 2 + 1 && i != 0) {
			EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].ctxID));
			ASSERT_TRUE(contexts[i].next->next != nullptr);
			EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->next->ctxID));
			ASSERT_TRUE(contexts[i].next->next->next == nullptr);
		} else {
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].ctxID));
			ASSERT_TRUE(contexts[i].next->next == nullptr);
		}
	}

	// Remove some of the contexts
	EXPECT_FALSE(pool->removeContext(0));
	for (uint64_t i = 1; i < kFirstIterCount; ++i) {
		ASSERT_TRUE(pool->removeContext(i));
	}

	auto validateAfterRemove = [&]() {
		for (size_t i = 0; i < kCtxPoolSize; ++i) {
			EXPECT_EQ(contexts[i].ctxID, 0);
			ASSERT_TRUE(contexts[i].next != nullptr);
			EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->ctxID));
			EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->ctxID));
			if (i < kCtxPoolSize / 2 + 1 && i != 0) {
				ASSERT_TRUE(contexts[i].next->next != nullptr);
				EXPECT_TRUE(ContextsPoolImpl<CancelContextImpl>::isInitialized(contexts[i].next->next->ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceling(contexts[i].next->next->ctxID));
				EXPECT_FALSE(ContextsPoolImpl<CancelContextImpl>::isCanceled(contexts[i].next->next->ctxID));
				ASSERT_TRUE(contexts[i].next->next->next == nullptr);
			} else {
				ASSERT_TRUE(contexts[i].next->next == nullptr);
			}
		}
	};
	validateAfterRemove();

	// Remove the same of the contexts
	for (uint64_t i = 1; i < kFirstIterCount; ++i) {
		ASSERT_FALSE(pool->removeContext(i));
	}
	validateAfterRemove();

	// Remove the rest of the contexts
	for (uint64_t i = kFirstIterCount; i < kThirdIterCount; ++i) {
		ASSERT_TRUE(pool->removeContext(i));
	}
	for (size_t i = 0; i < kCtxPoolSize; ++i) {
		EXPECT_EQ(contexts[i].ctxID, 0);
		ASSERT_TRUE(contexts[i].next != nullptr);
		EXPECT_EQ(contexts[i].next->ctxID, 0);
		if (i < kCtxPoolSize / 2 + 1 && i != 0) {
			ASSERT_TRUE(contexts[i].next->next != nullptr);
			EXPECT_EQ(contexts[i].next->next->ctxID, 0);
			ASSERT_TRUE(contexts[i].next->next->next == nullptr);
		} else {
			ASSERT_TRUE(contexts[i].next->next == nullptr);
		}
	}
}

void CGOCtxPoolApi::multiThreadTest(size_t threadsCount, MultiThreadTestMode mode) {
	auto pool = createCtxPool(kCtxPoolSize);

	std::vector<std::thread> threads;
	threads.reserve(threadsCount);
	reindexer::condition_variable cond;
	reindexer::mutex mtx;
	std::atomic<uint32_t> awaitCount{0};
	for (size_t i = 0; i < threadsCount; ++i) {
		threads.emplace_back(std::thread([this, &pool, &cond, &mtx, &awaitCount, mode]() {
			static const size_t kCtxCount = kCtxPoolSize * 2;
			std::vector<IRdxCancelContext*> ctxPtrs(kCtxCount);
			reindexer::unique_lock lck(mtx);
			awaitCount.fetch_add(1, std::memory_order_relaxed);
			cond.wait(lck);
			lck.unlock();
			for (uint64_t i = 0; i < kCtxCount; ++i) {
				ctxPtrs[i] = getAndValidateCtx(i + 1, *pool);
			}

			if (mode == MultiThreadTestMode::Synced) {
				awaitCount.fetch_add(-1, std::memory_order_acq_rel);
				while (awaitCount.load(std::memory_order_acquire) > 0) {
					std::this_thread::yield();
				}
			}

			for (uint64_t i = 0; i < kCtxCount; ++i) {
				if (ctxPtrs[i] && i % 2 == 0) {
					EXPECT_TRUE(pool->cancelContext(i + 1, CancelType::Explicit));
					if (mode == MultiThreadTestMode::Synced) {
						EXPECT_EQ(ctxPtrs[i]->GetCancelType(), CancelType::Explicit);
					}
				}
			}
			for (uint64_t i = 0; i < kCtxCount; ++i) {
				if (ctxPtrs[i]) {
					EXPECT_TRUE(pool->removeContext(i + 1));
				}
			}
		}));
	}

	while (awaitCount.load(std::memory_order_relaxed) < threadsCount) {
		std::this_thread::yield();
	}
	reindexer::unique_lock lck(mtx);
	cond.notify_all();
	lck.unlock();
	for (auto& thread : threads) {
		thread.join();
	}

	auto& contexts = pool->contexts();
	for (size_t i = 0; i < kCtxPoolSize; ++i) {
		auto node = &contexts[i];
		ASSERT_TRUE(node->next != nullptr);
		do {
			EXPECT_EQ(node->ctxID, 0);
			node = node->next;
		} while (node);
	}
}

TEST_F(CGOCtxPoolApi, MultiThread) {
	const size_t kThreadsCount = 16;
	for (size_t testNum = 0; testNum < kCtxPoolTestsRepeatCount; ++testNum) {
		multiThreadTest(kThreadsCount, MultiThreadTestMode::Simple);
	}
}

TEST_F(CGOCtxPoolApi, MultiThreadSynced) {
	const size_t kThreadsCount = 16;
	for (size_t testNum = 0; testNum < kCtxPoolTestsRepeatCount; ++testNum) {
		multiThreadTest(kThreadsCount, MultiThreadTestMode::Synced);
	}
}

TEST_F(CGOCtxPoolApi, ConcurrentCancel) {
	static const size_t kGetThreadsCount = 8;
	static const size_t kCancelThreadsCount = 8;

	auto pool = createCtxPool(kCtxPoolSize);

	for (size_t testNum = 0; testNum < kCtxPoolTestsRepeatCount; ++testNum) {
		std::vector<std::thread> threads;
		threads.reserve(kGetThreadsCount + kCancelThreadsCount);
		reindexer::condition_variable cond;
		reindexer::mutex mtx;
		std::atomic<uint32_t> awaitCount{0};
		for (size_t i = 0; i < kGetThreadsCount; ++i) {
			threads.emplace_back(std::thread([i, &pool, &cond, &mtx, &awaitCount]() {
				size_t threadID = i;
				reindexer::unique_lock lck(mtx);
				awaitCount.fetch_add(1, std::memory_order_relaxed);
				cond.wait(lck);
				lck.unlock();
				static const size_t kCtxCount = 2 * kCtxPoolSize / kGetThreadsCount;
				std::vector<IRdxCancelContext*> ctxPtrs(kCtxCount);
				for (uint64_t i = kCtxCount * threadID, j = 0; i < kCtxCount * (threadID + 1); ++i, ++j) {
					ctxPtrs[j] = pool->getContext(i + 1);
					ASSERT_TRUE(ctxPtrs[j] != nullptr);
				}

				for (uint64_t i = kCtxCount * threadID, j = 0; i < kCtxCount * (threadID + 1); ++i, ++j) {
					while (ctxPtrs[j]->GetCancelType() == CancelType::None) {
						std::this_thread::yield();
					}
					EXPECT_TRUE(pool->removeContext(i + 1));
				}
			}));
		}

		for (size_t i = 0; i < kCancelThreadsCount; ++i) {
			threads.emplace_back(std::thread([i, &pool, &cond, &mtx, &awaitCount]() {
				size_t threadID = i;
				reindexer::unique_lock lck(mtx);
				awaitCount.fetch_add(1, std::memory_order_acq_rel);
				cond.wait(lck);
				lck.unlock();
				static const size_t kCtxCount = 2 * kCtxPoolSize / kCancelThreadsCount;
				for (uint64_t i = kCtxCount * threadID; i < kCtxCount * (threadID + 1); ++i) {
					while (!pool->cancelContext(i + 1, CancelType::Explicit)) {
						std::this_thread::yield();
					}
				}
			}));
		}

		while (awaitCount.load(std::memory_order_acquire) < kCancelThreadsCount + kGetThreadsCount) {
			std::this_thread::yield();
		}
		reindexer::unique_lock lck(mtx);
		cond.notify_all();
		lck.unlock();
		for (auto& thread : threads) {
			thread.join();
		}

		auto& contexts = pool->contexts();
		for (size_t i = 0; i < kCtxPoolSize; ++i) {
			auto node = &contexts[i];
			do {
				EXPECT_EQ(node->ctxID, 0);
				node = node->next;
			} while (node);
		}
	}
}

// Just for tsan check
TEST_F(CGOCtxPoolApi, GeneralConcurrencyCheck) {
	static const size_t kGetThreadsCount = 8;
	static const size_t kRemoveThreadsCount = 8;
	static const size_t kCancelThreadsCount = 8;

	for (size_t testNum = 0; testNum < kCtxPoolTestsRepeatCount; ++testNum) {
		auto pool = createCtxPool(kCtxPoolSize);

		std::vector<std::thread> threads;
		threads.reserve(kGetThreadsCount + kCancelThreadsCount + kRemoveThreadsCount);
		reindexer::condition_variable cond;
		reindexer::mutex mtx;
		std::atomic<uint32_t> awaitCount{0};
		for (size_t i = 0; i < kGetThreadsCount; ++i) {
			threads.emplace_back(std::thread([&pool, &cond, &mtx, &awaitCount]() {
				reindexer::unique_lock lck(mtx);
				awaitCount.fetch_add(1, std::memory_order_relaxed);
				cond.wait(lck);
				lck.unlock();
				static const size_t kCtxCount = 2 * kCtxPoolSize;
				for (uint64_t i = 1; i <= kCtxCount; ++i) {
					std::ignore = pool->getContext(i);
				}
			}));
		}

		for (size_t i = 0; i < kCancelThreadsCount; ++i) {
			threads.emplace_back(std::thread([&pool, &cond, &mtx, &awaitCount]() {
				reindexer::unique_lock lck(mtx);
				awaitCount.fetch_add(1, std::memory_order_relaxed);
				cond.wait(lck);
				lck.unlock();
				static const size_t kCtxCount = 2 * kCtxPoolSize;
				for (uint64_t i = 1; i <= kCtxCount; ++i) {
					std::ignore = pool->cancelContext(i, CancelType::Explicit);
				}
			}));
		}

		for (size_t i = 0; i < kRemoveThreadsCount; ++i) {
			threads.emplace_back(std::thread([&pool, &cond, &mtx, &awaitCount]() {
				reindexer::unique_lock lck(mtx);
				awaitCount.fetch_add(1, std::memory_order_relaxed);
				cond.wait(lck);
				lck.unlock();
				static const size_t kCtxCount = 2 * kCtxPoolSize;
				for (uint64_t i = 1; i <= kCtxCount; ++i) {
					std::ignore = pool->removeContext(i);
				}
			}));
		}

		while (awaitCount.load(std::memory_order_acquire) < kRemoveThreadsCount + kCancelThreadsCount + kGetThreadsCount) {
			std::this_thread::yield();
		}
		reindexer::unique_lock lck(mtx);
		cond.notify_all();
		lck.unlock();
		for (auto& thread : threads) {
			thread.join();
		}
	}
}

}  // namespace CGOCtxPoolTests
