#include "itemsloader.h"
#include "core/index/index.h"
#include "tools/logger.h"

namespace reindexer {

ItemsLoader::LoadData ItemsLoader::Load() {
	logPrintf(LogTrace, "Loading items to '%s' from storage", ns_.name_);

	std::thread readingTh = std::thread([this] {
		try {
			reading();
		} catch (...) {
			std::lock_guard lck(mtx_);
			loadingData_.ex = std::current_exception();
			terminated_ = true;
			cv_.notify_all();
		}
	});
	std::thread insertionTh = std::thread([this] {
		try {
			insertion();
		} catch (...) {
			std::lock_guard lck(mtx_);
			loadingData_.ex = std::current_exception();
			terminated_ = true;
			cv_.notify_all();
		}
	});
	readingTh.join();
	insertionTh.join();

	clearIndexCache();
	if (loadingData_.ex) {
		std::rethrow_exception(loadingData_.ex);
	}
	return loadingData_;
}

void ItemsLoader::reading() {
	StorageOpts opts;
	opts.FillCache(false);
	size_t ldcount = 0;
	unsigned errCount = 0;
	Error lastErr;
	int64_t maxLSN = -1;
	int64_t minLSN = std::numeric_limits<int64_t>::max();
	const bool nsIsSystem = ns_.isSystem();
	auto dbIter = ns_.storage_.GetCursor(opts);
	unsigned sliceId = 0;
	for (dbIter->Seek(kRxStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kRxStorageItemPrefix "\xFF\xFF\xFF\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			if (!ns_.pkFields().size()) {
				throw Error(errLogic, "Can't load data storage of '%s' - there are no PK fields in ns", ns_.name_);
			}
			if (dataSlice.size() < sizeof(int64_t)) {
				lastErr = Error(errParseBin, "Not enougth data in data slice");
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", ns_.name_, lastErr.what());
				++errCount;
				continue;
			}

			// Read LSN
			int64_t lsn = *reinterpret_cast<const int64_t *>(dataSlice.data());
			if (lsn < 0) {
				lastErr = Error(errParseBin, "Invalid LSN value: %d", lsn);
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", ns_.name_, lastErr.what());
				++errCount;
				continue;
			}
			lsn_t l(lsn);
			if (nsIsSystem) {
				l.SetServer(0);
			}

			maxLSN = std::max(maxLSN, l.Counter());
			minLSN = std::min(minLSN, l.Counter());
			dataSlice = dataSlice.substr(sizeof(lsn));

			std::unique_lock lck(mtx_);
			cv_.wait(lck, [this] { return !items_.IsFull() || terminated_; });
			if (terminated_) {
				return;
			}
			auto &item = items_.PlaceItem();
			lck.unlock();

			auto &sliceStorageP = slices_[sliceId];
			if (sliceStorageP.len < dataSlice.size()) {
				sliceStorageP.len = dataSlice.size() * 1.1;
				sliceStorageP.data.reset(new char[sliceStorageP.len]);
			}
			memcpy(sliceStorageP.data.get(), dataSlice.data(), dataSlice.size());
			dataSlice = std::string_view(sliceStorageP.data.get(), dataSlice.size());
			sliceId = (sliceId + 1) % slices_.size();
			item.impl.Unsafe(true);
			try {
				item.impl.FromCJSON(dataSlice);
			} catch (const Error &err) {
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", ns_.name_, err.what());
				++errCount;
				lastErr = err;

				lck.lock();
				items_.ErasePlaced();
				lck.unlock();
				continue;
			}
			item.impl.Value().SetLSN(l);
			// Prealloc payload here, because reading|parsing thread is faster then index insertion thread
			item.preallocPl = PayloadValue(item.impl.GetConstPayload().RealSize());

			lck.lock();
			const bool wasEmpty = items_.HasNoWrittenItems();
			items_.WritePlaced();
			lck.unlock();

			if (wasEmpty) {
				cv_.notify_all();
			}
		}
	}
	std::lock_guard lck(mtx_);
	terminated_ = true;
	loadingData_.maxLSN = maxLSN;
	loadingData_.minLSN = minLSN;
	loadingData_.lastErr = std::move(lastErr);
	loadingData_.errCount = errCount;
	loadingData_.ldcount = ldcount;
	if (items_.HasNoWrittenItems()) {
		cv_.notify_all();
	}
}

void ItemsLoader::insertion() {
	bool terminated = false;
	bool requireNotification = false;

	assertrx(ns_.indexes_.firstCompositePos() != 0);

	IndexInserters indexInserters(ns_.indexes_, ns_.payloadType_);
	indexInserters.Run(indexInsertionThreads_);

	span<ItemData> items;
	VariantArray krefs, skrefs;
	const unsigned totalIndexesSize = ns_.indexes_.totalSize();
	const unsigned compositeIndexesSize = ns_.indexes_.compositeIndexesSize();
	dummy_mutex dummyMtx;
	do {
		std::unique_lock lck(mtx_);
		if (items_.IsFull()) {
			requireNotification = true;
		}
		items_.Erase(items.size());
		if (requireNotification && items.size()) {
			requireNotification = false;
			cv_.notify_all();
		}
		cv_.wait(lck, [this] { return terminated_ || !items_.HasNoWrittenItems(); });
		items = items_.Tail(kReadSize);
		if (items.size()) {
			lck.unlock();

			const unsigned startId = ns_.items_.size();
			for (unsigned i = 0; i < items.size(); ++i) {
				ns_.items_.emplace_back(std::move(items[i].preallocPl));
			}

			if (totalIndexesSize > 1) {
				indexInserters.BuildSimpleIndexesAsync(startId, items, span<PayloadValue>(ns_.items_.data() + startId, items.size()));
				indexInserters.AwaitIndexesBuild();
			}

			for (unsigned i = 0; i < items.size(); ++i) {
				const auto id = i + startId;
				auto &plData = ns_.items_[id];
				Payload pl(ns_.payloadType_, plData);
				Payload plNew(items[i].impl.GetPayload());
				// Index [0] must be inserted after all other simple indexes
				doInsertField(ns_.indexes_, 0, id, pl, plNew, krefs, skrefs, dummyMtx);
			}

			if (compositeIndexesSize) {
				indexInserters.BuildCompositeIndexesAsync();
			}
			for (unsigned i = 0; i < items.size(); ++i) {
				auto &plData = ns_.items_[i + startId];
				Payload pl(ns_.payloadType_, plData);
				plData.SetLSN(items[i].impl.Value().GetLSN());
				ns_.repl_.dataHash ^= pl.GetHash();
				ns_.itemsDataSize_ += plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
			}
			if (compositeIndexesSize) {
				indexInserters.AwaitIndexesBuild();
			}
		} else {
			terminated = terminated_;
		}
	} while (!terminated);
}

void ItemsLoader::clearIndexCache() {
	for (auto &idx : ns_.indexes_) {
		idx->ClearCache();
		idx->Commit();
	}
}

template <typename MutexT>
void ItemsLoader::doInsertField(NamespaceImpl::IndexesStorage &indexes, unsigned field, IdType id, Payload &pl, Payload &plNew,
								VariantArray &krefs, VariantArray &skrefs, MutexT &mtx) {
	Index &index = *indexes[field];
	const bool isIndexSparse = index.Opts().IsSparse();
	if (isIndexSparse) {
		assertrx(index.Fields().getTagsPathsLength() > 0);
		try {
			plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} catch (const Error &) {
			skrefs.resize(0);
		}
	} else {
		plNew.Get(field, skrefs);
	}

	if (index.Opts().GetCollateMode() == CollateUTF8)
		for (auto &key : skrefs) key.EnsureUTF8();

	// Put value to index
	krefs.resize(0);
	bool needClearCache{false};
	index.Upsert(krefs, skrefs, id, needClearCache);

	if (!isIndexSparse) {
		// Put value to payload
		// Array values may reallocate payload, so must be synchronized via mutex
		if (pl.Type().Field(field).IsArray()) {
			std::lock_guard lck(mtx);
			pl.Set(field, krefs);
		} else {
			if (krefs.size() != 1) {
				throw Error(errLogic, "Array value for scalar field");
			}
			shared_lock lck(mtx);
			pl.SetSingleElement(field, krefs[0]);
		}
	}
}

IndexInserters::IndexInserters(NamespaceImpl::IndexesStorage &indexes, PayloadType pt) : indexes_(indexes), pt_(std::move(pt)) {
	for (int i = 1; i < indexes_.firstCompositePos(); ++i) {
		if (indexes_[i]->Opts().IsArray()) {
			hasArrayIndexes_ = true;
			break;
		}
	}
}

void IndexInserters::Run(unsigned threadsCnt) {
	assertrx(threads_.empty());
	if (threadsCnt) {
		threads_.reserve(threadsCnt);
		for (unsigned tid = 0; tid < threadsCnt; ++tid) {
			threads_.emplace_back([this, tid] { insertionLoop(tid + kTIDOffset); });
		}
	}
}

void IndexInserters::Stop() {
	if (threads_.size()) {
		std::lock_guard lck(mtx_);
		shared_.terminate = true;
		cv_.notify_all();
	}
	for (auto &th : threads_) {
		th.join();
	}
	threads_.clear();
}

void IndexInserters::AwaitIndexesBuild() {
	if (readyThreads_.load(std::memory_order_acquire) != threads_.size()) {
		std::unique_lock lck(mtx_);
		cv_.wait(lck, [this] { return readyThreads_.load(std::memory_order_acquire) == threads_.size(); });
		if (!status_.ok()) {
			throw status_;
		}
		assertrx(shared_.threadsWithNewData.empty());
	}
}

void IndexInserters::BuildSimpleIndexesAsync(unsigned startId, span<ItemsLoader::ItemData> newItems, span<PayloadValue> nsItems) {
	{
		std::lock_guard lck(mtx_);
		shared_.newItems = newItems;
		shared_.nsItems = nsItems;
		shared_.startId = startId;
		assertrx(shared_.threadsWithNewData.empty());
		for (unsigned tid = 0; tid < threads_.size(); ++tid) {
			shared_.threadsWithNewData.emplace_back(tid + kTIDOffset);
		}
		shared_.composite = false;
		readyThreads_.store(0, std::memory_order_relaxed);
	}
	cv_.notify_all();
}

void IndexInserters::BuildCompositeIndexesAsync() {
	{
		std::lock_guard lck(mtx_);
		assertrx(shared_.threadsWithNewData.empty());
		for (unsigned tid = 0; tid < threads_.size(); ++tid) {
			shared_.threadsWithNewData.emplace_back(tid + kTIDOffset);
		}
		shared_.composite = true;
		readyThreads_.store(0, std::memory_order_relaxed);
	}
	cv_.notify_all();
}

void IndexInserters::insertionLoop(unsigned threadId) noexcept {
	VariantArray krefs, skrefs;
	const unsigned firstCompositeIndex = indexes_.firstCompositePos();
	const unsigned totalIndexes = indexes_.totalSize();

	while (true) {
		try {
			std::unique_lock lck(mtx_);
			cv_.wait(lck, [this, threadId] {
				return shared_.terminate || std::find(shared_.threadsWithNewData.begin(), shared_.threadsWithNewData.end(), threadId) !=
												shared_.threadsWithNewData.end();
			});
			if (shared_.terminate) {
				return;
			}
			shared_.threadsWithNewData.erase(std::find(shared_.threadsWithNewData.begin(), shared_.threadsWithNewData.end(), threadId));
			lck.unlock();

			const unsigned startId = shared_.startId;
			const unsigned threadsCnt = threads_.size();
			assertrx(shared_.newItems.size() == shared_.nsItems.size());
			if (shared_.composite) {
				for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
					const auto id = startId + i;
					auto &plData = shared_.nsItems[i];
					for (unsigned field = firstCompositeIndex + threadId - kTIDOffset; field < totalIndexes; field += threadsCnt) {
						bool needClearCache{false};
						indexes_[field]->Upsert(Variant{plData}, id, needClearCache);
					}
				}
			} else {
				if (hasArrayIndexes_) {
					for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
						const auto id = startId + i;
						auto &item = shared_.newItems[i].impl;
						auto &plData = shared_.nsItems[i];
						Payload pl(pt_, plData);
						Payload plNew = item.GetPayload();
						for (unsigned field = threadId; field < firstCompositeIndex; field += threadsCnt) {
							ItemsLoader::doInsertField(indexes_, field, id, pl, plNew, krefs, skrefs,
													   plArrayMtxs_[id % plArrayMtxs_.size()]);
						}
					}
				} else {
					dummy_mutex dummyMtx;
					for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
						const auto id = startId + i;
						auto &item = shared_.newItems[i].impl;
						auto &plData = shared_.nsItems[i];
						Payload pl(pt_, plData);
						Payload plNew = item.GetPayload();
						for (unsigned field = threadId; field < firstCompositeIndex; field += threadsCnt) {
							ItemsLoader::doInsertField(indexes_, field, id, pl, plNew, krefs, skrefs, dummyMtx);
						}
					}
				}
			}
			onItemsHandled();
		} catch (Error &e) {
			onException(e);
		} catch (...) {
			onException(Error(errLogic, "Unknown exception in insertion loop"));
		}
	}
}

}  // namespace reindexer
