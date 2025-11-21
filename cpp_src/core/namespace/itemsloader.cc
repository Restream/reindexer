#include "itemsloader.h"
#include "core/index/float_vector/float_vector_index.h"
#include "tools/logger.h"

namespace reindexer {

ItemsLoader::LoadData ItemsLoader::Load() {
	logFmt(LogTrace, "Loading items to '{}' from storage", ns_.name_);

	prepareANNData();

	std::thread readingTh = std::thread([this] {
		try {
			reading();
		} catch (...) {
			lock_guard lck(mtx_);
			loadingData_.ex = std::current_exception();
			terminated_ = true;
			cv_.notify_all();
		}
	});
	std::thread insertionTh = std::thread([this] {
		try {
			insertion();
		} catch (...) {
			lock_guard lck(mtx_);
			loadingData_.ex = std::current_exception();
			terminated_ = true;
			cv_.notify_all();
		}
	});
	readingTh.join();
	insertionTh.join();

	loadCachedANNIndexes();

	clearIndexCache();
	if (loadingData_.ex) {
		std::rethrow_exception(loadingData_.ex);
	}
	return loadingData_;
}

void ItemsLoader::prepareANNData() {
	auto annIndexes = ns_.getVectorIndexes();
	annIndexes_.resize(annIndexes.size());
	std::transform(annIndexes.begin(), annIndexes.end(), annIndexes_.begin(), [](const FloatVectorIndexData& d) {
		return ANNIndexInfo{.field = d.ptField, .dims = d.ptr->Opts().FloatVector().Dimension()};
	});

	const auto [pkIdx, pkField] = ns_.getPkIdx();
	if (pkIdx && !annIndexes.empty()) {
		annCacheReader_ = std::make_unique<ann_storage_cache::Reader>(
			ns_.name_, std::chrono::nanoseconds(ns_.lastUpdateTimeNano()), pkField, ns_.storage_,
			[this](std::string_view name) { return ns_.getIndexByName(name); },
			[this](size_t field) { return ns_.getIndexDefinition(field); });
	} else if (!annIndexes.empty()) {
		assertrx_dbg(false);  // Do not expect this error in test scenarios
		logFmt(LogError, "[{}] PK field does not exist. Unable to use ANN storage cache", ns_.name_);
	}
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
	VariantArray tmp(1);
	for (dbIter->Seek(kRxStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kRxStorageItemPrefix "\xFF\xFF\xFF\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			if (!ns_.pkFields().size()) {
				throw Error(errLogic, "Can't load data storage of '{}' - there are no PK fields in ns", ns_.name_);
			}
			if (dataSlice.size() < sizeof(int64_t)) {
				lastErr = Error(errParseBin, "Not enough data in data slice");
				logFmt(LogTrace, "Error load item to '{}' from storage: '{}'", ns_.name_, lastErr.what());
				++errCount;
				continue;
			}

			// Read LSN
			int64_t lsn = *reinterpret_cast<const int64_t*>(dataSlice.data());
			if (lsn < 0) {
				lastErr = Error(errParseBin, "Invalid LSN value: {}", lsn);
				logFmt(LogTrace, "Error load item to '{}' from storage: '{}'", ns_.name_, lastErr.what());
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

			unique_lock lck(mtx_);
			cv_.wait(lck, [this] { return !items_.IsFull() || terminated_; });
			if (terminated_) {
				return;
			}
			auto& item = items_.PlaceItem();
			lck.unlock();

			auto& sliceStorageP = slices_[sliceId];
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
			} catch (const Error& err) {
				logFmt(LogTrace, "Error load item to '{}' from storage: '{}'", ns_.name_, err.what());
				++errCount;
				lastErr = err;

				lck.lock();
				items_.ErasePlaced();
				lck.unlock();
				continue;
			}
			item.impl.Value().SetLSN(l);
			// Preallocate payload here, because reading|parsing thread is faster than index insertion thread
			item.preallocPl = PayloadValue(item.impl.GetConstPayload().RealSize());

			for (const auto& idx : annIndexes_) {
				if (!annCacheReader_ || !annCacheReader_->HasCacheFor(idx.field)) {
					continue;
				}

				auto& vectors = vectorsData_[idx.field];
				Variant vecVar = item.impl.GetField(idx.field);
				const size_t vecSizeBytes = idx.dims * sizeof(float);
				auto cur = vectors.emplace_back(std::make_unique<uint8_t[]>(vecSizeBytes)).get();
				if (vecVar.Type().Is<KeyValueType::FloatVector>()) {
					ConstFloatVectorView vec = ConstFloatVectorView(vecVar);
					if (!vec.Dimension().IsZero()) {
						assertrx(vec.Dimension() == FloatVectorDimension(idx.dims));
						std::memcpy(cur, vec.Data(), vecSizeBytes);
						tmp[0] = Variant{FloatVectorView{std::span<float>{reinterpret_cast<float*>(cur), idx.dims}}};
						item.impl.GetPayload().Set(idx.field, tmp);
					}
				} else {
					logFmt(LogWarning, "[{}] Error load float vector from storage: actual type of the field is {}", ns_.name_,
						   vecVar.Type().Name());
				}
			}

			lck.lock();
			const bool wasEmpty = items_.HasNoWrittenItems();
			items_.WritePlaced();

			if (wasEmpty) {
				cv_.notify_all();
			}
		}
	}

	lock_guard lck(mtx_);
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

	IndexInserters indexInserters(ns_.indexes_, ns_.payloadType_, annCacheReader_ ? annCacheReader_.get() : nullptr);
	indexInserters.Run(indexInsertionThreads_);

	std::span<ItemData> items;
	VariantArray krefs, skrefs;
	const unsigned totalIndexesSize = ns_.indexes_.totalSize();
	const unsigned compositeIndexesSize = ns_.indexes_.compositeIndexesSize();
	DummyMutex dummyMtx;
	do {
		unique_lock lck(mtx_);
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
				indexInserters.BuildSimpleIndexesAsync(startId, items, std::span<PayloadValue>(ns_.items_.data() + startId, items.size()));
				indexInserters.AwaitIndexesBuild();
			}

			for (unsigned i = 0; i < items.size(); ++i) {
				const auto id = i + startId;
				auto& plData = ns_.items_[id];
				Payload pl(ns_.payloadType_, plData);
				Payload plNew(items[i].impl.GetPayload());
				// Index [0] must be inserted after all other simple indexes
				doInsertField(ns_.indexes_, 0, id, pl, plNew, krefs, skrefs, dummyMtx, annCacheReader_.get());
			}

			if (compositeIndexesSize) {
				indexInserters.BuildCompositeIndexesAsync();
			}
			for (unsigned i = 0; i < items.size(); ++i) {
				auto& plData = ns_.items_[i + startId];
				plData.SetLSN(items[i].impl.Value().GetLSN());
				ns_.repl_.dataHash ^= ns_.calculateItemHash(i + startId);
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

void ItemsLoader::loadCachedANNIndexes() {
	if (!annCacheReader_ || vectorsData_.empty()) {
		return;
	}
	ns_.annStorageCacheState_.Clear();

	auto pkIdx = ns_.getPkIdx().first;
	const RdxContext dummyCtx;

	std::vector<unsigned> indexesWithError;
	for (auto cachedIndex = annCacheReader_->GetNextCachedIndex(); cachedIndex.has_value();
		 cachedIndex = annCacheReader_->GetNextCachedIndex()) {
		logFmt(LogInfo, "[{}] Trying to load ANN index '{}' from storage cache", ns_.name_, cachedIndex->name);
		auto idxPtr = dynamic_cast<FloatVectorIndex*>(ns_.indexes_[cachedIndex->field].get());
		assertrx(idxPtr);
		assertrx(!idxPtr->Opts().IsArray());
		assertrx(!idxPtr->Opts().IsSparse());
		const auto vecSizeBytes = sizeof(float) * idxPtr->Opts().FloatVector().Dimension();
		auto vectorsDataIt = vectorsData_.find(cachedIndex->field);
		assertrx(vectorsDataIt != vectorsData_.end());
		auto& vectorsData = vectorsDataIt->second;
		auto res = idxPtr->LoadIndexCache(cachedIndex->data, IsComposite(pkIdx->Type()), [&](const VariantArray& keys, void* targetVec) {
			auto res = pkIdx->SelectKey(keys, CondEq, 0, Index::SelectContext{}, dummyCtx).Front();
			if (!res[0].ids_.empty()) {
				const IdType id = res[0].ids_[0];
				std::memcpy(targetVec, vectorsData[id].get(), vecSizeBytes);
				return id;
			} else {
				throw Error(errLogic, "Requested PK does not exist");
			}
		});
		if (!res.ok()) {
			assertrx_dbg(false);  // Do not expect this error in test scenarios
			logFmt(LogError, "[{}] Unable to restore ANN index '{}' from storage cache: {}", ns_.name_, cachedIndex->name, res.what());
			indexesWithError.emplace_back(cachedIndex->field);
			continue;
		}
		vectorsData = std::vector<std::unique_ptr<uint8_t[]>>();
		ns_.annStorageCacheState_.Update(idxPtr->Name(), cachedIndex->lastUpdate);

		// Strip restored vectors
		VariantArray tmp;
		tmp.resize(1);
		for (IdType id = 0, s = ns_.items_.size(); id < s; ++id) {
			Payload pl(ns_.payloadType_, ns_.items_[id]);
			Variant vecVar = pl.Get(cachedIndex->field, 0);
			if (vecVar.Type().Is<KeyValueType::FloatVector>()) {
				ConstFloatVectorView vec = ConstFloatVectorView(vecVar);
				if (vec.IsEmpty()) {
					bool clearCache = false;
					std::ignore = ns_.indexes_[cachedIndex->field]->Upsert(vecVar, id, clearCache);
				} else {
					vec.Strip();
					tmp[0] = Variant{vec};
					pl.Set(cachedIndex->field, tmp);
				}
			} else {
				logFmt(LogWarning, "[{}] Error load float vector from storage: actual type of the field is {}", ns_.name_,
					   vecVar.Type().Name());
			}
		}
		logFmt(LogInfo, "[{}] ANN index '{}' was restored from storage cache", ns_.name_, cachedIndex->name);
	}
	loadCachedANNIndexesFallback(indexesWithError);
}

void ItemsLoader::loadCachedANNIndexesFallback(const std::vector<unsigned>& indexes) {
	VariantArray krefs, skrefs;
	skrefs.resize(1);
	for (auto field : indexes) {
		auto& idxPtr = ns_.indexes_[field];
		auto vectorsDataIt = vectorsData_.find(field);
		assertrx(vectorsDataIt != vectorsData_.end());
		auto& vectorsData = vectorsDataIt->second;
		const auto dims = idxPtr->Opts().FloatVector().Dimension();

		for (IdType itemID = 0; itemID < IdType(ns_.items_.size()); ++itemID) {
			auto& pv = ns_.items_[itemID];
			if (pv.IsFree()) [[unlikely]] {
				continue;
			}
			Payload pl(ns_.payloadType_, pv);
			krefs.resize(0);
			auto& vecPtr = vectorsData[itemID];
			std::span<float> vec{reinterpret_cast<float*>(vecPtr.get()), dims};
			skrefs[0] = Variant{ConstFloatVectorView{vec}};
			bool needClearCache{false};
			idxPtr->Upsert(krefs, skrefs, itemID, needClearCache);
			pl.Set(field, krefs);
			vecPtr.reset();
		}
		vectorsData = std::vector<std::unique_ptr<uint8_t[]>>();
	}
}

void ItemsLoader::clearIndexCache() {
	for (auto& idx : ns_.indexes_) {
		idx->DestroyCache();
		idx->Commit();
	}
}

template <typename MutexT>
void ItemsLoader::doInsertField(NamespaceImpl::IndexesStorage& indexes, unsigned field, IdType id, Payload& pl, Payload& plNew,
								VariantArray& krefs, VariantArray& skrefs, MutexT& mtx, const ann_storage_cache::Reader* annCache) {
	Index& index = *indexes[field];
	const IsSparse isIndexSparse = index.Opts().IsSparse();
	if (isIndexSparse) {
		assertrx(index.Fields().getTagsPathsLength() > 0);
		try {
			plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} catch (const Error&) {
			skrefs.resize(0);
		}
	} else {
		plNew.Get(field, skrefs);
	}

	if (index.Opts().GetCollateMode() == CollateUTF8) {
		for (auto& key : skrefs) {
			key.EnsureUTF8();
		}
	}

	if (!annCache || !annCache->HasCacheFor(field)) {
		// Put value to index
		krefs.resize(0);
		bool needClearCache{false};
		index.Upsert(krefs, skrefs, id, needClearCache);
	} else {
		// Just make vector view's copy
		krefs = std::move(skrefs);
	}

	if (!isIndexSparse) {
		// Put value to payload
		// Array values may reallocate payload, so must be synchronized via mutex
		if (pl.Type().Field(field).IsArray()) {
			lock_guard lck(mtx);
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

IndexInserters::IndexInserters(NamespaceImpl::IndexesStorage& indexes, PayloadType pt, const ann_storage_cache::Reader* annCache)
	: indexes_{indexes}, pt_{std::move(pt)}, annCache_{annCache} {
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
		lock_guard lck(mtx_);
		shared_.terminate = true;
		cvReady_.notify_all();
	}
	for (auto& th : threads_) {
		th.join();
	}
	threads_.clear();
}

void IndexInserters::AwaitIndexesBuild() {
	unique_lock lck(mtx_);
	cvDone_.wait(lck, [this] { return readyThreads_ == threads_.size(); });
	if (!status_.ok()) {
		throw status_;
	}
}

void IndexInserters::BuildSimpleIndexesAsync(unsigned startId, std::span<ItemsLoader::ItemData> newItems, std::span<PayloadValue> nsItems) {
	lock_guard lck(mtx_);
	shared_.newItems = newItems;
	shared_.nsItems = nsItems;
	shared_.startId = startId;
	shared_.composite = false;
	readyThreads_ = 0;
	++iteration_;
	cvReady_.notify_all();
}

void IndexInserters::BuildCompositeIndexesAsync() {
	lock_guard lck(mtx_);
	shared_.composite = true;
	readyThreads_ = 0;
	++iteration_;
	cvReady_.notify_all();
}

void IndexInserters::insertionLoop(unsigned threadId) noexcept {
	VariantArray krefs, skrefs;
	const unsigned firstCompositeIndex = indexes_.firstCompositePos();
	const unsigned totalIndexes = indexes_.totalSize();
	unsigned thisLoopIteration{0};

	while (true) {
		try {
			unique_lock lck(mtx_);
			cvReady_.wait(lck, [this, thisLoopIteration] { return shared_.terminate || iteration_ > thisLoopIteration; });
			if (shared_.terminate) {
				return;
			}
			lck.unlock();
			++thisLoopIteration;

			const unsigned startId = shared_.startId;
			const unsigned threadsCnt = threads_.size();
			assertrx(shared_.newItems.size() == shared_.nsItems.size());
			if (shared_.composite) {
				for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
					const auto id = startId + i;
					const auto& plData = shared_.nsItems[i];
					for (unsigned field = firstCompositeIndex + threadId - kTIDOffset; field < totalIndexes; field += threadsCnt) {
						bool needClearCache{false};
						std::ignore = indexes_[field]->Upsert(Variant{plData}, id, needClearCache);
					}
				}
			} else {
				if (hasArrayIndexes_) {
					for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
						const auto id = startId + i;
						auto& item = shared_.newItems[i].impl;
						auto& plData = shared_.nsItems[i];
						Payload pl(pt_, plData);
						Payload plNew = item.GetPayload();
						for (unsigned field = threadId - kTIDOffset + 1; field < firstCompositeIndex; field += threadsCnt) {
							ItemsLoader::doInsertField(indexes_, field, id, pl, plNew, krefs, skrefs,
													   plArrayMtxs_[id % plArrayMtxs_.size()], annCache_);
						}
					}
				} else {
					DummyMutex dummyMtx;
					for (unsigned i = 0; i < shared_.newItems.size(); ++i) {
						const auto id = startId + i;
						auto& item = shared_.newItems[i].impl;
						auto& plData = shared_.nsItems[i];
						Payload pl(pt_, plData);
						Payload plNew = item.GetPayload();
						for (unsigned field = threadId - kTIDOffset + 1; field < firstCompositeIndex; field += threadsCnt) {
							ItemsLoader::doInsertField(indexes_, field, id, pl, plNew, krefs, skrefs, dummyMtx, annCache_);
						}
					}
				}
			}
			onItemsHandled();
		} catch (Error& e) {
			onException(e);
		} catch (...) {
			onException(Error(errLogic, "Unknown exception in insertion loop"));
		}
	}
}

}  // namespace reindexer
