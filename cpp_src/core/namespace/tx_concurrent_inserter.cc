#include "tx_concurrent_inserter.h"
#include "core/index/float_vector/float_vector_index.h"
#include "namespaceimpl.h"
#include "tools/logger.h"

namespace reindexer {

TransactionContext::TransactionContext(const NamespaceImpl& ns, const LocalTransaction& tx) {
	const size_t expectedRows = tx.GetSteps().size();
	for (size_t field = 1, total = ns.indexes_.firstCompositePos(); field < total; ++field) {
		Index& idx = *ns.indexes_[field];
		if (idx.IsSupportMultithreadTransactions()) {
			indexesData_.emplace_back(field, expectedRows, FloatVectorDimension(idx.Opts().FloatVector().Dimension()));
		}
	}
}

void TransactionConcurrentInserter::operator()(const TransactionContext& ctx) noexcept {
	std::vector<std::thread> threads;
	threads.reserve(threads_);

	std::atomic<size_t> nextId{0};
	for (size_t i = 0; i < threads_; ++i) {
		threads.emplace_back([this](std::atomic<size_t>& nextId, const TransactionContext& ctx) noexcept { threadFn(nextId, ctx); },
							 std::reference_wrapper(nextId), std::reference_wrapper(ctx));
	}

	for (auto& th : threads) {
		th.join();
	}
}

#define kThreadErrorFormat "[{}] Unable to concurrently index item: '{}'"

void TransactionConcurrentInserter::threadFn(std::atomic<size_t>& nextId, const TransactionContext& ctx) noexcept {
	VariantArray krefs, skrefs;
	const PayloadType pt(ns_.payloadType_);

	for (size_t i = nextId.fetch_add(1, std::memory_order_relaxed); i < ctx.Buckets(); i = nextId.fetch_add(1, std::memory_order_relaxed)) {
		if (auto [field, vec] = ctx[i]; vec->IsValid()) {
			try {
				assertrx_dbg(field > 0);
				assertrx_dbg(field < size_t(ns_.indexes_.firstCompositePos()));
				assertrx_dbg(dynamic_cast<FloatVectorIndex*>(ns_.indexes_[field].get()));
				auto& idx = *static_cast<FloatVectorIndex*>(ns_.indexes_[field].get());
				krefs.resize(0);
				bool needClearCache{false};
				const IdType id = vec->id;
				krefs.emplace_back(idx.UpsertConcurrent(Variant{ConstFloatVectorView{vec->vec}, Variant::noHold}, id, needClearCache));
				assertrx(ns_.items_.exists(id));
				Payload pl(pt, ns_.items_[id]);
				pl.Set(field, krefs);
			} catch (std::exception& e) {
				// TODO: Probably this error handling should be improved. Currently assuming that it's better to crash, than loss data
				// Possible solution is to set an empty vector view and throw exception, but we do nos support empty vector view currently
				// (those views will crash on assertion at any call)
				assertf(false, kThreadErrorFormat, ns_.name_, e.what());
				logFmt(LogError, kThreadErrorFormat, ns_.name_, e.what());
				std::terminate();
			} catch (...) {
				assertf(false, kThreadErrorFormat, ns_.name_, "<unknown exception>");
				logFmt(LogError, kThreadErrorFormat, ns_.name_, "<unknown exception>");
				std::terminate();
			}
		}
	}
}

}  // namespace reindexer
