#include "knn_fixture.h"
#include <thread>
#include "allocs_tracker.h"
#include "core/ft/config/ftconfig.h"
#include "gtests/tools.h"
#include "yaml-cpp/yaml.h"

namespace reindexer_benchmarks::knn_bench {

using namespace std::string_view_literals;
using reindexer::VectorMetric;
using reindexer::QueryResults;
using reindexer::IndexOpts;
using reindexer::FloatVectorIndexOpts;
using reindexer::FTConfig;
using reindexer::MultithreadingMode;
using reindexer::ConstFloatVectorView;
using reindexer::Query;
using reindexer::HnswSearchParams;
using reindexer::IvfSearchParams;
using reindexer::Item;
using reindexer::Error;

static constexpr int kMaxValueTreeIndex = 1'000'000;

#ifdef REINDEX_WITH_TSAN
static constexpr size_t kK = 300;
static constexpr size_t kNProbe = 4;
static constexpr size_t kWordsInFtIndex = 300;
static constexpr size_t kDimention = 8;
static constexpr size_t kNsSize = 1'500;
#else  // REINDEX_WITH_TSAN
static constexpr size_t kK = 1'000;
static constexpr size_t kNProbe = 16;
static constexpr size_t kWordsInFtIndex = 1'000;
#if defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
static constexpr size_t kDimention = 32;
static constexpr size_t kNsSize = 10'000;
#else	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
static constexpr size_t kDimention = 32;
static constexpr size_t kNsSize = 100'000;
#endif	// defined(RX_WITH_STDLIB_DEBUG) || defined(REINDEX_WITH_ASAN)
#endif	// REINDEX_WITH_TSAN

template <IndexType, VectorMetric>
static consteval float radius() noexcept;

#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)

template <>
consteval float radius<IndexType::Hnsw, VectorMetric::L2>() noexcept {
	return std::numeric_limits<float>::max();
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::InnerProduct>() noexcept {
	return 0.0f;
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::Cosine>() noexcept {
	return 0.0f;
}

template <>
consteval float radius<IndexType::Ivf, VectorMetric::L2>() noexcept {
	return std::numeric_limits<float>::max();
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::InnerProduct>() noexcept {
	return 0.0f;
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::Cosine>() noexcept {
	return 0.0f;
}

#else	// defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)

template <>
consteval float radius<IndexType::Hnsw, VectorMetric::L2>() noexcept {
	return 26.5f;
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::InnerProduct>() noexcept {
	return 2.15f;
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::Cosine>() noexcept {
	return 0.129f;
}

template <>
consteval float radius<IndexType::Ivf, VectorMetric::L2>() noexcept {
	return 28.1f;
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::InnerProduct>() noexcept {
	return 0.3f;
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::Cosine>() noexcept {
	return 0.02f;
}
#endif	// defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)

static constexpr int kHalfMaxValueTreeIndex = kMaxValueTreeIndex / 2;

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
class [[nodiscard]] KnnBench<indexType, metric>::ItemsCounter : private LowSelectivityItemsCounter<> {
	using Base = LowSelectivityItemsCounter<>;

public:
	ItemsCounter(State& state, size_t limit) noexcept : Base{state}, limit_{limit} {}
	using Base::Base;
	~ItemsCounter() {
		state_.counters["Items/Op"] = itemsCounter_ / std::max<size_t>(size_t(1), state_.iterations());

		if (limit_.has_value()) {
			state_.counters["Limit"] = *limit_;
		} else {
			if constexpr (knnParams != KnnParams::Radius) {
				state_.counters["K"] = kK;
			}
		}
	}
	void operator()(const QueryResults& qres) noexcept {
		Base::operator()(qres);
		itemsCounter_ += qres.Count();
	}

private:
	std::optional<size_t> limit_;
	size_t itemsCounter_{0};
};

template <IndexType indexType, VectorMetric metric>
KnnBench<indexType, metric>::KnnBench(Reindexer* db, std::string_view name, WithQuantization withQuantization)
	: BaseFixture(db, name, kNsSize * 3), FullTextBase{kWordsInFtIndex}, withQuantization_(withQuantization) {
	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
	nsdef_.AddIndex("tree", "tree", "int", IndexOpts());
	switch (indexType) {
		case IndexType::Hnsw: {
			auto fvOpts = FloatVectorIndexOpts{}
							  .SetDimension(kDimention)
							  .SetMetric(metric)
							  .SetM(16)
							  .SetEfConstruction(200)
							  .SetMultithreading(MultithreadingMode::MultithreadTransactions)
							  .SetStartSize(kNsSize);

			if (withQuantization_ == WithQuantization::Yes) {
				std::ignore = fvOpts.SetQuantizationConfig({.quantile = 1.f, .sampleSize = kNsSize, .quantizationThreshold = kNsSize});
			}

			nsdef_.AddIndex("vec", "hnsw", "float_vector", IndexOpts().SetFloatVector(IndexHnsw, std::move(fvOpts)));
			break;
		}
		case IndexType::Ivf: {
			nsdef_.AddIndex("vec", "ivf", "float_vector",
							IndexOpts().SetFloatVector(
								IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimention).SetMetric(metric).SetNCentroids(kNsSize / 100)));
			break;
		}
	}

	nsdef_.AddIndex("ft", "text", "string", IndexOpts().SetConfig(IndexFastFT, FTConfig{1}.GetJSON({})));
}

template KnnBench<IndexType::Hnsw, VectorMetric::L2>::KnnBench(Reindexer*, std::string_view, WithQuantization);
template KnnBench<IndexType::Hnsw, VectorMetric::Cosine>::KnnBench(Reindexer*, std::string_view, WithQuantization);
template KnnBench<IndexType::Hnsw, VectorMetric::InnerProduct>::KnnBench(Reindexer*, std::string_view, WithQuantization);
template KnnBench<IndexType::Ivf, VectorMetric::L2>::KnnBench(Reindexer*, std::string_view, WithQuantization);
template KnnBench<IndexType::Ivf, VectorMetric::Cosine>::KnnBench(Reindexer*, std::string_view, WithQuantization);
template KnnBench<IndexType::Ivf, VectorMetric::InnerProduct>::KnnBench(Reindexer*, std::string_view, WithQuantization);

template <IndexType indexType, VectorMetric metric>
Item KnnBench<indexType, metric>::MakeItem(State& state) {
	return MakeItem(state, id_seq_->Next());
}

template <IndexType indexType, VectorMetric metric>
Item KnnBench<indexType, metric>::MakeItem(State&, int id) {
	Item item = db_->NewItem(nsdef_.name);
	if (item.Status().ok()) {
		item["id"sv] = id;
		item["tree"sv] = rand() % kMaxValueTreeIndex;
		static std::array<float, kDimention> vect;
		reindexer_tests_tools::rndFloatVector(vect);
		item["vec"sv] = ConstFloatVectorView{vect};
	}
	return item;
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Fill(State& state) {
	AllocsTracker allocsTracker(state);

	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto tx = db_->NewTransaction(nsdef_.name);
		if (!tx.Status().ok()) {
			state.SkipWithError(tx.Status().what());
		}
		for (size_t i = 0; i < kNsSize; ++i) {
			auto item = MakeItem(state);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
			}

			auto err = tx.Upsert(std::move(item));
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
		QueryResults qr;
		auto err = db_->CommitTransaction(tx, qr);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		state.SetItemsProcessed(state.items_processed() + kNsSize);
	}
	if constexpr (indexType == IndexType::Hnsw) {
		prepareStreamingParams(state);
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::prepareStreamingParams(State& state) {
	if constexpr (indexType != IndexType::Hnsw) {
		return;
	}

	streamingQuery_.resize(kDimention);
	std::array<float, kDimention> buf{};
	reindexer_tests_tools::rndFloatVector(buf);
	streamingQuery_.assign(buf.begin(), buf.end());

	const ConstFloatVectorView query{std::span<const float>{streamingQuery_.data(), streamingQuery_.size()}};
	const int idThreshold = int(kNsSize / 2);
	const auto refParams = HnswSearchParams{}.K(kK).Ef(kK);

	QueryResults qr;
	auto err = db_->Select(Query(nsdef_.name).Where("tree"sv, CondGt, kHalfMaxValueTreeIndex).WhereKNN("vec"sv, query, refParams), qr);
	if (!err.ok()) {
		state.SkipWithError(err.what());
		return;
	}
	streamingLimitTree50_ = qr.Count();

	qr.Clear();

	err = db_->Select(Query(nsdef_.name)
						  .Where("id"sv, CondGt, idThreshold)
						  .Where("tree"sv, CondGt, kHalfMaxValueTreeIndex)
						  .WhereKNN("vec"sv, query, refParams),
					  qr);
	if (!err.ok()) {
		state.SkipWithError(err.what());
		return;
	}
	streamingLimit2Cond_ = qr.Count();

	state.counters["Limit/Tree50pct"] = streamingLimitTree50_;
	state.counters["Limit/2Cond25pct"] = streamingLimit2Cond_;
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Insert(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeItem(state);
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}

		auto err = db_->Insert(nsdef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		state.SetItemsProcessed(state.items_processed() + 1);
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Update(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeItem(state, rand() % id_seq_->Current());
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}

		auto err = db_->Update(nsdef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		state.SetItemsProcessed(state.items_processed() + 1);
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::FillFtIndex(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto tx = db_->NewTransaction(nsdef_.name);
		if (!tx.Status().ok()) {
			state.SkipWithError(tx.Status().what());
		}
		const auto count = id_seq_->Current();
		for (int i = id_seq_->Start(); i < count; ++i) {
			auto q = Query(nsdef_.name).Set("ft"sv, CreatePhrase()).Where("id"sv, CondEq, i);
			q.type_ = QueryUpdate;
			auto err = tx.Modify(std::move(q));
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
		{
			QueryResults qr;
			auto err = db_->CommitTransaction(tx, qr);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
		{
			// build ft index
			QueryResults qr;
			auto err = db_->Select(Query(nsdef_.name).Where("ft"sv, CondEq, RndWord1()), qr);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::DeleteFtIndex(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto err = db_->DropIndex(nsdef_.name, reindexer::IndexDef("ft"));
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

template <IndexType, VectorMetric, KnnParams>
struct KnnSearchParams;

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Hnsw, metric, KnnParams::K> {
	reindexer::KnnSearchParams operator()() const noexcept { return HnswSearchParams{}.Ef(kK).K(kK); }
};

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Hnsw, metric, KnnParams::K_Radius> {
	reindexer::KnnSearchParams operator()() const noexcept {
		return HnswSearchParams{}.Ef(kK).K(kK).Radius(radius<IndexType::Hnsw, metric>());
	}
};

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Hnsw, metric, KnnParams::Radius> {
	reindexer::KnnSearchParams operator()() const noexcept { return HnswSearchParams{}.Ef(kK).Radius(radius<IndexType::Hnsw, metric>()); }
};

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Ivf, metric, KnnParams::K> {
	reindexer::KnnSearchParams operator()() const noexcept { return IvfSearchParams{}.NProbe(kNProbe).K(kK); }
};

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Ivf, metric, KnnParams::K_Radius> {
	reindexer::KnnSearchParams operator()() const noexcept {
		return IvfSearchParams{}.NProbe(kNProbe).K(kK).Radius(radius<IndexType::Ivf, metric>());
	}
};

template <VectorMetric metric>
struct [[nodiscard]] KnnSearchParams<IndexType::Ivf, metric, KnnParams::Radius> {
	reindexer::KnnSearchParams operator()() const noexcept {
		return IvfSearchParams{}.NProbe(kNProbe).Radius(radius<IndexType::Ivf, metric>());
	}
};

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::Knn(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name).WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}());
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::KnnWithVectors(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}())
			.SelectAllFields();
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::KnnWithCondition(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.Where("tree"sv, CondGt, rand() % kHalfMaxValueTreeIndex)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}());
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::KnnWith2Conditions(State& state) {
	static std::array<float, kDimention> vect;
	const auto halfOfItemsCount = id_seq_->Current() / 2;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.Where("id"sv, CondGt, rand() % halfOfItemsCount)
			.Where("tree"sv, CondGt, rand() % kHalfMaxValueTreeIndex)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}());
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::AndHybridRrf(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}())
			.Where("ft"sv, CondEq, RndWord1() + ' ' + RndWord1())
			.Sort("RRF()"sv, false);
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::AndHybridLinear(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}())
			.Where("ft"sv, CondEq, RndWord1() + ' ' + RndWord1())
			.Sort("rank(vec) + rank(ft)"sv, false);
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::OrHybridRrf(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}())
			.Or()
			.Where("ft"sv, CondEq, RndWord1() + ' ' + RndWord1())
			.Sort("RRF()"sv, false)
			.Limit(kK);
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
void KnnBench<indexType, metric>::OrHybridLinear(State& state) {
	static std::array<float, kDimention> vect;
	const auto q = [&] {
		reindexer_tests_tools::rndFloatVector(vect);
		return Query(nsdef_.name)
			.WhereKNN("vec"sv, ConstFloatVectorView{vect}, KnnSearchParams<indexType, metric, knnParams>{}())
			.Or()
			.Where("ft"sv, CondEq, RndWord1() + ' ' + RndWord1())
			.Sort("rank(vec) + rank(ft)"sv, false)
			.Limit(kK);
	};
	ItemsCounter<knnParams> itemsCounter{state};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::StreamingKnnTree50pct(State& state) {
	if constexpr (indexType != IndexType::Hnsw) {
		state.SkipWithError("Streaming KNN benchmarks are supported for HNSW indexes only");
		return;
	}
	if (streamingLimitTree50_ == 0) {
		state.SkipWithError("Run Fill before streaming benchmarks");
	}

	const ConstFloatVectorView query{std::span<const float>{streamingQuery_.data(), streamingQuery_.size()}};
	const auto limit = streamingLimitTree50_;
	const auto q = [&] {
		return Query(nsdef_.name).Where("tree"sv, CondGt, kHalfMaxValueTreeIndex).WhereKNN("vec"sv, query, HnswSearchParams{}).Limit(limit);
	};
	ItemsCounter<KnnParams::K> itemsCounter{state, limit};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::StreamingKnnTree50pctLimit10(State& state) {
	if constexpr (indexType != IndexType::Hnsw) {
		state.SkipWithError("Streaming KNN benchmarks are supported for HNSW indexes only");
		return;
	}
	if (streamingQuery_.empty()) {
		state.SkipWithError("Run Fill before streaming benchmarks");
	}

	static constexpr size_t kLimit = 10;
	const ConstFloatVectorView query{std::span<const float>{streamingQuery_.data(), streamingQuery_.size()}};
	const auto q = [&] {
		return Query(nsdef_.name)
			.Where("tree"sv, CondGt, kHalfMaxValueTreeIndex)
			.WhereKNN("vec"sv, query, HnswSearchParams{})
			.Limit(kLimit);
	};
	ItemsCounter<KnnParams::K> itemsCounter{state, kLimit};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::StreamingKnn2Cond25pct(State& state) {
	if constexpr (indexType != IndexType::Hnsw) {
		state.SkipWithError("Streaming KNN benchmarks are supported for HNSW indexes only");
		return;
	}
	if (streamingLimit2Cond_ == 0) {
		state.SkipWithError("Run Fill before streaming benchmarks");
	}

	const int idThreshold = int(kNsSize / 2);
	const ConstFloatVectorView query{std::span<const float>{streamingQuery_.data(), streamingQuery_.size()}};
	const auto limit = streamingLimit2Cond_;
	const auto q = [&] {
		return Query(nsdef_.name)
			.Where("id"sv, CondGt, idThreshold)
			.Where("tree"sv, CondGt, kHalfMaxValueTreeIndex)
			.WhereKNN("vec"sv, query, HnswSearchParams{})
			.Limit(limit);
	};
	ItemsCounter<KnnParams::K> itemsCounter{state, limit};
	benchQuery(q, state, itemsCounter);
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::StreamingKnnNoFilter(State& state) {
	if constexpr (indexType != IndexType::Hnsw) {
		state.SkipWithError("Streaming KNN benchmarks are supported for HNSW indexes only");
		return;
	}
	if (streamingQuery_.empty()) {
		state.SkipWithError("Run Fill before streaming benchmarks");
	}

	const ConstFloatVectorView query{std::span<const float>{streamingQuery_.data(), streamingQuery_.size()}};
	const auto q = [&] { return Query(nsdef_.name).WhereKNN("vec"sv, query, HnswSearchParams{}).Limit(kK); };
	ItemsCounter<KnnParams::K> itemsCounter{state, kK};
	benchQuery(q, state, itemsCounter);
}

#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

static bool GetQuantizationStatus(Reindexer* db, std::string_view nsName, std::string_view indexName) {
	QueryResults qr;
	auto err = db->Select(reindexer::Query("#memstats").Where("name", CondEq, nsName), qr);
	if (!err.ok()) {
		throw err;
	}

	if (qr.Count() != 1) {
		throw Error(errLogic, "Unexpected QueryResults size ({}) for #memstats query", qr.Count());
	}

	auto item = YAML::Load(std::string{(*qr.begin()).GetItem().GetJSON()});
	const auto indexes = item["indexes"];
	for (auto& index : indexes) {
		if (index["name"].as<std::string>() == indexName) {
			return index["is_quantized"].as<bool>();
		}
	}

	throw Error(errLogic, "Info about index {} not found in #memstats", indexName);
}

static void WaitQuantization(Reindexer* db, std::string_view nsName, std::string_view indexName) {
	auto now = std::chrono::milliseconds(0);
	const auto pause = std::chrono::milliseconds(50);
	constexpr auto kMaxSyncTime = std::chrono::seconds(10);
	while (!GetQuantizationStatus(db, nsName, indexName)) {
		now += pause;
		if (now > kMaxSyncTime) {
			throw Error(errLogic, "Wait quantization is too long");
		}
		std::this_thread::sleep_for(pause);
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Sleep(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		std::this_thread::sleep_for(std::chrono::seconds(20));
	}
	if (withQuantization_ == WithQuantization::Yes) {
		WaitQuantization(db_, nsdef_.name, "vec");
	}
}
#endif	// !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::RegisterAllCases() {
	using namespace std::string_literals;

	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Fill"s, &KnnBench<indexType, metric>::Fill, this)->Iterations(1)->UseRealTime();
#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	Register("Sleep"s, &KnnBench<indexType, metric>::Sleep, this)->Iterations(1);
#endif	// !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

	Register("Knn/K"s, &KnnBench<indexType, metric>::Knn<KnnParams::K>, this);
	if constexpr (indexType == IndexType::Hnsw) {
		Register("StreamingKnn/NoFilter"s, &KnnBench<indexType, metric>::StreamingKnnNoFilter, this);
	}

	Register("Knn/K_Radius"s, &KnnBench<indexType, metric>::Knn<KnnParams::K_Radius>, this);
	Register("Knn/Radius"s, &KnnBench<indexType, metric>::Knn<KnnParams::Radius>, this);
	Register("KnnWithVectors/K"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::K>, this);
	Register("KnnWithVectors/K_Radius"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::K_Radius>, this);
	Register("KnnWithVectors/Radius"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::Radius>, this);

	if constexpr (metric == VectorMetric::Cosine) {
		Register("KnnWithCondition/K"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::K>, this);
		if constexpr (indexType == IndexType::Hnsw) {
			Register("StreamingKnn/Tree50pct"s, &KnnBench<indexType, metric>::StreamingKnnTree50pct, this);
			Register("StreamingKnn/Tree50pct/limit10"s, &KnnBench<indexType, metric>::StreamingKnnTree50pctLimit10, this);
		}

		Register("KnnWithCondition/K_Radius"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::K_Radius>, this);
		Register("KnnWithCondition/Radius"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::Radius>, this);
		Register("KnnWith2Conditions/K"s, &KnnBench<indexType, metric>::KnnWith2Conditions<KnnParams::K>, this);
		if constexpr (indexType == IndexType::Hnsw) {
			Register("StreamingKnn/2Cond25pct"s, &KnnBench<indexType, metric>::StreamingKnn2Cond25pct, this);
		}

		Register("KnnWith2Conditions/K_Radius"s, &KnnBench<indexType, metric>::KnnWith2Conditions<KnnParams::K_Radius>, this);
		Register("KnnWith2Conditions/Radius"s, &KnnBench<indexType, metric>::KnnWith2Conditions<KnnParams::Radius>, this);

		Register("InitFtIndex"s, &KnnBench<indexType, metric>::FillFtIndex, this)->Iterations(1);
#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
		Register("Sleep"s, &KnnBench<indexType, metric>::Sleep, this)->Iterations(1);
#endif	// !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

		Register("AndHybridRrf/K"s, &KnnBench<indexType, metric>::AndHybridRrf<KnnParams::K>, this);
		Register("AndHybridRrf/K_Radius"s, &KnnBench<indexType, metric>::AndHybridRrf<KnnParams::K_Radius>, this);
		Register("AndHybridRrf/Radius"s, &KnnBench<indexType, metric>::AndHybridRrf<KnnParams::Radius>, this);
		Register("AndHybridLinear/K"s, &KnnBench<indexType, metric>::AndHybridLinear<KnnParams::K>, this);
		Register("AndHybridLinear/K_Radius"s, &KnnBench<indexType, metric>::AndHybridLinear<KnnParams::K_Radius>, this);
		Register("AndHybridLinear/Radius"s, &KnnBench<indexType, metric>::AndHybridLinear<KnnParams::Radius>, this);

		Register("OrHybridRrf/K"s, &KnnBench<indexType, metric>::OrHybridRrf<KnnParams::K>, this);
		Register("OrHybridRrf/K_Radius"s, &KnnBench<indexType, metric>::OrHybridRrf<KnnParams::K_Radius>, this);
		Register("OrHybridRrf/Radius"s, &KnnBench<indexType, metric>::OrHybridRrf<KnnParams::Radius>, this);
		Register("OrHybridLinear/K"s, &KnnBench<indexType, metric>::OrHybridLinear<KnnParams::K>, this);
		Register("OrHybridLinear/K_Radius"s, &KnnBench<indexType, metric>::OrHybridLinear<KnnParams::K_Radius>, this);
		Register("OrHybridLinear/Radius"s, &KnnBench<indexType, metric>::OrHybridLinear<KnnParams::Radius>, this);

		Register("DeleteFtIndex"s, &KnnBench<indexType, metric>::DeleteFtIndex, this)->Iterations(1);
	}

	Register("Update"s, &KnnBench<indexType, metric>::Update, this);
	Register("Insert"s, &KnnBench<indexType, metric>::Insert, this);
	//  NOLINTEND(*cplusplus.NewDeleteLeaks)
}

template void KnnBench<IndexType::Hnsw, VectorMetric::L2>::RegisterAllCases();
template void KnnBench<IndexType::Hnsw, VectorMetric::Cosine>::RegisterAllCases();
template void KnnBench<IndexType::Hnsw, VectorMetric::InnerProduct>::RegisterAllCases();
template void KnnBench<IndexType::Ivf, VectorMetric::L2>::RegisterAllCases();
template void KnnBench<IndexType::Ivf, VectorMetric::Cosine>::RegisterAllCases();
template void KnnBench<IndexType::Ivf, VectorMetric::InnerProduct>::RegisterAllCases();

}  // namespace reindexer_benchmarks::knn_bench
