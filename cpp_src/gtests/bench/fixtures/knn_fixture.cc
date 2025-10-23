#include "knn_fixture.h"
#include <thread>
#include "allocs_tracker.h"
#include "core/ft/config/ftfastconfig.h"
#include "gtests/tools.h"

namespace reindexer::knn_bench {

using namespace std::string_view_literals;

static constexpr int kMaxValueOrTreeIndex = 1'000'000;

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
static constexpr size_t kDimention = 256;
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
	return 1.47e14f;
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::InnerProduct>() noexcept {
	return 1.0e13f;
}
template <>
consteval float radius<IndexType::Hnsw, VectorMetric::Cosine>() noexcept {
	return 1.1e-1f;
}

template <>
consteval float radius<IndexType::Ivf, VectorMetric::L2>() noexcept {
	return 1.62e14f;
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::InnerProduct>() noexcept {
	return 1.45e12f;
}
template <>
consteval float radius<IndexType::Ivf, VectorMetric::Cosine>() noexcept {
	return 1.5e-2f;
}
#endif	// defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN) || defined(RX_WITH_STDLIB_DEBUG)

template <IndexType indexType, VectorMetric metric>
template <KnnParams knnParams>
class [[nodiscard]] KnnBench<indexType, metric>::ItemsCounter : private LowSelectivityItemsCounter<> {
	using Base = LowSelectivityItemsCounter<>;

public:
	using Base::Base;
	~ItemsCounter() {
		state_.counters["Items/Op"] = itemsCounter_ / state_.iterations();
		if constexpr (knnParams != KnnParams::Radius) {
			state_.counters["K"] = kK;
		}
	}
	void operator()(const QueryResults& qres) noexcept {
		Base::operator()(qres);
		itemsCounter_ += qres.Count();
	}

private:
	size_t itemsCounter_{0};
};

template <IndexType indexType, VectorMetric metric>
KnnBench<indexType, metric>::KnnBench(Reindexer* db, std::string_view name)
	: BaseFixture(db, name, kNsSize * 3), bench::FullTextBase{kWordsInFtIndex} {
	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK());
	nsdef_.AddIndex("tree", "tree", "int", IndexOpts());
	switch (indexType) {
		case IndexType::Hnsw:
			nsdef_.AddIndex("vec", "hnsw", "float_vector",
							IndexOpts().SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																	  .SetDimension(kDimention)
																	  .SetMetric(metric)
																	  .SetM(16)
																	  .SetEfConstruction(200)
																	  .SetMultithreading(MultithreadingMode::MultithreadTransactions)
																	  .SetStartSize(kNsSize)));
			break;
		case IndexType::Ivf:
			nsdef_.AddIndex("vec", "ivf", "float_vector",
							IndexOpts().SetFloatVector(
								IndexIvf, FloatVectorIndexOpts{}.SetDimension(kDimention).SetMetric(metric).SetNCentroids(kNsSize / 100)));
			break;
	}
	IndexOpts ftIndexOpts;
	ftIndexOpts.SetConfig(IndexFastFT, FtFastConfig{1}.GetJSON({}));
	nsdef_.AddIndex("ft", "text", "string", ftIndexOpts);
}

template KnnBench<IndexType::Hnsw, VectorMetric::L2>::KnnBench(Reindexer*, std::string_view);
template KnnBench<IndexType::Hnsw, VectorMetric::Cosine>::KnnBench(Reindexer*, std::string_view);
template KnnBench<IndexType::Hnsw, VectorMetric::InnerProduct>::KnnBench(Reindexer*, std::string_view);
template KnnBench<IndexType::Ivf, VectorMetric::L2>::KnnBench(Reindexer*, std::string_view);
template KnnBench<IndexType::Ivf, VectorMetric::Cosine>::KnnBench(Reindexer*, std::string_view);
template KnnBench<IndexType::Ivf, VectorMetric::InnerProduct>::KnnBench(Reindexer*, std::string_view);

template <IndexType indexType, VectorMetric metric>
Item KnnBench<indexType, metric>::MakeItem(State& state) {
	return MakeItem(state, id_seq_->Next());
}

template <IndexType indexType, VectorMetric metric>
Item KnnBench<indexType, metric>::MakeItem(State&, int id) {
	Item item = db_->NewItem(nsdef_.name);
	if (item.Status().ok()) {
		item["id"sv] = id;
		item["tree"sv] = rand() % kMaxValueOrTreeIndex;
		static std::array<float, kDimention> vect;
		rndFloatVector(vect);
		item["vec"sv] = ConstFloatVectorView{vect};
	}
	return item;
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Fill(State& state) {
	benchmark::AllocsTracker allocsTracker(state);

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
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
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
	benchmark::AllocsTracker allocsTracker(state);
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
	benchmark::AllocsTracker allocsTracker(state);
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
	benchmark::AllocsTracker allocsTracker(state);
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
		rndFloatVector(vect);
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
		rndFloatVector(vect);
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
		rndFloatVector(vect);
		return Query(nsdef_.name)
			.Where("tree"sv, CondGt, rand() % (kMaxValueOrTreeIndex / 2))
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
		rndFloatVector(vect);
		return Query(nsdef_.name)
			.Where("id"sv, CondGt, rand() % halfOfItemsCount)
			.Where("tree"sv, CondGt, rand() % (kMaxValueOrTreeIndex / 2))
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
		rndFloatVector(vect);
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
		rndFloatVector(vect);
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
		rndFloatVector(vect);
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
		rndFloatVector(vect);
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
void KnnBench<indexType, metric>::Sleep(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		std::this_thread::sleep_for(std::chrono::seconds(20));
	}
}

template <IndexType indexType, VectorMetric metric>
void KnnBench<indexType, metric>::RegisterAllCases() {
	using namespace std::string_literals;

	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Fill"s, &KnnBench<indexType, metric>::Fill, this)->Iterations(1)->UseRealTime();
#if !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)
	Register("Sleep"s, &KnnBench<indexType, metric>::Sleep, this)->Iterations(1);
#endif	// !defined(RX_WITH_STDLIB_DEBUG) && !defined(REINDEX_WITH_ASAN) && !defined(REINDEX_WITH_TSAN)

	Register("Knn/K"s, &KnnBench<indexType, metric>::Knn<KnnParams::K>, this);
	Register("Knn/K_Radius"s, &KnnBench<indexType, metric>::Knn<KnnParams::K_Radius>, this);
	Register("Knn/Radius"s, &KnnBench<indexType, metric>::Knn<KnnParams::Radius>, this);
	Register("KnnWithVectors/K"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::K>, this);
	Register("KnnWithVectors/K_Radius"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::K_Radius>, this);
	Register("KnnWithVectors/Radius"s, &KnnBench<indexType, metric>::KnnWithVectors<KnnParams::Radius>, this);
	Register("KnnWithCondition/K"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::K>, this);
	Register("KnnWithCondition/K_Radius"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::K_Radius>, this);
	Register("KnnWithCondition/Radius"s, &KnnBench<indexType, metric>::KnnWithCondition<KnnParams::Radius>, this);
	Register("KnnWith2Conditions/K"s, &KnnBench<indexType, metric>::KnnWith2Conditions<KnnParams::K>, this);
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

}  // namespace reindexer::knn_bench
