#include "geometry.h"
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "gtests/tools.h"

namespace {

constexpr double kRange = 100.0;

}  // namespace

template <size_t N>
void Geometry::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		for (size_t i = 0; i < N; ++i) {
			auto item = MakeItem(state);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
			}

			auto err = db_->Insert(nsdef_.name, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	}
}

template <size_t N>
void Geometry::GetDWithin(benchmark::State& state) {
	const auto q = [&] { return reindexer::Query(nsdef_.name).DWithin("point", randPoint(kRange), kRange / N); };
	LowSelectivityItemsCounter itemsCounter(state);
	benchQuery(q, state, itemsCounter);
}

template <IndexOpts::RTreeIndexType rtreeType>
void Geometry::Reset(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		id_ = 0;
		nsdef_ = reindexer::NamespaceDef(nsdef_.name);
		nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK()).AddIndex("point", "rtree", "point", IndexOpts().RTreeType(rtreeType));

		auto err = db_->DropNamespace(nsdef_.name);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}

		err = db_->AddNamespace(nsdef_);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void Geometry::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
#ifdef REINDEX_WITH_TSAN
	Register("NonIndexPointInsert/10^4", &Geometry::Insert<10000>, this)->Iterations(1);
#else
	Register("NonIndexPointInsert/10^5", &Geometry::Insert<100000>, this)->Iterations(1);
#endif
	Register("NonIndexPointDWithin/1%", &Geometry::GetDWithin<10>, this);
	Register("NonIndexPointDWithin/0.01%", &Geometry::GetDWithin<100>, this);

	Register("ResetToLinear", &Geometry::Reset<IndexOpts::Linear>, this)->Iterations(1);
#ifdef REINDEX_WITH_TSAN
	Register("LinearRTreePointInsert/10^4", &Geometry::Insert<10000>, this)->Iterations(1);
#else
	Register("LinearRTreePointInsert/10^5", &Geometry::Insert<100000>, this)->Iterations(1);
#endif
	Register("LinearRTreePointDWithin/1%", &Geometry::GetDWithin<10>, this);
	Register("LinearRTreePointDWithin/0.01%", &Geometry::GetDWithin<100>, this);

	Register("ResetToQuadratic", &Geometry::Reset<IndexOpts::Quadratic>, this)->Iterations(1);
#ifdef REINDEX_WITH_TSAN
	Register("QuadraticRTreePointInsert/10^4", &Geometry::Insert<10000>, this)->Iterations(1);
#else
	Register("QuadraticRTreePointInsert/10^5", &Geometry::Insert<100000>, this)->Iterations(1);
#endif
	Register("QuadraticRTreePointDWithin/1%", &Geometry::GetDWithin<10>, this);
	Register("QuadraticRTreePointDWithin/0.01%", &Geometry::GetDWithin<100>, this);

	Register("ResetToGreene", &Geometry::Reset<IndexOpts::Greene>, this)->Iterations(1);
#ifdef REINDEX_WITH_TSAN
	Register("GreeneRTreePointInsert/10^4", &Geometry::Insert<10000>, this)->Iterations(1);
#else
	Register("GreeneRTreePointInsert/10^5", &Geometry::Insert<100000>, this)->Iterations(1);
#endif
	Register("GreeneRTreePointDWithin/1%", &Geometry::GetDWithin<10>, this);
	Register("GreeneRTreePointDWithin/0.01%", &Geometry::GetDWithin<100>, this);

	Register("ResetToRStar", &Geometry::Reset<IndexOpts::RStar>, this)->Iterations(1);
#ifdef REINDEX_WITH_TSAN
	Register("RStarRTreePointInsert/10^4", &Geometry::Insert<10000>, this)->Iterations(1);
#else
	Register("RStarRTreePointInsert/10^5", &Geometry::Insert<100000>, this)->Iterations(1);
#endif
	Register("RStarRTreePointDWithin/1%", &Geometry::GetDWithin<10>, this);
	Register("RStarRTreePointDWithin/0.01%", &Geometry::GetDWithin<100>, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error Geometry::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}

	return {};
}

reindexer::Item Geometry::MakeItem(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be holded by app
	std::ignore = item.Unsafe();

	wrSer_.Reset();
	reindexer::JsonBuilder bld(wrSer_);
	bld.Put("id", id_++);
	const reindexer::Point point = randPoint(kRange);
	bld.Array("point", {point.X(), point.Y()});
	bld.End();
	const auto err = item.FromJSON(wrSer_.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}

	return item;
}
