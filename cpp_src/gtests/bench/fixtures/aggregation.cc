#include "aggregation.h"
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"

template <size_t N>
void Aggregation::Insert(State& state) {
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

void Aggregation::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &Aggregation::Insert<100000>, this)->Iterations(1);
	Register("Facet", &Aggregation::Facet, this);
	Register("MultiFacet", &Aggregation::MultiFacet, this);
	Register("ArrayFacet", &Aggregation::ArrayFacet, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error Aggregation::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}
	return {};
}

reindexer::Item Aggregation::MakeItem(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	// All strings passed to item must be holded by app
	std::ignore = item.Unsafe();

	wrSer_.Reset();
	reindexer::JsonBuilder bld(wrSer_);
	const auto id = id_++;
	bld.Put("id", id);
	bld.Put("int_data", rand() % 100);
	bld.Put("str_data", RandString());
	auto arr = bld.Array("int_array_data");
	for (size_t i = 0, s = rand() % 100 + 100; i < s; ++i) {
		arr.Put(reindexer::TagName::Empty(), rand() % 1000);
	}
	arr.End();
	bld.End();
	const auto err = item.FromJSON(wrSer_.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
	return item;
}

class [[nodiscard]] Aggregation::FacetNotEmptyChecker {
public:
	FacetNotEmptyChecker(State& state) noexcept : state_{state} {}

	void operator()(reindexer::QueryResults& qres) {
		const auto& aggRes = qres.GetAggregationResults();
		if (aggRes.empty() || aggRes[0].GetFacets().empty()) [[unlikely]] {
			state_.SkipWithError("Results does not contain any value");
		}
	}

private:
	State& state_;
};

void Aggregation::Facet(State& state) {
	const auto q = reindexer::Query(nsdef_.name).Aggregate(AggFacet, {"int_data"});
	FacetNotEmptyChecker facetNotEmptyChecker{state};
	benchQuery(q, state, facetNotEmptyChecker);
}

void Aggregation::MultiFacet(State& state) {
	const auto q = reindexer::Query(nsdef_.name).Aggregate(AggFacet, {"int_data", "str_data"});
	FacetNotEmptyChecker facetNotEmptyChecker{state};
	benchQuery(q, state, facetNotEmptyChecker);
}

void Aggregation::ArrayFacet(State& state) {
	const auto q = reindexer::Query(nsdef_.name).Aggregate(AggFacet, {"int_array_data"});
	FacetNotEmptyChecker facetNotEmptyChecker{state};
	benchQuery(q, state, facetNotEmptyChecker);
}
