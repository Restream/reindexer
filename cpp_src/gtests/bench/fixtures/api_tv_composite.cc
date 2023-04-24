#include "api_tv_composite.h"
#include "core/keyvalue/variant.h"
#include "core/query/query.h"
#include "helpers.h"

using benchmark::AllocsTracker;

using reindexer::Query;
using reindexer::QueryResults;
using reindexer::Variant;

reindexer::Error ApiTvComposite::Initialize() {
	auto err = BaseFixture::Initialize();
	if (!err.ok()) return err;

	names_ = {"ox",	  "ant",  "ape",  "asp",  "bat",  "bee",  "boa",  "bug",  "cat",  "cod",  "cow",  "cub",  "doe",  "dog",
			  "eel",  "eft",  "elf",  "elk",  "emu",  "ewe",  "fly",  "fox",  "gar",  "gnu",  "hen",  "hog",  "imp",  "jay",
			  "kid",  "kit",  "koi",  "lab",  "man",  "owl",  "pig",  "pug",  "pup",  "ram",  "rat",  "ray",  "yak",  "bass",
			  "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo",
			  "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull",
			  "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite",
			  "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole",
			  "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"};

	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	compositeIdSet_.resize(20000);
	int counter = compositeIdSet_.size() * 3;
	for (auto& v : compositeIdSet_) {
		counter = (counter % 2) ? counter - 1 : counter - 3;
		v = VariantArray{Variant{counter}, Variant{counter}};
	}

	return {};
}

reindexer::Item ApiTvComposite::MakeItem(benchmark::State&) {
	auto item = db_->NewItem(nsdef_.name);

	auto startTime = random<int64_t>(0, 50000);

	const auto id = id_seq_->Next();
	item["id"] = id;
	item["name"] = names_.at(random<size_t>(0, names_.size() - 1));
	item["year"] = random<int>(2000, 2049);
	item["rate"] = random<double>(0, 10);
	item["age"] = random<int>(1, 5);
	item["location"] = locations_.at(random<size_t>(0, locations_.size() - 1));
	item["start_time"] = startTime;
	item["end_time"] = startTime + random<int64_t>(1, 5) * 1000;
	item["field1"] = id;
	item["field2"] = id;
	item["genre"] = std::to_string(random<int>(0, 49));
	item["sub_id"] = id_seq_->As<std::string>();

	return item;
}

void ApiTvComposite::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	// Skip BaseFixture::Update

	Register("Insert" + std::to_string(id_seq_->Count()), &ApiTvComposite::Insert, this)->Iterations(1);
	Register("WarmUpIndexes", &ApiTvComposite::WarmUpIndexes, this)->Iterations(1);
	Register("GetByCompositePK", &ApiTvComposite::GetByCompositePK, this);

	// Part I
	Register("RangeTreeInt", &ApiTvComposite::RangeTreeInt, this);
	Register("RangeTreeStrCollateNumeric", &ApiTvComposite::RangeTreeStrCollateNumeric, this);
	Register("RangeTreeDouble", &ApiTvComposite::RangeTreeDouble, this);
	Register("RangeTreeCompositeIntInt", &ApiTvComposite::RangeTreeCompositeIntInt, this);
	Register("RangeTreeCompositeIntStr", &ApiTvComposite::RangeTreeCompositeIntStr, this);

	// Part II
	Register("RangeHashInt", &ApiTvComposite::RangeHashInt, this);
	Register("RangeHashStringCollateASCII", &ApiTvComposite::RangeHashStringCollateASCII, this);
	Register("RangeHashStringCollateUTF8", &ApiTvComposite::RangeHashStringCollateUTF8, this);

	//	The test cases below fall with an error (It's can be fixed in the future)
	Register("RangeHashCompositeIntInt", &ApiTvComposite::RangeHashCompositeIntInt, this);
	Register("RangeHashCompositeIntStr", &ApiTvComposite::RangeHashCompositeIntStr, this);

	// Part III
	Register("RangeTreeIntSortByHashInt", &ApiTvComposite::RangeTreeIntSortByHashInt, this);
	Register("RangeTreeIntSortByTreeInt", &ApiTvComposite::RangeTreeIntSortByTreeInt, this);
	Register("RangeTreeStrSortByHashInt", &ApiTvComposite::RangeTreeStrSortByHashInt, this);
	Register("RangeTreeStrSortByTreeInt", &ApiTvComposite::RangeTreeStrSortByTreeInt, this);
	Register("RangeTreeDoubleSortByTreeInt", &ApiTvComposite::RangeTreeDoubleSortByTreeInt, this);
	Register("RangeTreeDoubleSortByHashInt", &ApiTvComposite::RangeTreeDoubleSortByHashInt, this);
	Register("RangeTreeStrSortByHashStrCollateASCII", &ApiTvComposite::RangeTreeStrSortByHashStrCollateASCII, this);
	Register("RangeTreeStrSortByHashStrCollateUTF8", &ApiTvComposite::RangeTreeStrSortByHashStrCollateUTF8, this);

	// Part IV
	Register("SortByHashInt", &ApiTvComposite::SortByHashInt, this);
	Register("SortByHashStrCollateASCII", &ApiTvComposite::SortByHashStrCollateASCII, this);
	Register("SortByHashStrCollateUTF8", &ApiTvComposite::SortByHashStrCollateUTF8, this);
	//	The test cases below fall with an error ((It's can be fixed in the future))
	Register("SortByHashCompositeIntInt", &ApiTvComposite::SortByHashCompositeIntInt, this);
	Register("SortByHashCompositeIntStr", &ApiTvComposite::SortByHashCompositeIntStr, this);

	Register("SortByTreeCompositeIntInt", &ApiTvComposite::SortByTreeCompositeIntInt, this);
	Register("SortByTreeCompositeIntStrCollateUTF8", &ApiTvComposite::SortByTreeCompositeIntStrCollateUTF8, this);
	Register("ForcedSortByHashInt", &ApiTvComposite::ForcedSortByHashInt, this);
	Register("ForcedSortWithSecondCondition", &ApiTvComposite::ForcedSortWithSecondCondition, this);

	Register("Query2CondIdSetComposite", &ApiTvComposite::Query2CondIdSetComposite, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

void ApiTvComposite::Insert(State& state) { BaseFixture::Insert(state); }

void ApiTvComposite::WarmUpIndexes(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		// Ensure indexes complete build
		WaitForOptimization();
	}
}

void ApiTvComposite::GetByCompositePK(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto randId = random<int>(id_seq_->Start(), id_seq_->End());
		auto randSubId = std::to_string(randId);
		Query q(nsdef_.name);
		q.WhereComposite("id+sub_id", CondEq, {{Variant(randId), Variant(randSubId)}});

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("Results does not contain any value");
	}
}

void ApiTvComposite::RangeTreeInt(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto leftYear = random<int>(2000, 2024);
		auto rightYear = random<int>(2025, 2049);

		Query q(nsdef_.name);
		q.Where("year", CondRange, {leftYear, rightYear}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeStrCollateNumeric(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		q.Where("sub_id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Limit(1);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeDouble(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		auto firstRate = random<double>(1, 5);
		auto secondRate = random<double>(5, 10);

		q.Where("rate", CondRange, {firstRate, secondRate}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (!qres.Count()) state.SkipWithError("empty qres");
	}
}

void ApiTvComposite::RangeTreeCompositeIntInt(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		auto leftYear = random<int>(2000, 2024);
		auto rightYear = random<int>(2025, 2049);

		q.WhereComposite("id+year", CondRange, {{Variant(idRange.first), Variant(leftYear)}, {Variant(idRange.second), Variant(rightYear)}})
			.Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeCompositeIntStr(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		auto randLeftStr = names_.at(random<size_t>(0, names_.size() - 1));
		auto randRightStr = names_.at(random<size_t>(0, names_.size() - 1));

		q.WhereComposite("id+name", CondRange,
						 {{Variant(idRange.first), Variant(randLeftStr)}, {Variant(idRange.second), Variant(randRightStr)}})
			.Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeHashInt(State& state) {
	AllocsTracker AllocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		q.Where("id", CondRange, {idRange.first, idRange.second}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeHashStringCollateASCII(State& state) {
	AllocsTracker AllocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		auto leftLoc = locations_.at(random<size_t>(0, locations_.size() - 1));
		auto rightLoc = locations_.at(random<size_t>(0, locations_.size() - 1));

		q.Where("location", CondRange, {leftLoc, rightLoc}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeHashStringCollateUTF8(State& state) {
	AllocsTracker AllocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto leftName = names_.at(random<size_t>(0, names_.size() - 1));
		auto rightName = names_.at(random<size_t>(0, names_.size() - 1));

		q.Where("name", CondRange, {leftName, rightName}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeHashCompositeIntInt(State& state) {
	AllocsTracker AllocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		auto leftStartTime = random<int64_t>(0, 24999);
		auto rightStartTime = random<int64_t>(25000, 50000);

		q.WhereComposite("id+start_time", CondRange,
						 {{Variant(idRange.first), Variant(leftStartTime)}, {Variant(idRange.second), Variant(rightStartTime)}})
			.Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeHashCompositeIntStr(benchmark::State& state) {
	AllocsTracker AllocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		auto leftGenre = std::to_string(random<int>(0, 24));
		auto rightGenre = std::to_string(random<int>(25, 49));

		q.WhereComposite("id+genre", CondRange,
						 {{Variant(idRange.first), Variant(leftGenre)}, {Variant(idRange.second), Variant(rightGenre)}})
			.Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeIntSortByHashInt(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeIntSortByTreeInt(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {idRange.first, idRange.second}).Sort("year", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeStrSortByHashInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeStrSortByTreeInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Sort("year", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeDoubleSortByTreeInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto leftRate = random<double>(0.0, 4.99);
		auto rightRate = random<double>(5.0, 10.0);

		q.Where("rate", CondRange, {leftRate, rightRate}).Sort("year", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeDoubleSortByHashInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto leftRate = random<double>(0.0, 4.99);
		auto rightRate = random<double>(5.0, 10.0);

		q.Where("rate", CondRange, {leftRate, rightRate}).Sort("age", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeStrSortByHashStrCollateASCII(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Sort("location", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::RangeTreeStrSortByHashStrCollateUTF8(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);

		q.Where("id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Sort("name", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByHashInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::ForcedSortByHashInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id", false, {10, 20, 30, 40, 50}).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::ForcedSortWithSecondCondition(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id", false, {10, 20, 30, 40, 50}).Sort("location", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::Query2CondIdSetComposite(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		// Expecting, that force sort will not be applied to idset
		const auto idx = random<unsigned>(0, compositeIdSet_.size() - 1);
		q.Where("id", CondEq, compositeIdSet_[idx][0].As<int>()).WhereComposite("field1+field2", CondSet, compositeIdSet_);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByHashStrCollateASCII(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("location", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByHashStrCollateUTF8(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("name", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByHashCompositeIntInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id+start_time", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByHashCompositeIntStr(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id+genre", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByTreeCompositeIntInt(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id+year", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}

void ApiTvComposite::SortByTreeCompositeIntStrCollateUTF8(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Sort("id+name", false).Limit(20);

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) state.SkipWithError(err.what().c_str());
	}
}
