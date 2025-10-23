#include "api_tv_composite.h"
#include "allocs_tracker.h"
#include "core/keyvalue/variant.h"
#include "core/query/query.h"
#include "helpers.h"

using benchmark::AllocsTracker;

using reindexer::Query;
using reindexer::Variant;
using namespace std::string_view_literals;

reindexer::Error ApiTvComposite::Initialize() {
	auto err = BaseFixture::Initialize();
	if (!err.ok()) {
		return err;
	}

	names_ = {"ox",	  "ant",  "ape",  "asp",  "bat",  "bee",  "boa",  "bug",  "cat",  "cod",  "cow",  "cub",  "doe",  "dog",
			  "eel",  "eft",  "elf",  "elk",  "emu",  "ewe",  "fly",  "fox",  "gar",  "gnu",  "hen",  "hog",  "imp",  "jay",
			  "kid",  "kit",  "koi",  "lab",  "man",  "owl",  "pig",  "pug",  "pup",  "ram",  "rat",  "ray",  "yak",  "bass",
			  "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo",
			  "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull",
			  "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite",
			  "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole",
			  "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"};
	std::sort(names_.begin(), names_.end());

	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};
	std::sort(locations_.begin(), locations_.end());

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
	const auto q = [&] {
		const auto randId = random<int>(id_seq_->Start(), id_seq_->End());
		const auto randSubId = std::to_string(randId);
		return Query(nsdef_.name).WhereComposite("id+sub_id", CondEq, {{Variant(randId), Variant(randSubId)}});
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeInt(State& state) {
	const auto q = [&] {
		const auto leftYear = random<int>(2000, 2024);
		const auto rightYear = random<int>(2025, 2049);
		return Query(nsdef_.name).Where("year", CondRange, {leftYear, rightYear}).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeStrCollateNumeric(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("sub_id", CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)}).Limit(1);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeDouble(State& state) {
	const auto q = [&] {
		const auto firstRate = random<double>(1, 5);
		const auto secondRate = random<double>(6, 10);
		return Query(nsdef_.name).Where("rate", CondRange, {firstRate, secondRate}).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeCompositeIntInt(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		const auto leftYear = random<int>(2000, 2024);
		const auto rightYear = random<int>(2025, 2049);
		return Query(nsdef_.name)
			.WhereComposite("id+year", CondRange,
							{{Variant(idRange.first), Variant(leftYear)}, {Variant(idRange.second), Variant(rightYear)}})
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeCompositeIntStr(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		const auto leftName = random<size_t>(0, names_.size() - 2);
		const auto rightName = random<size_t>(leftName + 1, names_.size() - 1);
		return Query(nsdef_.name)
			.WhereComposite("id+name", CondRange,
							{{Variant(idRange.first), Variant(names_[leftName])}, {Variant(idRange.second), Variant(names_[rightName])}})
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeHashInt(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("id", CondRange, {idRange.first, idRange.second}).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeHashStringCollateASCII(State& state) {
	assertrx(locations_.size() > 2);
	const auto q = [&] {
		const auto leftLoc = random<size_t>(0, locations_.size() - 2);
		const auto rightLoc = random<size_t>(leftLoc + 1, locations_.size() - 1);
		return Query(nsdef_.name).Where("location", CondRange, {locations_[leftLoc], locations_[rightLoc]}).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeHashStringCollateUTF8(State& state) {
	assertrx(names_.size() > 2);
	const auto q = [&] {
		const auto leftName = random<size_t>(0, names_.size() - 2);
		const auto rightName = random<size_t>(leftName + 1, names_.size() - 1);
		return Query(nsdef_.name).Where("name", CondRange, {names_[leftName], names_[rightName]}).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeHashCompositeIntInt(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		const auto leftStartTime = random<int64_t>(0, 24999);
		const auto rightStartTime = random<int64_t>(25000, 50000);
		return Query(nsdef_.name)
			.WhereComposite("id+start_time", CondRange,
							{{Variant(idRange.first), Variant(leftStartTime)}, {Variant(idRange.second), Variant(rightStartTime)}})
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeHashCompositeIntStr(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		const auto leftGenre = std::to_string(random<int>(0, 24));
		const auto rightGenre = std::to_string(random<int>(25, 49));
		return Query(nsdef_.name)
			.WhereComposite("id+genre", CondRange,
							{{Variant(idRange.first), Variant(leftGenre)}, {Variant(idRange.second), Variant(rightGenre)}})
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeIntSortByHashInt(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("id", CondRange, {idRange.first, idRange.second}).Sort("age"sv, false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeIntSortByTreeInt(State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("id"sv, CondRange, {idRange.first, idRange.second}).Sort("year"sv, false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeStrSortByHashInt(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name)
			.Where("id"sv, CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)})
			.Sort("age"sv, false)
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeStrSortByTreeInt(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name)
			.Where("id"sv, CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)})
			.Sort("year"sv, false)
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeDoubleSortByTreeInt(benchmark::State& state) {
	const auto q = [&] {
		const auto leftRate = random<double>(0.0, 4.99);
		const auto rightRate = random<double>(5.0, 10.0);
		return Query(nsdef_.name).Where("rate"sv, CondRange, {leftRate, rightRate}).Sort("year"sv, false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeDoubleSortByHashInt(benchmark::State& state) {
	const auto q = [&] {
		const auto leftRate = random<double>(0.0, 4.99);
		const auto rightRate = random<double>(5.0, 10.0);
		return Query(nsdef_.name).Where("rate"sv, CondRange, {leftRate, rightRate}).Sort("age"sv, false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeStrSortByHashStrCollateASCII(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name)
			.Where("id"sv, CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)})
			.Sort("location"sv, false)
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::RangeTreeStrSortByHashStrCollateUTF8(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name)
			.Where("id"sv, CondRange, {std::to_string(idRange.first), std::to_string(idRange.second)})
			.Sort("name"sv, false)
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvComposite::SortByHashInt(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::ForcedSortByHashInt(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id"sv, false, {10, 20, 30, 40, 50}).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::ForcedSortWithSecondCondition(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id"sv, false, {10, 20, 30, 40, 50}).Sort("location"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::Query2CondIdSetComposite(benchmark::State& state) {
	const auto q = [&] {
		// Expecting, that force sort will not be applied to idset
		const auto idx = random<unsigned>(0, compositeIdSet_.size() - 1);
		return Query(nsdef_.name)
			.Where("id", CondEq, compositeIdSet_[idx][0].As<int>())
			.WhereComposite("field1+field2", CondSet, compositeIdSet_);
	};
	benchQuery(q, state);
}

void ApiTvComposite::SortByHashStrCollateASCII(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("location"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::SortByHashStrCollateUTF8(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("name"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::SortByHashCompositeIntInt(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id+start_time"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::SortByHashCompositeIntStr(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id+genre"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::SortByTreeCompositeIntInt(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id+year"sv, false).Limit(20);
	benchQuery(q, state);
}

void ApiTvComposite::SortByTreeCompositeIntStrCollateUTF8(benchmark::State& state) {
	const auto q = Query(nsdef_.name).Sort("id+name"sv, false).Limit(20);
	benchQuery(q, state);
}
