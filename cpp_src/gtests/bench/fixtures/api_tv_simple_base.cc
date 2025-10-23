#include "api_tv_simple_base.h"
#include "gtests/tools.h"

using reindexer::Query;

reindexer::Error ApiTvSimpleBase::Initialize() {
	assertrx(db_);
	for (int i = 0; i < 10; ++i) {
		packages_.emplace_back(randomNumArray<int>(20, 10'000, 10));
	}
	start_times_.resize(20);
	for (int i = 0; i < 20; ++i) {
		start_times_[i] = random<int>(0, 50'000);
	}
	for (int i = 0; i < 20; ++i) {
		priceIDs_.emplace_back(randomNumArray<int>(10, kPriceIdStart, kPriceIdRegion));
	}
	uuids_.reserve(1'000);
	for (size_t i = 0; i < 1'000; ++i) {
		uuids_.emplace_back(randStrUuid());
	}
	devices_ = {"iphone", "android", "smarttv", "stb", "ottstb"};
	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};
	countries_ = {"Portugal",
				  "Afghanistan",
				  "El Salvador",
				  "Bolivia",
				  "Angola",
				  "Finland",
				  "Wales",
				  "Kosovo",
				  "Poland",
				  "Iraq",
				  "United States of America",
				  "Croatia"};

	return db_->AddNamespace(nsdef_);
}

void ApiTvSimpleBase::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Query1Cond", &ApiTvSimpleBase::Query1Cond<NoTotal>, BasePtr());
	Register("Query1CondTotal", &ApiTvSimpleBase::Query1Cond<ReqTotal>, BasePtr());
	Register("Query1CondCachedTotal", &ApiTvSimpleBase::Query1Cond<CachedTotal>, BasePtr());
	//  NOLINTEND(*cplusplus.NewDeleteLeaks)
}

void ApiTvSimpleBase::GetEqInt(benchmark::State& state) {
	const auto q = [&] {
		return Query(nsdef_.name).Where("start_time", CondEq, start_times_.at(random<size_t>(0, start_times_.size() - 1)));
	};
	benchQuery(q, state);
}

void ApiTvSimpleBase::GetEqArrayInt(benchmark::State& state) {
	const auto q = [&] { return Query(nsdef_.name).Where("price_id", CondEq, priceIDs_[random<size_t>(0, priceIDs_.size() - 1)]); };
	benchQuery(q, state);
}

void ApiTvSimpleBase::GetEqString(benchmark::State& state) {
	const auto q = [&] { return Query(nsdef_.name).Where("countries", CondEq, countries_[random<size_t>(0, countries_.size() - 1)]); };
	benchQuery(q, state);
}

void ApiTvSimpleBase::GetUuidStr(benchmark::State& state) {
	const auto& uuid = uuids_[rand() % uuids_.size()];
	const auto q = Query(nsdef_.name).Where("uuid_str", CondEq, uuid);
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimpleBase::Query1Cond(benchmark::State& state) {
	auto q = Query(nsdef_.name).Where("year", CondGe, 2020).Limit(20);
	Total::Apply(q);
	benchQuery(q, state);
}

template <typename Total>
void ApiTvSimpleBase::Query2Cond(benchmark::State& state) {
	auto q = Query(nsdef_.name).Where("genre", CondEq, 5).Where("year", CondRange, {2010, 2016}).Limit(20);
	Total::Apply(q);
	benchQuery(q, state);
}
template void ApiTvSimpleBase::Query2Cond<BaseFixture::NoTotal>(State&);
template void ApiTvSimpleBase::Query2Cond<BaseFixture::ReqTotal>(State&);
template void ApiTvSimpleBase::Query2Cond<BaseFixture::CachedTotal>(State&);

template <typename Total>
void ApiTvSimpleBase::Query3Cond(benchmark::State& state) {
	const auto q = [&] {
		const size_t randomPackage = random<size_t>(0, packages_.size() - 1);

		auto q = Query(nsdef_.name)
					 .Where("genre", CondEq, 5)
					 .Where("year", CondRange, {2010, 2016})
					 .Where("packages", CondSet, packages_.at(randomPackage))
					 .Sort("year", false)
					 .Limit(20);
		Total::Apply(q);
		return q;
	};
	benchQuery(q, state);
}
template void ApiTvSimpleBase::Query3Cond<BaseFixture::NoTotal>(State&);
template void ApiTvSimpleBase::Query3Cond<BaseFixture::ReqTotal>(State&);
template void ApiTvSimpleBase::Query3Cond<BaseFixture::CachedTotal>(State&);

template <typename Total>
void ApiTvSimpleBase::Query4Cond(benchmark::State& state) {
	const auto q = [&] {
		const size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		auto q = Query(nsdef_.name)
					 .Where("genre", CondEq, 5)
					 .Where("age", CondEq, 2)
					 .Where("year", CondRange, {2010, 2016})
					 .Where("packages", CondSet, packages_.at(randomIndex))
					 .Sort("year", false)
					 .Limit(20);
		Total::Apply(q);
		return q;
	};
	benchQuery(q, state);
}
template void ApiTvSimpleBase::Query4Cond<BaseFixture::NoTotal>(State&);
template void ApiTvSimpleBase::Query4Cond<BaseFixture::ReqTotal>(State&);
template void ApiTvSimpleBase::Query4Cond<BaseFixture::CachedTotal>(State&);

template <typename Total>
void ApiTvSimpleBase::Query4CondRange(benchmark::State& state) {
	const auto q = [&] {
		const int startTime = random<int>(0, 30000);
		const int endTime = startTime + 10000;

		auto q = Query(nsdef_.name)
					 .Where("genre", CondEq, 5)
					 .Where("year", CondRange, {2010, 2016})
					 .Where("start_time", CondGt, startTime)
					 .Where("end_time", CondLt, endTime)
					 .Sort("year", false)
					 .Limit(20);
		Total::Apply(q);
		return q;
	};
	benchQuery(q, state);
}
template void ApiTvSimpleBase::Query4CondRange<BaseFixture::NoTotal>(State&);
template void ApiTvSimpleBase::Query4CondRange<BaseFixture::ReqTotal>(State&);
template void ApiTvSimpleBase::Query4CondRange<BaseFixture::CachedTotal>(State&);

void ApiTvSimpleBase::GetByRangeIDAndSortByHash(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("id", CondRange, {idRange.first, idRange.second}).Sort("age", false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvSimpleBase::GetByRangeIDAndSortByTree(benchmark::State& state) {
	const auto q = [&] {
		const auto idRange = id_seq_->GetRandomIdRange(id_seq_->Count() * 0.02);
		return Query(nsdef_.name).Where("id", CondRange, {idRange.first, idRange.second}).Sort("genre", false).Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvSimpleBase::StringsSelect(benchmark::State& state) { benchQuery(Query{stringSelectNs_}, state); }

void ApiTvSimpleBase::Query3CondKillIdsCache(benchmark::State& state) {
	const auto q = [&] {
		return Query(nsdef_.name)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, randomNumArray<int>(20, 10000, 10))
			.Sort("year", false)
			.Limit(20);
	};
	benchQuery(q, state);
}

void ApiTvSimpleBase::Query3CondRestoreIdsCache(benchmark::State& state) {
	const auto q = [&] {
		const size_t randomIndex = random<size_t>(0, packages_.size() - 1);
		return Query(nsdef_.name)
			.Where("genre", CondEq, 5)
			.Where("year", CondRange, {2010, 2016})
			.Where("packages", CondSet, packages_.at(randomIndex))
			.Sort("year", false)
			.Limit(20);
	};
	benchQuery(q, state);
}
