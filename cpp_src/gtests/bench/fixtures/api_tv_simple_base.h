#pragma once

#include "base_fixture.h"

class ApiTvSimpleBase : protected BaseFixture {
	using Base = BaseFixture;

public:
	~ApiTvSimpleBase() override = default;
	ApiTvSimpleBase(Reindexer* db, std::string_view name, size_t maxItems, std::string_view stringSelectNs)
		: Base(db, name, maxItems), stringSelectNs_{stringSelectNs} {}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

protected:
	constexpr static int kPriceIdStart = 7'000;
	constexpr static int kPriceIdRegion = 50;

	ApiTvSimpleBase* BasePtr() & noexcept { return this; }

	void GetEqInt(State&);
	void GetEqArrayInt(State&);
	void GetEqString(State&);
	void GetUuidStr(State&);

	template <typename Total>
	void Query1Cond(State&);
	template <typename Total>
	void Query2Cond(State&);
	template <typename Total>
	void Query3Cond(State&);
	template <typename Total>
	void Query4Cond(State&);
	template <typename Total>
	void Query4CondRange(State&);
	void StringsSelect(State&);
	void GetByRangeIDAndSortByHash(State&);
	void GetByRangeIDAndSortByTree(State&);
	void Query3CondKillIdsCache(State&);
	void Query3CondRestoreIdsCache(State&);

	std::string stringSelectNs_;
	std::vector<std::vector<int>> packages_;
	std::vector<int> start_times_;
	std::vector<std::vector<int>> priceIDs_;
	std::vector<std::string> countries_;
	std::vector<std::string> uuids_;
	std::vector<std::string> locations_;
	std::vector<std::string> devices_;
};
