#include "base_fixture.h"

#include <benchmark/benchmark.h>
#include <string>
#include <thread>
#include "allocs_tracker.h"
#include "core/system_ns_names.h"

reindexer::Error BaseFixture::Initialize() {
	assertrx(db_);
	return db_->AddNamespace(nsdef_);
}

void BaseFixture::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert" + std::to_string(id_seq_->Count()), &BaseFixture::Insert, this)->Iterations(1);
	Register("Update", &BaseFixture::Update, this)->Iterations(id_seq_->Count());
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

std::string BaseFixture::RandString() {
	std::string res;
	uint8_t len = rand() % 20 + 4;
	res.resize(len);
	for (int i = 0; i < len; ++i) {
		int f = rand() % letters.size();
		res[i] = letters[f];
	}
	return res;
}

// FIXTURES

void BaseFixture::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		for (int i = 0; i < id_seq_->Count(); ++i) {
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
}

void BaseFixture::Update(benchmark::State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	id_seq_->Reset();
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeItem(state);
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}

		auto err = db_->Update(nsdef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}

		if (item.GetID() < 0) {
			auto e = reindexer::Error(errConflict, "Item not exists [id = '{}']", item["id"].As<int>());
			state.SkipWithError(e.what());
		}
		state.SetItemsProcessed(state.items_processed() + 1);
	}
}

void BaseFixture::WaitForOptimization() {
	reindexer::Query q(reindexer::kMemStatsNamespace);
	q.Where("name", CondEq, nsdef_.name);
	for (;;) {
		reindexer::QueryResults res;
		auto e = db_->Select(q, res);
		assertrx(e.ok());
		assertrx(res.Count() == 1);
		assertrx(res.IsLocal());
		auto item = res.ToLocalQr().begin().GetItem(false);
		if (item["optimization_completed"].As<bool>() == true) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
}
