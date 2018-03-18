#include "base_fixture.h"

#include <benchmark/benchmark.h>
#include <functional>
#include <random>
#include <string>

using std::string;

using reindexer::Error;

using benchmark::RegisterBenchmark;

BaseFixture& BaseFixture::AddIndex(const std::string& name, const std::string& jsonPath, const std::string& indexType,
								   const std::string& fieldType, IndexOpts opts) {
	nsdef_.AddIndex(name, jsonPath, indexType, fieldType, opts);
	return *this;
}

BaseFixture& BaseFixture::AddIndex(const reindexer::IndexDef& index) {
	nsdef_.AddIndex(index);
	return *this;
}

Error BaseFixture::Initialize() {
	assert(db_);
	return db_->AddNamespace(nsdef_);
}

void BaseFixture::RegisterAllCases() {
	Register("Insert", &BaseFixture::Insert, this)->Iterations(id_seq_->Count());
	Register("Update", &BaseFixture::Update, this)->Iterations(id_seq_->Count());
}

// FIXTURES

void BaseFixture::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {
		auto item = MakeItem();
		if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());

		auto err = db_->Insert(nsdef_.name, item);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		state.SetItemsProcessed(state.items_processed() + 1);
	}

	auto err = db_->Commit(nsdef_.name);
	if (!err.ok()) state.SkipWithError(err.what().c_str());
}

void BaseFixture::Update(benchmark::State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	id_seq_->Reset();
	for (auto _ : state) {
		auto item = MakeItem();
		if (!item.Status().ok()) state.SkipWithError(item.Status().what().c_str());

		auto err = db_->Update(nsdef_.name, item);
		if (!err.ok()) state.SkipWithError(err.what().c_str());

		if (item.GetID() < 0) {
			auto e = Error(errConflict, "Item not exists [id = '%d']", item["id"].As<int>());
			state.SkipWithError(e.what().c_str());
		}

		state.SetItemsProcessed(state.items_processed() + 1);
	}
	auto err = db_->Commit(nsdef_.name);
	if (!err.ok()) state.SkipWithError(err.what().c_str());
}
