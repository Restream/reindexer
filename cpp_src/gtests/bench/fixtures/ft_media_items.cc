#include "ft_media_items.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <tuple>
#include <utility>

#include "allocs_tracker.h"
#include "core/ft/config/ftconfig.h"
#include "helpers.h"
#include "tools/stringstools.h"

namespace reindexer_benchmarks {

using reindexer::IndexOpts;
using reindexer::Query;
using reindexer::QueryResults;

namespace {

constexpr uint32_t kMergeLimit = 40'000;

bool isQueryWord(std::u16string_view word) noexcept {
	if (word.size() < 5 || word.size() > 12) {
		return false;
	}
	return std::all_of(word.begin(), word.end(), [](char16_t ch) noexcept {
		return (ch >= u'А' && ch <= u'Я') || (ch >= u'а' && ch <= u'я') || ch == u'Ё' || ch == u'ё';
	});
}

}  // namespace

FullTextMediaItems::FullTextMediaItems(Reindexer* db, std::string_view name, size_t maxItems)
	: BaseFixture(db, name, maxItems, 0), FullTextBase{} {
	reindexer::FTConfig ftCfg(8);
	ftCfg.mergeLimit = kMergeLimit;
	ftCfg.optimization = reindexer::FTConfig::Optimization::Memory;
	ftCfg.stopWords.clear();

	IndexOpts ftOpts = IndexOpts().SetConfig(IndexCompositeFastFT, ftCfg.GetJSON({}));
	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("small_text_field1", "-", "string", IndexOpts())
		.AddIndex("small_text_field2", "-", "string", IndexOpts())
		.AddIndex("small_text_field3", "-", "string", IndexOpts())
		.AddIndex("small_text_array_field1", "-", "string", IndexOpts().Array())
		.AddIndex("small_text_array_field2", "-", "string", IndexOpts().Array())
		.AddIndex("small_text_array_field3", "-", "string", IndexOpts().Array())
		.AddIndex("small_text_array_field4", "-", "string", IndexOpts().Array())
		.AddIndex("small_text_array_field5", "-", "string", IndexOpts().Array())
		.AddIndex(kIndexName_,
				  {"small_text_field1", "small_text_field2", "small_text_field3", "small_text_array_field1", "small_text_array_field2",
				   "small_text_array_field3", "small_text_array_field4", "small_text_array_field5"},
				  "text", "composite", std::move(ftOpts));
}

reindexer::Error FullTextMediaItems::Initialize() {
	auto err = FullTextBase::Initialize();
	if (!err.ok()) {
		return err;
	}
	err = BaseFixture::Initialize();
	if (!err.ok()) {
		return err;
	}

	queryWords_.reserve(kQueryWordsCount);
	while (queryWords_.size() < kQueryWordsCount) {
		const std::string& candidate = RndWord1();
		if (isQueryWord(reindexer::utf8_to_utf16(candidate)) &&
			std::find(queryWords_.begin(), queryWords_.end(), candidate) == queryWords_.end()) {
			queryWords_.emplace_back(candidate);
		}
	}
	return {};
}

void FullTextMediaItems::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &FullTextMediaItems::Insert, this)->Iterations(1)->Unit(benchmark::kMicrosecond);
	Register("Build", &FullTextMediaItems::Build, this)->Iterations(1)->Unit(benchmark::kMicrosecond);
	Register("SelectOneWord", &FullTextMediaItems::SelectOneWord, this)
		->Threads(kSelectThreads)
		->MinTime(kSelectMinTime)
		->Unit(benchmark::kMicrosecond);
	Register("SelectTwoWords", &FullTextMediaItems::SelectTwoWords, this)
		->Threads(kSelectThreads)
		->MinTime(kSelectMinTime)
		->Unit(benchmark::kMicrosecond);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Item FullTextMediaItems::MakeItem(benchmark::State&) {
	auto item = db_->NewItem(nsdef_.name);
	if (!item.Status().ok()) {
		return item;
	}
	std::ignore = item.Unsafe(false);

	const int id = id_seq_->Next();
	// Field presence and word counts approximate the eight fields of the media_items fulltext index.
	// Fixed offsets place every query word into at least 90'000 distinct documents at this dataset size.
	item["id"] = id;
	item["small_text_field1"] = makeText(size_t(RndInt(2, 4)), id);
	if (RndInt(0, 99) < 36) {
		item["small_text_field2"] = RndFrom(queryWords_);
	}
	item["small_text_field3"] = std::to_string(1980 + id % 50);
	if (id % 100 < 69) {
		item["small_text_array_field1"] =
			toArray<std::string>(std::vector<std::string>{queryWords_[(size_t(id) + 4) % queryWords_.size()]});
	}
	if (RndInt(0, 99) < 12) {
		item["small_text_array_field2"] = toArray<std::string>(makeWords(size_t(RndInt(12, 17))));
	}
	if (RndInt(0, 99) < 6) {
		item["small_text_array_field3"] = toArray<std::string>(makeWords(size_t(RndInt(9, 10))));
	}
	if (RndInt(0, 99) < 7) {
		item["small_text_array_field4"] = toArray<std::string>(makeWords(size_t(RndInt(5, 6))));
	}
	if (id % 100 < 93) {
		std::vector<std::string> values;
		values.reserve(3);
		const size_t valuesCount = size_t(RndInt(1, 3));
		values.emplace_back(queryWords_[(size_t(id) + 8) % queryWords_.size()]);
		while (values.size() < valuesCount) {
			values.emplace_back(RndFrom(queryWords_));
		}
		item["small_text_array_field5"] = toArray<std::string>(values);
	}

	return item;
}

void FullTextMediaItems::Insert(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		id_seq_->Reset();
		for (size_t begin = 0; begin < size_t(id_seq_->Count()); begin += kInsertBatchSize) {
			auto tx = db_->NewTransaction(nsdef_.name);
			if (!tx.Status().ok()) {
				state.SkipWithError(tx.Status().what());
				return;
			}

			const size_t end = std::min(begin + kInsertBatchSize, size_t(id_seq_->Count()));
			for (size_t i = begin; i < end; ++i) {
				auto item = MakeItem(state);
				if (!item.Status().ok()) {
					state.SkipWithError(item.Status().what());
					return;
				}
				auto err = tx.Insert(std::move(item));
				if (!err.ok()) {
					state.SkipWithError(err.what());
					return;
				}
			}

			QueryResults qr;
			auto err = db_->CommitTransaction(tx, qr);
			if (!err.ok()) {
				state.SkipWithError(err.what());
				return;
			}
			state.SetItemsProcessed(state.items_processed() + (end - begin));
		}
	}
	state.SetLabel("inserted " + std::to_string(id_seq_->Count()) + " documents");
}

void FullTextMediaItems::Build(benchmark::State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		QueryResults qr;
		auto err = db_->Select(Query(nsdef_.name).Where(kIndexName_, CondEq, queryWords_.front() + "*~").Limit(20), qr);
		if (!err.ok()) {
			state.SkipWithError(err.what());
			return;
		}
	}
	state.SetLabel("merge_limit " + std::to_string(kMergeLimit));
}

void FullTextMediaItems::SelectOneWord(benchmark::State& state) { select(state, QueryTerms::One); }

void FullTextMediaItems::SelectTwoWords(benchmark::State& state) { select(state, QueryTerms::Two); }

void FullTextMediaItems::select(benchmark::State& state, QueryTerms queryTerms) {
	size_t iteration = 0;
	size_t resultsCount = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const size_t wordIndex = (iteration++ * kSelectThreads + size_t(state.thread_index())) % queryWords_.size();
		std::string ftQuery = queryWords_[wordIndex] + "*~";
		switch (queryTerms) {
			case QueryTerms::One:
				break;
			case QueryTerms::Two:
				ftQuery += ' ';
				ftQuery += queryWords_[(wordIndex + 1) % queryWords_.size()];
				ftQuery += "*~";
				break;
		}

		QueryResults qr;
		auto err = db_->Select(Query(nsdef_.name).Where(kIndexName_, CondEq, std::move(ftQuery)), qr);
		if (!err.ok()) {
			state.SkipWithError(err.what());
			return;
		}
		resultsCount += qr.Count();
	}
	state.counters["Results/Op"] = benchmark::Counter(resultsCount / double(state.iterations()), benchmark::Counter::kAvgThreads);
}

std::string FullTextMediaItems::makeText(size_t wordsCount, int id) {
	std::string text;
	for (size_t i = 0; i < wordsCount; ++i) {
		if (!text.empty()) {
			text += ' ';
		}
		text += i < 2 ? queryWords_[(size_t(id) + i) % queryWords_.size()] : RndWord1();
	}
	return text;
}

std::vector<std::string> FullTextMediaItems::makeWords(size_t wordsCount) {
	std::vector<std::string> words;
	words.reserve(wordsCount);
	for (size_t i = 0; i < wordsCount; ++i) {
		words.emplace_back(RndWord1());
	}
	return words;
}

}  // namespace reindexer_benchmarks
