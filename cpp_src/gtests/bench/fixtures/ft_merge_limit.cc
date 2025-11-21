#include "ft_merge_limit.h"
#include "allocs_tracker.h"

static uint8_t printFlags = benchmark::AllocsTracker::kPrintAllocs | benchmark::AllocsTracker::kPrintHold;

FullTextMergeLimit::FullTextMergeLimit(Reindexer* db, const std::string& name, size_t maxItems)
	: BaseFixture(db, name, maxItems, 1, false) {
#ifdef REINDEX_FT_EXTRA_DEBUG
	std::cout << "!!!REINDEXER WITH FT_EXTRA_DEBUG FLAG!!!!!" << std::endl;
#endif
	static reindexer::FtFastConfig ftCfg(1);
	static IndexOpts ftIndexOpts;
	ftCfg.optimization = reindexer::FtFastConfig::Optimization::Memory;
	ftCfg.stopWords = {};
	ftCfg.splitOptions.SetSymbols("1234567890", "");
	ftCfg.mergeLimit = 0x1FFFFFF;
	ftCfg.maxTypos = 0;
	ftIndexOpts.SetConfig(IndexFastFT, ftCfg.GetJSON({}));
	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK()).AddIndex(kFastIndexTextName_, "text", "string", ftIndexOpts);
}

void FullTextMergeLimit::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &FullTextMergeLimit::Insert, this)->Iterations(1)->Unit(benchmark::kMicrosecond);
	Register("Build", &FullTextMergeLimit::BuildFastTextIndex, this)->Iterations(1)->Unit(benchmark::kMicrosecond);
	auto Select = [this](benchmark::State& state, const std::string& q) { this->FastTextIndexSelect(state, q); };
	for (unsigned i = 0; i < kWords_.size(); i++) {
		RegisterF("SelectOneWord" + std::to_string((i + 1) * 10) + "percent", Select, "=" + kWords_[i])
			->Iterations(1)
			->Unit(benchmark::kMicrosecond);
	}
	for (unsigned i = 0; i < kWords_.size() - 1; i++) {
		RegisterF("SelectTwoWord" + std::to_string((i * 2 + 3) * 10), Select, "=" + kWords_[i] + " =" + kWords_[i + 1])
			->Iterations(1)
			->Unit(benchmark::kMicrosecond);
	}
	for (int i = int(kWords_.size() - 1); i >= 0; i--) {
		RegisterF("SelectOneWordAsterisk" + std::to_string(100 - (i + 1) * 10) + "percent", Select, kWords_[i] + kEndWord + "*")
			->Iterations(1)
			->Unit(benchmark::kMicrosecond);
	}
	for (int i = int(kWords_.size() - 1); i > 0; i--) {
		RegisterF("SelectTwoWordasterisk" + std::to_string(100 - (i * 2 + 3) * 10), Select,
				  kWords_[i] + kEndWord + "*" + " " + kWords_[i - 1] + kEndWord + "*")
			->Iterations(1)
			->Unit(benchmark::kMicrosecond);
	}
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

std::unordered_set<int> FullTextMergeLimit::generateDistrib(int count) {
	std::unordered_set<int> vals;
	std::random_device r;
	std::default_random_engine e1(r());
	std::uniform_int_distribution<int> uniform_dist(0, id_seq_->Count() - 1);

	for (int i = 0; i < count; i++) {
		bool isInsert = false;
		while (!isInsert) {
			int indx = uniform_dist(e1);
			auto res = vals.insert(indx);
			isInsert = res.second;
		}
	}
	return vals;
}

reindexer::Item FullTextMergeLimit::MakeItem(benchmark::State&) {
	auto item = db_->NewItem(nsdef_.name);
	std::ignore = item.Unsafe(false);
	return item;
}

void FullTextMergeLimit::Insert(State& state) {
	std::vector<std::vector<bool>> indexes;
	indexes.resize(kWords_.size());
	for (unsigned int k = 0; k < indexes.size(); k++) {
		indexes[k].resize(id_seq_->Count(), false);
	}
	for (unsigned m = 0; m < indexes.size(); m++) {
		std::unordered_set<int> r = generateDistrib(id_seq_->Count() / kWords_.size() * (m + 1));
		for (auto v : r) {
			indexes[m][v] = true;
		}
	}
	benchmark::AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		std::string phrase;
		for (int h = 0; h < id_seq_->Count(); h++) {
			auto item = db_->NewItem(nsdef_.name);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
				continue;
			}
			std::ignore = item.Unsafe(true);
			phrase.clear();
			for (unsigned m = 0; m < indexes.size(); m++) {
				if (indexes[m][h]) {
					phrase += ' ';
					phrase += kWords_[m];
				} else {
					phrase += ' ';
					phrase += kWords_[m] + kEndWord + std::to_string(h);
				}
			}
			item["id"] = h;
			item[kFastIndexTextName_] = phrase;
			auto err = db_->Upsert(nsdef_.name, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	}
	state.SetLabel("inserted " + std::to_string(id_seq_->Count()) + " documents");
}

void FullTextMergeLimit::BuildFastTextIndex(benchmark::State& state) {
	benchmark::AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, kWords_[0]).Limit(20);

		reindexer::QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullTextMergeLimit::FastTextIndexSelect(benchmark::State& state, const std::string& qs) {
	benchmark::AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		reindexer::Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, qs);

		reindexer::QueryResults qres;
		auto err = db_->Select(q, qres);
		state.SetLabel("select " + std::to_string(qres.Count()) + " documents");
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}
