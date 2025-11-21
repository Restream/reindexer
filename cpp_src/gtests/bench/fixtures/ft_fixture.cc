#include "ft_fixture.h"
#include <benchmark/benchmark.h>

#include <thread>

#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "core/ft/config/ftfastconfig.h"
#include "tools/errors.h"

#include <dlfcn.h>

using benchmark::State;
using benchmark::AllocsTracker;

using reindexer::Query;
using reindexer::QueryResults;

static uint8_t printFlags = AllocsTracker::kPrintAllocs | AllocsTracker::kPrintHold;

FullText::FullText(Reindexer* db, const std::string& name, size_t maxItems)
	: BaseFixture(db, name, maxItems, 1, false), lowWordsDiversityNsDef_("LowWordsDiversityNs") {
#ifdef REINDEX_FT_EXTRA_DEBUG
	std::cout << "!!!REINDEXER WITH FT_EXTRA_DEBUG FLAG!!!!!" << std::endl;
#endif
	static reindexer::FtFastConfig ftFastCfg(1);
	ftFastCfg.optimization = reindexer::FtFastConfig::Optimization::Memory;

	static reindexer::FtFastConfig ftLowDiversityCfg(1);
	// for benching merge_limit break #2244
	ftLowDiversityCfg.mergeLimit = 4000;
	ftLowDiversityCfg.optimization = reindexer::FtFastConfig::Optimization::Memory;

	static IndexOpts ftFastIndexOpts;
	ftFastIndexOpts.SetConfig(IndexCompositeFastFT, ftFastCfg.GetJSON({}));
	ftFastIndexOpts.Dense();

	static IndexOpts ftLowDiversityIndexOpts;
	ftLowDiversityIndexOpts.SetConfig(IndexCompositeFastFT, ftLowDiversityCfg.GetJSON({}));
	ftLowDiversityIndexOpts.Dense();

	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("description", "-", "string", IndexOpts())
		.AddIndex("year", "tree", "int", IndexOpts())
		.AddIndex("countries", "tree", "string", IndexOpts().Array())
		.AddIndex(kFastIndexTextName_, {"countries", "description"}, "text", "composite", ftFastIndexOpts)
		.AddIndex("searchfuzzy", {"countries", "description"}, "fuzzytext", "composite", IndexOpts());
	lowWordsDiversityNsDef_.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("description1", "-", "string", IndexOpts())
		.AddIndex("description2", "-", "string", IndexOpts())
		.AddIndex(kLowDiversityIndexName_, {"description1", "description2"}, "text", "composite", ftLowDiversityIndexOpts);
}

template <reindexer::FtFastConfig::Optimization opt>
void FullText::UpdateIndex(State& state) {
	AllocsTracker allocsTracker(state, printFlags);

	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		static reindexer::FtFastConfig ftCfg(1);
		ftCfg.optimization = opt;
		setIndexConfig(nsdef_, kFastIndexTextName_, ftCfg);
	}

	// Warm up the index
	Query q(nsdef_.name);
	q.Where(kFastIndexTextName_, CondEq, "lskfj");
	QueryResults qres;
	auto err = db_->Select(q, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
}

reindexer::Error FullText::Initialize() {
	auto err = BaseFixture::Initialize();
	if (!err.ok()) {
		return err;
	}

	err = FullTextBase::Initialize();
	if (!err.ok()) {
		return err;
	}

	words2_ = {"стол", "столом", "столы", "cnjk",	"stol",		  "бежит",	"бегут", "бежали",	 ",tu",	  "beg",
			   "дом",  "доме",	 "ljv",	  "ракета", "ракетой",	  "ракеты", "hfrtn", "raketa",	 "летит", "летает",
			   "цель", "цели",	 "взрыв", "vzryv",	"взорвалось", "мир",	"танк",	 "танковый", "танки", "tayr"};

	// clang-format off
	countries_ = {
		"Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua & Deps",  "Argentina", "Armenia",
		"Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus",
		"Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia Herzegovina", "Botswana", "Brazil",
		"Brunei", "Bulgaria", "Burkina", "Burundi", "Cambodia", "Cameroon", "Canada", "Cape Verde",
		"Central African Rep", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Congo {Democratic Rep}",
		"Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark", "Djibouti", "Dominica",
		"Dominican Republic", "East Timor", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia",
		"Ethiopia", "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany",
		"Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti",
		"Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland {Republic}",
		"Israel", "Italy", "Ivory Coast", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya",
		"Kiribati", "Korea North", "Korea South", "Kosovo", "Kuwait", "Kyrgyzstan", "Laos", "Latvia",
		"Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Macedonia",
		"Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania",
		"Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco",
		"Mozambique", "Myanmar, {Burma}", "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand",
		"Nicaragua", "Niger", "Nigeria", "Norway", "Oman", "Pakistan", "Palau", "Panama",
		"Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania",
		"Russian Federation", "Rwanda", "St Kitts & Nevis", "St Lucia", "Saint Vincent & the Grenadines", "Samoa", "San Marino", "Sao Tome & Principe",
		"Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia",
		"Solomon Islands", "Somalia", "South Africa", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname",
		"Swaziland", "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand",
		"Togo", "Tonga", "Trinidad & Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda",
		"Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City",
		"Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe"
	};
	// clang-format on
	err = db_->AddNamespace(lowWordsDiversityNsDef_);
	if (!err.ok()) {
		return err;
	}

	return {};
}

void FullText::RegisterAllCases(std::optional<size_t> fastIterationCount, std::optional<size_t> slowIterationCount) {
	constexpr static auto Mem = reindexer::FtFastConfig::Optimization::Memory;
	constexpr static auto CPU = reindexer::FtFastConfig::Optimization::CPU;

	RegisterWrapper wrapSlow(slowIterationCount);  // std::numeric_limits<size_t>::max() test limit - default time
	RegisterWrapper wrapFast(fastIterationCount);
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("BuildAndInsertNs2", &FullText::BuildInsertLowDiversityNs, this)->Iterations(25000)->Unit(benchmark::kMicrosecond);
	wrapSlow.SetOptions(Register("Fast3PhraseLowDiversity", &FullText::Fast3PhraseLowDiversity, this));
	wrapSlow.SetOptions(Register("Fast3WordsLowDiversity", &FullText::Fast3WordsLowDiversity, this));

	wrapSlow.SetOptions(Register("Fast3WordsWithAreasLowDiversity", &FullText::Fast3WordsWithAreasLowDiversity, this));
	wrapSlow.SetOptions(Register("Fast3PhraseWithAreasLowDiversity", &FullText::Fast3PhraseWithAreasLowDiversity, this));

	wrapFast.SetOptions(Register("Fast2PhraseLowDiversity", &FullText::Fast2PhraseLowDiversity, this));
	wrapFast.SetOptions(Register("Fast2AndWordLowDiversity", &FullText::Fast2AndWordLowDiversity, this));

	Register("Insert", &FullText::Insert, this)->Iterations(id_seq_->Count())->Unit(benchmark::kMicrosecond);

	Register("BuildFastTextIndex", &FullText::BuildFastTextIndex, this)->Iterations(1)->Unit(benchmark::kMicrosecond);

	wrapFast.SetOptions(Register("Fast1WordMatch.OptByMem", &FullText::Fast1WordMatch, this));
	wrapFast.SetOptions(Register("Fast2WordsMatch.OptByMem", &FullText::Fast2WordsMatch, this));
	wrapSlow.SetOptions(Register("Fast1PrefixMatch.OptByMem", &FullText::Fast1PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast2PrefixMatch.OptByMem", &FullText::Fast2PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast1SuffixMatch.OptByMem", &FullText::Fast1SuffixMatch, this));
	wrapSlow.SetOptions(Register("Fast2SuffixMatch.OptByMem", &FullText::Fast2SuffixMatch, this));
	wrapFast.SetOptions(Register("Fast1TypoWordMatch.OptByMem", &FullText::Fast1TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast2TypoWordMatch.OptByMem", &FullText::Fast2TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast1WordWithAreaHighDiversity.OptByMem", &FullText::Fast1WordWithAreaHighDiversity, this));

	Register("SetOptimizationByCPU", &FullText::UpdateIndex<CPU>, this)->Iterations(1)->Unit(benchmark::kMicrosecond);

	wrapFast.SetOptions(Register("Fast1WordMatch.OptByCPU", &FullText::Fast1WordMatch, this));
	wrapFast.SetOptions(Register("Fast2WordsMatch.OptByCPU", &FullText::Fast2WordsMatch, this));
	wrapSlow.SetOptions(Register("Fast1PrefixMatch.OptByCPU", &FullText::Fast1PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast2PrefixMatch.OptByCPU", &FullText::Fast2PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast1SuffixMatch.OptByCPU", &FullText::Fast1SuffixMatch, this));
	wrapSlow.SetOptions(Register("Fast2SuffixMatch.OptByCPU", &FullText::Fast2SuffixMatch, this));
	wrapFast.SetOptions(Register("Fast1TypoWordMatch.OptByCPU", &FullText::Fast1TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast2TypoWordMatch.OptByCPU", &FullText::Fast2TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast1WordWithAreaHighDiversity.OptByCPU", &FullText::Fast1WordWithAreaHighDiversity, this));

	// Register("BuildFuzzyTextIndex", &FullText::BuildFuzzyTextIndex, this)->Iterations(1)->Unit(benchmark::kMicrosecond);

	// Register("Fuzzy1WordMatch", &FullText::Fuzzy1WordMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy2WordsMatch", &FullText::Fuzzy2WordsMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy1PrefixMatch", &FullText::Fuzzy1PrefixMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy2PrefixMatch", &FullText::Fuzzy2PrefixMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy1SuffixMatch", &FullText::Fuzzy1SuffixMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy2SuffixMatch", &FullText::Fuzzy2SuffixMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy1TypoWordMatch", &FullText::Fuzzy1TypoWordMatch, this)->Unit(benchmark::kMicrosecond);
	// Register("Fuzzy2TypoWordMatch", &FullText::Fuzzy2TypoWordMatch, this)->Unit(benchmark::kMicrosecond);

	Register("BuildInsert.Incremental", &FullText::BuildInsertIncremental, this)
		->Iterations(id_seq_->Count())
		->Unit(benchmark::kMicrosecond);
	wrapFast.SetOptions(Register("Fast1WordMatch.Incremental", &FullText::Fast1WordMatch, this));
	wrapFast.SetOptions(Register("Fast2WordsMatch.Incremental", &FullText::Fast2WordsMatch, this));
	wrapSlow.SetOptions(Register("Fast1PrefixMatch.Incremental", &FullText::Fast1PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast2PrefixMatch.Incremental", &FullText::Fast2PrefixMatch, this));
	wrapSlow.SetOptions(Register("Fast1SuffixMatch.Incremental", &FullText::Fast1SuffixMatch, this));
	wrapSlow.SetOptions(Register("Fast2SuffixMatch.Incremental", &FullText::Fast2SuffixMatch, this));
	wrapFast.SetOptions(Register("Fast1TypoWordMatch.Incremental", &FullText::Fast1TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast2TypoWordMatch.Incremental", &FullText::Fast2TypoWordMatch, this));
	wrapFast.SetOptions(Register("Fast1WordWithAreaHighDiversity.Incremental", &FullText::Fast1WordWithAreaHighDiversity, this));

	Register("InitForAlternatingUpdatesAndSelects.OptByMem", &FullText::InitForAlternatingUpdatesAndSelects<Mem>, this)
		->Iterations(1)
		->Unit(benchmark::kMicrosecond);
	wrapFast.SetOptions(Register("AlternatingUpdatesAndSelects.OptByMem", &FullText::AlternatingUpdatesAndSelects, this));
	wrapFast.SetOptions(
		Register("AlternatingUpdatesAndSelectsByComposite.OptByMem", &FullText::AlternatingUpdatesAndSelectsByComposite, this));
	wrapFast.SetOptions(Register("AlternatingUpdatesAndSelectsByCompositeByNotIndexFields.OptByMem",
								 &FullText::AlternatingUpdatesAndSelectsByCompositeByNotIndexFields, this));
	Register("InitForAlternatingUpdatesAndSelects.OptByCPU", &FullText::InitForAlternatingUpdatesAndSelects<CPU>, this)
		->Iterations(1)
		->Unit(benchmark::kMicrosecond);
	wrapFast.SetOptions(Register("AlternatingUpdatesAndSelects.OptByCPU", &FullText::AlternatingUpdatesAndSelects, this));
	wrapFast.SetOptions(
		Register("AlternatingUpdatesAndSelectsByComposite.OptByCPU", &FullText::AlternatingUpdatesAndSelectsByComposite, this));
	wrapFast.SetOptions(Register("AlternatingUpdatesAndSelectsByCompositeByNotIndexFields.OptByCPU",
								 &FullText::AlternatingUpdatesAndSelectsByCompositeByNotIndexFields, this));
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Item FullText::MakeLowDiversityItem(int id) {
	auto createText = [this]() {
		const size_t wordCnt = RndInt(10, 25);
		reindexer::WrSerializer r;
		r.Reserve(wordCnt * 30);

		for (size_t i = 0; i < wordCnt; i++) {
			r << RndFrom(words2_);
			if (i < wordCnt - 1) {
				r << ' ';
			}
		}
		return std::string(r.Slice());
	};

	auto item = db_->NewItem(lowWordsDiversityNsDef_.name);
	item["id"] = id;
	item["description1"] = createText();
	item["description2"] = createText();
	return item;
}

reindexer::Item FullText::MakeItem(benchmark::State&) {
	auto item = db_->NewItem(nsdef_.name);
	std::ignore = item.Unsafe(false);

	auto phrase = CreatePhrase();
	auto countries = GetRandomCountries();
	raw_data_sz_ += phrase.size();
	for (auto& c : countries) {
		raw_data_sz_ += c.size();
	}

	item["id"] = id_seq_->Next();
	item["description"] = phrase;
	item["year"] = RndInt(2000, 2049);
	item["countries"] = toArray<std::string>(countries);

	return item;
}

constexpr unsigned kMaxIterStepsMultiplier = 10;

void FullText::BuildInsertIncremental(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t i = 0, mem = 0;

	dropNamespace(nsdef_.name, state);
	id_seq_->Reset();
	raw_data_sz_ = 0;

	auto err = BaseFixture::Initialize();
	if (!err.ok()) {
		state.SkipWithError(err.what());
		assertf(err.ok(), "{}", err.what());
	}

	constexpr int kMaxStepsCount = 50;
	const auto itemsPerStep = initStepsConfig(kMaxStepsCount, nsdef_, kFastIndexTextName_, state.max_iterations / kMaxIterStepsMultiplier);

	auto execQuery = [&] {
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, RndWord1()).Limit(20);

		QueryResults qres;
		size_t memory = get_alloc_size();
		err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
			return;
		}
		mem += get_alloc_size() - memory;
	};

	assertrx(Words1Count());
	const auto itemsWithoutRebuild = size_t((state.max_iterations - state.max_iterations / kMaxIterStepsMultiplier));
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeItem(state);
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
			assertf(item.Status().ok(), "{}", item.Status().what());
		}
		err = db_->Insert(nsdef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
			assertf(err.ok(), "{}", err.what());
		}

		if ((++i) >= itemsWithoutRebuild && i % itemsPerStep == 0) {
			// UpdateTracker will force full index rebuild if there are to many updates. kMaxIterStepsMultiplier is required to avoid the
			// mechanism
			execQuery();
		}
	}
	execQuery();
	double ratio = mem / double(raw_data_sz_);
	state.SetLabel("Commit ratio: " + std::to_string(ratio));
}

void FullText::Insert(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeItem(state);
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
			continue;
		}

		auto err = db_->Insert(nsdef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullText::BuildCommonIndexes(benchmark::State& state) {
	using namespace std::string_view_literals;
	AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("year"sv, CondRange, {2010, 2016}).Limit(20).Sort("year"sv, false);
		std::this_thread::sleep_for(std::chrono::milliseconds(2000));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullText::BuildInsertLowDiversityNs(State& state) {
	AllocsTracker allocsTracker(state, printFlags);

	int idCounter = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto item = MakeLowDiversityItem(idCounter);
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
			continue;
		}

		auto err = db_->Insert(lowWordsDiversityNsDef_.name, item);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		idCounter++;
	}

	Query q(lowWordsDiversityNsDef_.name);
	q.Where(kLowDiversityIndexName_, CondEq, words2_.at(0)).Limit(1);
	QueryResults qres;
	auto err = db_->Select(q, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
}

void FullText::Fast3PhraseLowDiversity(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		const std::string& w3 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + w3.size() + 32);
		ftQuery.append("'").append(w1).append(" ").append(w2).append("' ").append(w3);
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		QueryResults qres;

		auto err = db_->Select(q, qres);

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast3WordsLowDiversity(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		const std::string& w3 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + w3.size() + 32);
		ftQuery.append("+").append(w1).append(" +").append(w2).append(" +").append(w3);
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		QueryResults qres;

		auto err = db_->Select(q, qres);

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2PhraseLowDiversity(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + 32);
		ftQuery.append("'").append(w1).append(" ").append(w2).append("'~50");
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		QueryResults qres;

		auto err = db_->Select(q, qres);

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2AndWordLowDiversity(State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + 32);
		ftQuery.append("+").append(w1).append(" +").append(w2);
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		QueryResults qres;

		auto err = db_->Select(q, qres);

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast3PhraseWithAreasLowDiversity(State& state) {
	const auto hilightStr = fmt::format("{} = highlight(!,!)", kLowDiversityIndexName_);

	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		const std::string& w3 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + w3.size() + 32);
		ftQuery.append("'").append(w1).append(" ").append(w2).append("' ").append(w3);
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		q.AddFunction(hilightStr);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}
void FullText::Fast1WordWithAreaHighDiversity(State& state) {
	const auto hilightStr = fmt::format("{} = highlight(!,!)", kFastIndexTextName_);

	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		const std::string& word = RndWord1();
		q.Where(kFastIndexTextName_, CondEq, word);
		q.AddFunction(hilightStr);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}
void FullText::Fast3WordsWithAreasLowDiversity(State& state) {
	const auto hilightStr = fmt::format("{} = highlight(!,!)", kLowDiversityIndexName_);

	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(lowWordsDiversityNsDef_.name);
		const std::string& w1 = RndFrom(words2_);
		const std::string& w2 = RndFrom(words2_);
		const std::string& w3 = RndFrom(words2_);
		std::string ftQuery;
		ftQuery.reserve(w1.size() + w2.size() + w3.size() + 32);
		ftQuery.append(w1).append(" ").append(w2).append(" ").append(w3);
		q.Where(kLowDiversityIndexName_, CondEq, std::move(ftQuery));
		q.AddFunction(hilightStr);
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::BuildFastTextIndex(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t mem = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, RndWord1()).Limit(20);

		QueryResults qres;

		mem = get_alloc_size();
		auto err = db_->Select(q, qres);
		mem = get_alloc_size() - mem;

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
	double ratio = mem / double(raw_data_sz_);
	state.SetLabel("Commit ratio: " + std::to_string(ratio));
}

void FullText::BuildFuzzyTextIndex(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t mem = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, RndWord1()).Limit(20);

		QueryResults qres;

		mem = get_alloc_size();
		auto err = db_->Select(q, qres);
		mem = get_alloc_size() - mem;

		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
	double ratio = mem / double(raw_data_sz_);
	state.SetLabel("Commit ratio: " + std::to_string(ratio));
}

void FullText::Fast1WordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast1WordMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)

		TIMEMEASURE();
		Query q(nsdef_.name);

		q.Where(kFastIndexTextName_, CondEq, RndWord1());

		QueryResults qres;

		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2WordsMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast2WordsMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		auto q = Query(nsdef_.name).Where(kFastIndexTextName_, CondEq, RndWord1() + ' ' + RndWord1());
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}

	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy1WordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);

		q.Where("searchfuzzy", CondEq, RndWord1());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy2WordsMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		auto q = Query(nsdef_.name).Where("searchfuzzy", CondEq, RndWord1() + ' ' + RndWord1());
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast1PrefixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast1PrefixMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakePrefixWord());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2PrefixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast2PrefixMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakePrefixWord().append(" ").append(MakePrefixWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy1PrefixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakePrefixWord());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy2PrefixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakePrefixWord().append(" ").append(MakePrefixWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast1SuffixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast1SuffixMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakeSuffixWord());
		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2SuffixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast2SuffixMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakeSuffixWord().append(" ").append(MakeSuffixWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy1SuffixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakeSuffixWord());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy2SuffixMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakeSuffixWord().append(" ").append(MakeSuffixWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast1TypoWordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast1TypoWordMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakeTypoWord());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fast2TypoWordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	TIMETRACKER("Fast2TypoWordMatch.gist");
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		TIMEMEASURE();
		Query q(nsdef_.name);
		q.Where(kFastIndexTextName_, CondEq, MakeTypoWord().append(" ").append(MakeTypoWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy1TypoWordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakeTypoWord());

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

void FullText::Fuzzy2TypoWordMatch(benchmark::State& state) {
	AllocsTracker allocsTracker(state, printFlags);
	size_t cnt = 0;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		Query q(nsdef_.name);
		q.Where("searchfuzzy", CondEq, MakeTypoWord().append(" ").append(MakeTypoWord()));

		QueryResults qres;
		auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		cnt += qres.Count();
	}
	state.SetLabel(FormatString("RPR: %.1f", cnt / double(state.iterations())));
}

std::vector<std::string> FullText::GetRandomCountries(size_t cnt) {
	std::vector<std::string> result;
	result.reserve(cnt);
	for (auto i = cnt; i > 0; --i) {
		result.emplace_back(RndFrom(countries_));
	}
	return result;
}

template <reindexer::FtFastConfig::Optimization opt>
void FullText::InitForAlternatingUpdatesAndSelects(State& state) {
	constexpr int kNsSize = 100'000;
	static reindexer::FtFastConfig ftCfg(1);
	static IndexOpts ftIndexOpts;
	ftCfg.optimization = opt;
	ftIndexOpts.SetConfig(IndexFastFT, ftCfg.GetJSON({}));
	AllocsTracker allocsTracker(state, printFlags);

	dropNamespace(alternatingNs_, state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		NamespaceDef nsDef{alternatingNs_};
		nsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
			.AddIndex("search1", "text", "string", ftIndexOpts)
			.AddIndex("search2", "text", "string", ftIndexOpts)
			.AddIndex("rand", "hash", "int", IndexOpts())
			.AddIndex("search_comp", {"search1", "search2"}, "text", "composite", ftIndexOpts)
			.AddIndex("search_comp_not_index_fields", {"field1", "field2"}, "text", "composite", ftIndexOpts);
		auto err = db_->AddNamespace(nsDef);
		if (!err.ok()) {
			state.SkipWithError(err.what());
			assertf(err.ok(), "{}", err.what());
		}
		values_.clear();
		values_.reserve(kNsSize);
		reindexer::WrSerializer ser;
		for (int i = 0; i < kNsSize; ++i) {
			values_.emplace_back(RandString(), RandString(), RandString(), RandString());
			ser.Reset();
			reindexer::JsonBuilder bld(ser);
			bld.Put("id", i);
			bld.Put("search1", values_.back().search1);
			bld.Put("search2", values_.back().search2);
			bld.Put("field1", values_.back().field1);
			bld.Put("field2", values_.back().field2);
			bld.Put("rand", rand());
			bld.End();
			auto item = db_->NewItem(alternatingNs_);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
				continue;
			}
			err = item.FromJSON(ser.Slice());
			if (!err.ok()) {
				state.SkipWithError(err.what());
				continue;
			}
			err = db_->Insert(alternatingNs_, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	}

	// Init index build
	assert(!values_.empty());
	const Query q1 = Query(alternatingNs_).Where("search1", CondEq, RndFrom(values_).search1);
	QueryResults qres;
	auto err = db_->Select(q1, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}

	const auto& rndValue1 = RndFrom(values_);
	const Query q2 = Query(alternatingNs_).Where("search_comp", CondEq, rndValue1.search1 + ' ' + rndValue1.search2);
	qres.Clear();
	err = db_->Select(q2, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}

	const auto& rndValue2 = RndFrom(values_);
	const Query q3 = Query(alternatingNs_).Where("search_comp_not_index_fields", CondEq, rndValue2.field1 + ' ' + rndValue2.field2);
	qres.Clear();
	err = db_->Select(q3, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
}

void FullText::updateAlternatingNs(reindexer::WrSerializer& ser, benchmark::State& state) {
	using namespace std::string_literals;
	assert(!values_.empty());
	const int i = RndIndexOf(values_);
	ser.Reset();
	reindexer::JsonBuilder bld(ser);
	bld.Put("id", i);
	bld.Put("search1", values_[i].search1);
	bld.Put("search2", values_[i].search2);
	bld.Put("field1", values_[i].field1);
	bld.Put("field2", values_[i].field2);
	bld.Put("rand", rand());
	bld.End();
	auto item = db_->NewItem(alternatingNs_);
	std::ignore = item.Unsafe(false);
	if (!item.Status().ok()) {
		state.SkipWithError(item.Status().what());
		return;
	}
	auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
		return;
	}
	err = db_->Update(alternatingNs_, item);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}

	const std::string sql = fmt::format("UPDATE {} SET rand = {} WHERE id = {}", alternatingNs_, rand(), RndIndexOf(values_));
	QueryResults qres;
	err = db_->ExecSQL(sql, qres);
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
}

void FullText::AlternatingUpdatesAndSelects(benchmark::State& state) {
	assert(!values_.empty());
	reindexer::WrSerializer ser;
	AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		state.PauseTiming();
		updateAlternatingNs(ser, state);
		const Query q = Query(alternatingNs_).Where("search1", CondEq, RndFrom(values_).search1);
		QueryResults qres;
		state.ResumeTiming();
		const auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullText::AlternatingUpdatesAndSelectsByComposite(benchmark::State& state) {
	assert(!values_.empty());
	reindexer::WrSerializer ser;
	AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		state.PauseTiming();
		updateAlternatingNs(ser, state);
		const auto& rndValue = RndFrom(values_);
		const Query q = Query(alternatingNs_).Where("search_comp", CondEq, rndValue.search1 + ' ' + rndValue.search2);
		QueryResults qres;
		state.ResumeTiming();
		const auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullText::AlternatingUpdatesAndSelectsByCompositeByNotIndexFields(benchmark::State& state) {
	assert(!values_.empty());
	reindexer::WrSerializer ser;
	AllocsTracker allocsTracker(state, printFlags);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		state.PauseTiming();
		updateAlternatingNs(ser, state);
		const auto& rndValue = RndFrom(values_);
		Query q = Query(alternatingNs_).Where("search_comp_not_index_fields", CondEq, rndValue.field1 + ' ' + rndValue.field2);
		QueryResults qres;
		state.ResumeTiming();
		const auto err = db_->Select(q, qres);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
	}
}

void FullText::setIndexConfig(NamespaceDef& nsDef, std::string_view indexName, const reindexer::FtFastConfig& ftCfg) {
	const auto it =
		std::find_if(nsDef.indexes.begin(), nsDef.indexes.end(), [indexName](const auto& idx) { return idx.Name() == indexName; });
	assertrx(it != nsDef.indexes.end());
	auto opts = it->Opts();
	opts.SetConfig(IndexFastFT, ftCfg.GetJSON({}));
	it->SetOpts(std::move(opts));
	const auto err = db_->UpdateIndex(nsDef.name, *it);
	(void)err;
	assertf(err.ok(), "err: {}", err.what());
}

unsigned FullText::initStepsConfig(int maxStepsCount, NamespaceDef& nsDef, std::string_view indexName, benchmark::IterationCount iters) {
	const auto totalItems = iters;
	const auto itemsPerStep = totalItems / maxStepsCount + 1;
	assertrx(itemsPerStep > 2);
	{
		static reindexer::FtFastConfig ftCfg(1);
		ftCfg.maxRebuildSteps = maxStepsCount;
		ftCfg.maxStepSize = std::max(5, int(itemsPerStep / 2));
		setIndexConfig(nsDef, indexName, ftCfg);
	}
	return itemsPerStep;
}

void FullText::dropNamespace(std::string_view name, benchmark::State& state) {
	auto err = db_->DropNamespace(name);
	if (!err.ok()) {
		if (err.code() != errNotFound || err.what() != "Namespace '" + alternatingNs_ + "' does not exist") {
			state.SkipWithError(err.what());
			assertf(err.ok(), "{}", err.what());
		}
	}
}
