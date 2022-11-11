#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"
#include "core/ft/config/ftfastconfig.h"
#include "helpers.h"

class FullText : protected BaseFixture {
public:
	virtual ~FullText() {}
	FullText(Reindexer* db, const std::string& name, size_t maxItems);

	virtual reindexer::Error Initialize();
	virtual void RegisterAllCases();

protected:
	virtual reindexer::Item MakeItem();

protected:
	template <reindexer::FtFastConfig::Optimization>
	void UpdateIndex(State&);
	void Insert(State& state);
	void BuildInsertSteps(State& state);

	void BuildCommonIndexes(State& state);
	void BuildFastTextIndex(State& state);
	void BuildFuzzyTextIndex(State& state);

	void Fast1WordMatch(State& state);
	void Fast2WordsMatch(State& state);
	void Fuzzy1WordMatch(State& state);
	void Fuzzy2WordsMatch(State& state);

	void Fast1PrefixMatch(State& state);
	void Fast2PrefixMatch(State& state);
	void Fuzzy1PrefixMatch(State& state);
	void Fuzzy2PrefixMatch(State& state);

	void Fast1SuffixMatch(State& state);
	void Fast2SuffixMatch(State& state);
	void Fuzzy1SuffixMatch(State& state);
	void Fuzzy2SuffixMatch(State& state);

	void Fast1TypoWordMatch(State& state);
	void Fast2TypoWordMatch(State& state);
	void Fuzzy1TypoWordMatch(State& state);
	void Fuzzy2TypoWordMatch(State& state);

	void BuildStepFastIndex(State& state);

	template <reindexer::FtFastConfig::Optimization>
	void InitForAlternatingUpdatesAndSelects(State&);
	void AlternatingUpdatesAndSelects(benchmark::State&);
	void AlternatingUpdatesAndSelectsByComposite(benchmark::State&);
	void AlternatingUpdatesAndSelectsByCompositeByNotIndexFields(benchmark::State&);

protected:
	string CreatePhrase();

	string MakePrefixWord();
	string MakeSuffixWord();
	string MakeTypoWord();

	std::wstring GetRandomUTF16WordByLength(size_t minLen = 4);

	vector<std::string> GetRandomCountries(size_t cnt = 5);
	reindexer::Item MakeSpecialItem();

protected:
	vector<std::string> words_;
	vector<std::string> countries_;
	struct Values {
		Values(std::string s1, std::string s2, std::string f1, std::string f2) noexcept
			: search1{std::move(s1)}, search2{std::move(s2)}, field1{std::move(f1)}, field2{std::move(f2)} {}
		std::string search1;
		std::string search2;
		std::string field1;
		std::string field2;
	};
	std::vector<Values> values_;

private:
	void updateAlternatingNs(reindexer::WrSerializer&, benchmark::State&);
	const char* alternatingNs_ = "FtAlternatingUpdatesAndSelects";

	size_t raw_data_sz_ = 0;
};
