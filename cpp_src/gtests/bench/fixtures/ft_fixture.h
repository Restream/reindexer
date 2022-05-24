#pragma once

#include <string>
#include <vector>

#include "base_fixture.h"
#include "helpers.h"

class FullText : protected BaseFixture {
public:
	virtual ~FullText() {}
	FullText(Reindexer* db, const string& name, size_t maxItems);

	virtual Error Initialize();
	virtual void RegisterAllCases();

protected:
	virtual Item MakeItem();

protected:
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

	void InitForAlternatingUpdatesAndSelects(State&);
	void AlternatingUpdatesAndSelects(benchmark::State&);
	void AlternatingUpdatesAndSelectsByComposite(benchmark::State&);
	void AlternatingUpdatesAndSelectsByCompositeByNotIndexFields(benchmark::State&);

protected:
	string CreatePhrase();

	string MakePrefixWord();
	string MakeSuffixWord();
	string MakeTypoWord();

	wstring GetRandomUTF16WordByLength(size_t minLen = 4);

	vector<string> GetRandomCountries(size_t cnt = 5);
	string RandString();
	Item MakeSpecialItem();

protected:
	vector<string> words_;
	vector<string> countries_;
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
	const string letters = "abcdefghijklmnopqrstuvwxyz";
	const char* alternatingNs_ = "FtAlternatingUpdatesAndSelects";

	size_t raw_data_sz_ = 0;
};
