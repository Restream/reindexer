#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "base_fixture.h"

class [[nodiscard]] FullTextMergeLimit : private BaseFixture {
public:
	virtual ~FullTextMergeLimit() {}
	FullTextMergeLimit(Reindexer* db, const std::string& name, size_t maxItems);

	void RegisterAllCases();

private:
	virtual reindexer::Item MakeItem(benchmark::State&) override;

	void Insert(State& state);
	void BuildFastTextIndex(benchmark::State& state);
	void FastTextIndexSelect(benchmark::State& state, const std::string& q);

	const std::string kFastIndexTextName_ = "description";

	std::unordered_set<int> generateDistrib(int count);

	const std::vector<std::string> kWords_ = {"корова", "бык", "дорога", "гора", "машина", "ведро", "титан", "телефон", "ключ", "мопед"};
	const std::string kEndWord = "разд";
};
