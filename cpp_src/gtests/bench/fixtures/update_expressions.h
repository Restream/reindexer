#pragma once

#include <array>
#include <string>
#include "base_fixture.h"

namespace reindexer_benchmarks {

class [[nodiscard]] UpdateExpressions : protected BaseFixture {
public:
	~UpdateExpressions() override = default;
	UpdateExpressions(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		using reindexer::IndexOpts;

		nsdef_.AddIndex(std::string(kFieldId), "hash", "int", IndexOpts().PK());
		nsdef_.AddIndex(std::string(kFieldIntIndex), "tree", "int", IndexOpts());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	void Insert(State& state);

	void SetValueLimit1(State& state);
	void SetValueLimit1000(State& state);

	void SetExprIndexLimit1(State& state);
	void SetExprIndexLimit1000(State& state);

	void SetExprNotIndexLimit1(State& state);
	void SetExprNotIndexLimit1000(State& state);

	void SetExprNestedLimit1(State& state);
	void SetExprNestedLimit1000(State& state);

	void SetExprNowLimit1(State& state);
	void SetExprNowLimit1000(State& state);

	void SetExprConcatLimit1(State& state);
	void SetExprConcatLimit1000(State& state);

	void setValue(State& state, unsigned int limit);
	void setExpression(State& state, std::string_view field, std::string_view expression, unsigned int limit);

	static std::string nestedPath() { return std::string(kFieldNestedIntNotindex[0]) + '.' + std::string(kFieldNestedIntNotindex[1]); }

	reindexer::WrSerializer wrSer_;
	int id_ = 0;
	static const unsigned kMaxScalarValue = 10000;
	static const unsigned kCount1000 = 1000;
	static const unsigned kIntArrayLen = 10;

	static constexpr std::string_view kFieldId = "id";
	static constexpr std::string_view kFieldIntIndex = "int_index";
	static constexpr std::string_view kFieldIntNotindex = "int_notindex";
	static constexpr std::array<std::string_view, 2> kFieldNestedIntNotindex = {"nested", "int_notindex"};
	static constexpr std::string_view kFieldTimeNotindex = "time_notindex";
	static constexpr std::string_view kFieldIntArrayNotindex = "int_array_notindex";
	static constexpr std::string_view kFieldIntArrayConcat = "int_array_concat";
};

}  // namespace reindexer_benchmarks
