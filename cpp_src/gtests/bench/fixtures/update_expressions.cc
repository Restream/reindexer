#include "update_expressions.h"
#include "core/cjson/jsonbuilder.h"

namespace reindexer_benchmarks {

reindexer::Error UpdateExpressions::Initialize() {
	assertrx(db_);
	return db_->AddNamespace(nsdef_);
}

void UpdateExpressions::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &UpdateExpressions::Insert, this)->Iterations(1);
	Register("SetValueLimit1", &UpdateExpressions::SetValueLimit1, this);
	Register("SetValueLimit1000", &UpdateExpressions::SetValueLimit1000, this);
	Register("SetExprIndexLimit1", &UpdateExpressions::SetExprIndexLimit1, this);
	Register("SetExprIndexLimit1000", &UpdateExpressions::SetExprIndexLimit1000, this);
	Register("SetExprNotIndexLimit1", &UpdateExpressions::SetExprNotIndexLimit1, this);
	Register("SetExprNotIndexLimit1000", &UpdateExpressions::SetExprNotIndexLimit1000, this);
	Register("SetExprNestedLimit1", &UpdateExpressions::SetExprNestedLimit1, this);
	Register("SetExprNestedLimit1000", &UpdateExpressions::SetExprNestedLimit1000, this);
	Register("SetExprNowLimit1", &UpdateExpressions::SetExprNowLimit1, this);
	Register("SetExprNowLimit1000", &UpdateExpressions::SetExprNowLimit1000, this);
	Register("SetExprConcatLimit1", &UpdateExpressions::SetExprConcatLimit1, this);
	Register("SetExprConcatLimit1000", &UpdateExpressions::SetExprConcatLimit1000, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Item UpdateExpressions::MakeItem(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	std::ignore = item.Unsafe();

	wrSer_.Reset();
	{
		reindexer::JsonBuilder json(wrSer_);
		json.Put(kFieldId, id_++);
		json.Put(kFieldIntIndex, static_cast<int>(rand() % kMaxScalarValue));
		json.Put(kFieldIntNotindex, static_cast<int>(rand() % kMaxScalarValue));
		json.Put(kFieldTimeNotindex, int64_t(0));
		{
			auto nested = json.Object(kFieldNestedIntNotindex[0]);
			nested.Put(kFieldNestedIntNotindex[1], static_cast<int>(rand() % kMaxScalarValue));
		}
		std::vector<int> array;
		array.reserve(kIntArrayLen);
		for (unsigned i = 0; i < kIntArrayLen; ++i) {
			array.emplace_back(static_cast<int>(rand() % kMaxScalarValue));
		}
		json.Array<int>(kFieldIntArrayNotindex, array);
		json.Array<int>(kFieldIntArrayConcat, std::span<const int>{});
	}
	const auto err = item.FromJSON(wrSer_.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
	return item;
}

void UpdateExpressions::Insert(State& state) { BaseFixture::Insert(state); }

void UpdateExpressions::setValue(State& state, unsigned int limit) {
	auto qGen = [&]() {
		return reindexer::Query(nsdef_.name).Set(kFieldIntNotindex, static_cast<int>(rand() % kMaxScalarValue)).Limit(limit);
	};
	benchUpdate(qGen, state);
}

void UpdateExpressions::setExpression(State& state, std::string_view field, std::string_view expression, unsigned int limit) {
	auto qGen = [&]() {
		return reindexer::Query(nsdef_.name)
			.Set(field, reindexer::Variant{std::string{expression}}, reindexer::HasExpression_True)
			.Limit(limit);
	};
	benchUpdate(qGen, state);
}

void UpdateExpressions::SetValueLimit1(State& state) { setValue(state, 1); }
void UpdateExpressions::SetValueLimit1000(State& state) { setValue(state, kCount1000); }

void UpdateExpressions::SetExprIndexLimit1(State& state) { setExpression(state, kFieldIntIndex, std::string(kFieldIntIndex) + " + 1", 1); }
void UpdateExpressions::SetExprIndexLimit1000(State& state) {
	setExpression(state, kFieldIntIndex, std::string(kFieldIntIndex) + " + 1", kCount1000);
}

void UpdateExpressions::SetExprNotIndexLimit1(State& state) {
	setExpression(state, kFieldIntNotindex, std::string(kFieldIntNotindex) + " + 1", 1);
}
void UpdateExpressions::SetExprNotIndexLimit1000(State& state) {
	setExpression(state, kFieldIntNotindex, std::string(kFieldIntNotindex) + " + 1", kCount1000);
}

void UpdateExpressions::SetExprNestedLimit1(State& state) {
	const auto path = nestedPath();
	setExpression(state, path, path + " + 1", 1);
}
void UpdateExpressions::SetExprNestedLimit1000(State& state) {
	const auto path = nestedPath();
	setExpression(state, path, path + " + 1", kCount1000);
}

void UpdateExpressions::SetExprNowLimit1(State& state) { setExpression(state, kFieldTimeNotindex, "now(msec)", 1); }
void UpdateExpressions::SetExprNowLimit1000(State& state) { setExpression(state, kFieldTimeNotindex, "now(msec)", kCount1000); }

void UpdateExpressions::SetExprConcatLimit1(State& state) {
	setExpression(state, kFieldIntArrayConcat, std::string(kFieldIntArrayNotindex) + " || [1, 2]", 1);
}
void UpdateExpressions::SetExprConcatLimit1000(State& state) {
	setExpression(state, kFieldIntArrayConcat, std::string(kFieldIntArrayNotindex) + " || [1, 2]", kCount1000);
}

}  // namespace reindexer_benchmarks
