#include "equalpositions.h"
#include "core/cjson/jsonbuilder.h"

void EqualPositions::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &EqualPositions::Insert, this)->Iterations(1);
	Register("EqPos2Index", &EqualPositions::EqPos2Index, this);
	Register("EqPos2", &EqualPositions::EqPos2, this);
	Register("EqPos2Grouping", &EqualPositions::EqPos2Grouping, this);
	Register("EqPos4", &EqualPositions::EqPos4, this);
	Register("EqPos2GroupingSubArray", &EqualPositions::EqPos2GroupingSubArray, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

class [[nodiscard]] ResultCounter {
public:
	void operator()(const reindexer::QueryResults& qres) {
		++callCount_;
		if (qres.Count() == 0) {
			++emptyResultCount_;
		}
	}
	bool isResultValid() { return double(emptyResultCount_) / double(callCount_) < 0.5; }

private:
	unsigned int emptyResultCount_ = 0;
	unsigned int callCount_ = 0;
};

void EqualPositions::Insert(State& state) {
	benchmark::AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		for (size_t i = 0; i < kRowCount; ++i) {
			auto item = MakeItem(state);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
			}
			auto err = db_->Insert(nsdef_.name, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
		}
	}
}

void EqualPositions::EqPos2Index(State& state) {
	auto qGen = [&]() {
		int val1 = rand() % (kArray1End - kArray1Start) + kArray1Start + (kArray1End - kArray1Start) / 5;
		int val2 = rand() % (kArray2End - kArray2Start) + kArray2Start + (kArray2End - kArray2Start) / 10;
		return reindexer::Query(nsdef_.name)
			.Where("int_array_index1", CondEq, val1)
			.And()
			.Where("int_array_index2", CondEq, val2)
			.EqualPositions({"int_array_index1", "int_array_index2"});
	};
	LowSelectivityItemsCounter<50> counter(state, 100);
	benchQuery(qGen, state, counter);
}

void EqualPositions::EqPos2(State& state) {
	auto qGen = [&]() {
		int val1 = rand() % (kArray1End - kArray1Start) + kArray1Start + (kArray1End - kArray1Start) / 5;
		int val2 = rand() % (kArray2End - kArray2Start) + kArray2Start + (kArray2End - kArray2Start) / 10;
		return reindexer::Query(nsdef_.name)
			.Where("int_array1", CondEq, val1)
			.And()
			.Where("int_array2", CondEq, val2)
			.EqualPositions({"int_array1", "int_array2"});
	};
	LowSelectivityItemsCounter<50> counter(state, 100);
	benchQuery(qGen, state, counter);
}

void EqualPositions::EqPos2Grouping(State& state) {
	auto qGen = [&]() {
		int val1 = rand() % (kArray1End - kArray1Start) + kArray1Start + (kArray1End - kArray1Start) / 5;
		int val2 = rand() % (kArray2End - kArray2Start) + kArray2Start + (kArray2End - kArray2Start) / 10;
		return reindexer::Query(nsdef_.name)
			.Where("int_array1", CondEq, val1)
			.And()
			.Where("int_array2", CondEq, val2)
			.EqualPositions({"int_array1[#]", "int_array2[#]"});
	};
	LowSelectivityItemsCounter<50> counter(state, 100);
	benchQuery(qGen, state, counter);
}

void EqualPositions::EqPos4(State& state) {
	auto qGen = [&]() {
		int val1 = rand() % (kArray1End - kArray1Start) + kArray1Start;
		int val2 = rand() % (kArray2End - kArray2Start) + kArray2Start;
		int val3 = rand() % (kArray1End - kArray1Start) + kArray1Start;
		int val4 = rand() % (kArray2End - kArray2Start) + kArray2Start;

		return reindexer::Query(nsdef_.name)
			.Where("int_array1", CondEq, val1)
			.And()
			.Where("int_array2", CondEq, val2)
			.And()
			.Where("int_array3", CondEq, val3)
			.And()
			.Where("int_array4", CondEq, val4)
			.EqualPositions({"int_array1", "int_array2", "int_array3", "int_array4"});
	};
	LowSelectivityItemsCounter<90> counter(state, 100);
	benchQuery(qGen, state, counter);
}

void EqualPositions::EqPos2GroupingSubArray(State& state) {
	auto qGen = [&]() {
		int val1 = rand() % (kArray1End - kArray1Start) + kArray1Start + (kArray1End - kArray1Start) / 5;
		int val2 = rand() % (kArray2End - kArray2Start) + kArray2Start + (kArray2End - kArray2Start) / 10;

		return reindexer::Query(nsdef_.name)
			.Where("array1.int_in_obj1", CondEq, val1)
			.And()
			.Where("array1.int_in_obj2", CondEq, val2)
			.EqualPositions({"array1[#].int_in_obj1", "array1[#].int_in_obj2"});
	};
	LowSelectivityItemsCounter<50> counter(state, 100);
	benchQuery(qGen, state, counter);
}

reindexer::Item EqualPositions::MakeItem(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	std::ignore = item.Unsafe();

	wrSer_.Reset();
	{
		reindexer::JsonBuilder json(wrSer_);
		auto genArray = [&](reindexer::JsonBuilder& node, int count, int seqStart, int seqEnd, std::string_view name) {
			auto arr = node.Array(name);
			for (size_t i = 0, s = count; i < s; ++i) {
				arr.Put(reindexer::TagName::Empty(), rand() % (seqEnd - seqStart) + seqStart);
			}
		};

		const auto id = id_++;
		json.Put("id", id);
		genArray(json, kArray1Count, kArray1Start, kArray1End, "int_array_index1");
		genArray(json, kArray2Count, kArray2Start, kArray2End, "int_array_index2");
		genArray(json, kArray1Count, kArray1Start, kArray1End, "int_array1");
		genArray(json, kArray2Count, kArray2Start, kArray2End, "int_array2");
		genArray(json, kArray1Count, kArray1Start, kArray1End, "int_array3");
		genArray(json, kArray2Count, kArray2Start, kArray2End, "int_array4");

		{
			auto arrayNode = json.Array("array1");
			{
				for (unsigned i = 0; i < 10; i++) {
					auto objNode2 = arrayNode.Object();
					genArray(objNode2, kArray1Count, kArray1Start, kArray1End, "int_in_obj1");
					genArray(objNode2, kArray2Count, kArray2Start, kArray2End, "int_in_obj2");
				}
			}
		}
	}
	const auto err = item.FromJSON(wrSer_.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
	return item;
}

reindexer::Error EqualPositions::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}
	return {};
}
