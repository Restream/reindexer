#include "update_items.h"
#include "core/cjson/jsonbuilder.h"

reindexer::Error UpdateItems::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}
	return {};
}

void UpdateItems::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert", &UpdateItems::Insert, this)->Iterations(1);
	Register("ScalarIndexLimit1", &UpdateItems::ScalarIndexLimit1, this);
	Register("ScalarIndexLimit1000", &UpdateItems::ScalarIndexLimit1000, this);
	Register("ScalarSparseIndexLimit1", &UpdateItems::ScalarSparseIndexLimit1, this);
	Register("ScalarSparseIndexLimit1000", &UpdateItems::ScalarSparseIndexLimit1000, this);
	Register("ScalarLimit1", &UpdateItems::ScalarLimit1, this);
	Register("ScalarLimit1000", &UpdateItems::ScalarLimit1000, this);
	Register("ArrayIndexLimit1", &UpdateItems::ArrayIndexLimit1, this);
	Register("ArrayIndexLimit1000", &UpdateItems::ArrayIndexLimit1000, this);
	Register("ArraySparseIndexLimit1", &UpdateItems::ArraySparseIndexLimit1, this);
	Register("ArraySparseIndexLimit1000", &UpdateItems::ArraySparseIndexLimit1000, this);
	Register("ArrayLimit1", &UpdateItems::ArrayLimit1, this);
	Register("ArrayLimit1000", &UpdateItems::ArrayLimit1000, this);
	Register("Array2IndexLimit1", &UpdateItems::Array2IndexLimit1, this);
	Register("Array2IndexLimit1000", &UpdateItems::Array2IndexLimit1000, this);
	Register("Array2IndexSparseLimit1", &UpdateItems::Array2IndexSparseLimit1, this);
	Register("Array2IndexSparseLimit1000", &UpdateItems::Array2IndexSparseLimit1000, this);
	Register("Array2Limit1", &UpdateItems::Array2Limit1, this);
	Register("Array2Limit1000", &UpdateItems::Array2Limit1000, this);
	Register("ThreeIndexFieldLimit1", &UpdateItems::ThreeIndexFieldLimit1, this);
	Register("ThreeIndexFieldLimit1000", &UpdateItems::ThreeIndexFieldLimit1000, this);
	Register("ThreeIndexSparseFieldLimit1", &UpdateItems::ThreeIndexSparseFieldLimit1, this);
	Register("ThreeIndexSparseFieldLimit1000", &UpdateItems::ThreeIndexSparseFieldLimit1000, this);
	Register("ThreeFieldLimit1", &UpdateItems::ThreeFieldLimit1, this);
	Register("ThreeFieldLimit1000", &UpdateItems::ThreeFieldLimit1000, this);
	Register("NineFieldLimit1", &UpdateItems::NineFieldLimit1, this);
	Register("NineFieldLimit1000", &UpdateItems::NineFieldLimit1000, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

static std::vector<std::string> RandStrLocal(unsigned stringCount, unsigned stringMaxLen) {
	std::vector<std::string> ret;
	ret.reserve(stringCount);
	for (size_t i = 0; i < stringCount; ++i) {
		ret.emplace_back(randString(rand() % stringMaxLen + 2));
	}
	return ret;
}

reindexer::Item UpdateItems::MakeItem(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	std::ignore = item.Unsafe();

	wrSer_.Reset();
	{
		reindexer::JsonBuilder json(wrSer_);
		const auto id = id_++;
		json.Put(kFieldId, id);
		json.Put(kFieldScalar, randString(kStringLen));
		json.Put(kFieldScalarSparse, randString(kStringLen));
		json.Put(kFieldScalarNotindex, randString(kStringLen));

		json.Array<std::string>(kFieldArray, RandStrLocal(rand() % kMaxArrayLen, kStringLen));
		json.Array<std::string>(kFieldArraySparse, RandStrLocal(rand() % kMaxArrayLen, kStringLen));
		json.Array<std::string>(kFieldArrayNotindex, RandStrLocal(rand() % kMaxArrayLen, kStringLen));
		{
			auto node = json.Array(kFieldArraySub[0], kFirstArrayLen);
			for (unsigned i = 0; i < kFirstArrayLen; i++) {
				auto obj = node.Object();
				obj.Array<std::string>(kFieldArraySub[1], RandStrLocal(rand() % kSecondArrayMaxLen + 2, kStringLen));
			}
		}
		{
			auto node = json.Array(kFieldArraySubSparse[0], kFirstArrayLen);
			for (unsigned i = 0; i < kFirstArrayLen; i++) {
				auto obj = node.Object();
				obj.Array<std::string>(kFieldArraySubSparse[1], RandStrLocal(rand() % kSecondArrayMaxLen + 2, kStringLen));
			}
		}
		{
			auto node = json.Array(kFieldArraySubNotIndex[0], kFirstArrayLen);
			for (unsigned i = 0; i < kFirstArrayLen; i++) {
				auto obj = node.Object();
				obj.Array<std::string>(kFieldArraySubNotIndex[1], RandStrLocal(rand() % kSecondArrayMaxLen + 2, kStringLen));
			}
		}
	}
	const auto err = item.FromJSON(wrSer_.Slice());
	if (!err.ok()) {
		state.SkipWithError(err.what());
	}
	return item;
}

void UpdateItems::Insert(State& state) { BaseFixture::Insert(state); }

void UpdateItems::scalarIndex(State& state, unsigned int limit) {
	auto qGen = [&]() { return reindexer::Query(nsdef_.name).Set(kFieldScalar, randString(kStringLen)).Limit(limit); };
	benchUpdate(qGen, state);
}

void UpdateItems::ScalarIndexLimit1(State& state) { scalarIndex(state, 1); }
void UpdateItems::ScalarIndexLimit1000(State& state) { scalarIndex(state, kCount1000); }

void UpdateItems::scalarSparseIndex(State& state, unsigned int limit) {
	auto qGen = [&]() { return reindexer::Query(nsdef_.name).Set(kFieldScalarSparse, randString(kStringLen)).Limit(limit); };
	benchUpdate(qGen, state);
}

void UpdateItems::ScalarSparseIndexLimit1(State& state) { scalarSparseIndex(state, 1); }
void UpdateItems::ScalarSparseIndexLimit1000(State& state) { scalarSparseIndex(state, kCount1000); }

void UpdateItems::scalar(State& state, unsigned int limit) {
	auto qGen = [&]() { return reindexer::Query(nsdef_.name).Set(kFieldScalarNotindex, randString(kStringLen)).Limit(limit); };
	benchUpdate(qGen, state);
}

void UpdateItems::ScalarLimit1(State& state) { scalar(state, 1); }
void UpdateItems::ScalarLimit1000(State& state) { scalar(state, kCount1000); }

void UpdateItems::arrayIndex(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		return reindexer::Query(nsdef_.name).Set(kFieldArray, array).Limit(limit);
	};
	benchUpdate(qGen, state);
}

void UpdateItems::ArrayIndexLimit1(State& state) { arrayIndex(state, 1); }
void UpdateItems::ArrayIndexLimit1000(State& state) { arrayIndex(state, kCount1000); }

void UpdateItems::arraySparseIndex(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		return reindexer::Query(nsdef_.name).Set(kFieldArraySparse, array).Limit(limit);
	};
	benchUpdate(qGen, state);
}

void UpdateItems::ArraySparseIndexLimit1(State& state) { arraySparseIndex(state, 1); }
void UpdateItems::ArraySparseIndexLimit1000(State& state) { arraySparseIndex(state, kCount1000); }

void UpdateItems::array(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		return reindexer::Query(nsdef_.name).Set(kFieldArrayNotindex, array).Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::ArrayLimit1(State& state) { array(state, 1); }
void UpdateItems::ArrayLimit1000(State& state) { array(state, kCount1000); }

void UpdateItems::array2Index(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto path = getPathIndex<2>(kFieldArraySub, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val = randString(rand() % kStringLen + 1);
		return reindexer::Query(nsdef_.name).Set(path, val).Limit(limit);
	};
	benchUpdate(qGen, state);
}

void UpdateItems::Array2IndexLimit1(State& state) { array2Index(state, 1); }
void UpdateItems::Array2IndexLimit1000(State& state) { array2Index(state, kCount1000); }

void UpdateItems::array2IndexSparse(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto path = getPathIndex<2>(kFieldArraySubSparse, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val = randString(rand() % kStringLen + 1);
		return reindexer::Query(nsdef_.name).Set(path, val).Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::Array2IndexSparseLimit1(State& state) { array2IndexSparse(state, 1); }
void UpdateItems::Array2IndexSparseLimit1000(State& state) { array2IndexSparse(state, kCount1000); }

void UpdateItems::array2(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto path = getPathIndex<2>(kFieldArraySubNotIndex, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		int val = rand() % 1000;
		return reindexer::Query(nsdef_.name).Set(path, val).Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::Array2Limit1(State& state) { array2(state, 1); }
void UpdateItems::Array2Limit1000(State& state) { array2(state, kCount1000); }

void UpdateItems::threeIndexField(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path = getPathIndex<2>(kFieldArraySub, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val = randString(rand() % kStringLen + 1);
		return reindexer::Query(nsdef_.name).Set(kFieldScalar, randString(kStringLen)).Set(kFieldArray, array).Set(path, val).Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::ThreeIndexFieldLimit1(State& state) { threeIndexField(state, 1); }
void UpdateItems::ThreeIndexFieldLimit1000(State& state) { threeIndexField(state, kCount1000); }

void UpdateItems::threeIndexSparseField(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path = getPathIndex<2>(kFieldArraySubSparse, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val = randString(rand() % kStringLen + 2);
		return reindexer::Query(nsdef_.name)
			.Set(kFieldScalarSparse, randString(kStringLen))
			.Set(kFieldArraySparse, array)
			.Set(path, val)
			.Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::ThreeIndexSparseFieldLimit1(State& state) { threeIndexSparseField(state, 1); }
void UpdateItems::ThreeIndexSparseFieldLimit1000(State& state) { threeIndexSparseField(state, kCount1000); }

void UpdateItems::threeField(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path = getPathIndex<2>(kFieldArraySubNotIndex, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val = randString(rand() % kStringLen + 2);
		return reindexer::Query(nsdef_.name)
			.Set(kFieldScalar, randString(kStringLen))
			.Set(kFieldArrayNotindex, array)
			.Set(path, val)
			.Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::ThreeFieldLimit1(State& state) { threeField(state, 1); }
void UpdateItems::ThreeFieldLimit1000(State& state) { threeField(state, kCount1000); }

void UpdateItems::nineField(State& state, unsigned int limit) {
	auto qGen = [&]() {
		auto array1 = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path1 = getPathIndex<2>(kFieldArraySubNotIndex, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val1 = randString(rand() % kStringLen + 2);
		auto array2 = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path2 = getPathIndex<2>(kFieldArraySubSparse, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val2 = randString(rand() % kStringLen + 2);
		auto array3 = RandStrLocal(rand() % kMaxArrayLen + 2, kStringLen);
		auto path3 = getPathIndex<2>(kFieldArraySub, std::array<int, 2>{int(rand() % kFirstArrayLen), -1});
		auto val3 = randString(rand() % kStringLen + 2);

		return reindexer::Query(nsdef_.name)
			.Set(kFieldScalar, randString(kStringLen))
			.Set(kFieldArray, array3)
			.Set(path3, val3)
			.Set(kFieldScalarSparse, randString(kStringLen))
			.Set(kFieldArraySparse, array2)
			.Set(path2, val2)
			.Set(kFieldScalarNotindex, randString(kStringLen))
			.Set(kFieldArrayNotindex, array1)
			.Set(path1, val1)
			.Limit(limit);
	};
	benchUpdate(qGen, state);
}
void UpdateItems::NineFieldLimit1(State& state) { nineField(state, 1); }
void UpdateItems::NineFieldLimit1000(State& state) { nineField(state, kCount1000); }
