#pragma once

#include <string>
#include "array"
#include "base_fixture.h"

class [[nodiscard]] UpdateItems : protected BaseFixture {
public:
	~UpdateItems() override = default;
	UpdateItems(Reindexer* db, std::string_view name, size_t maxItems) : BaseFixture(db, name, maxItems) {
		nsdef_.AddIndex(std::string(kFieldId), "hash", "int", IndexOpts().PK());
		nsdef_.AddIndex(std::string(kFieldScalar), "tree", "string", IndexOpts());
		nsdef_.AddIndex(std::string(kFieldScalarSparse), "tree", "string", IndexOpts().Sparse());
		nsdef_.AddIndex(std::string(kFieldArray), "tree", "string", IndexOpts().Array());
		nsdef_.AddIndex(std::string(kFieldArraySparse), "tree", "string", IndexOpts().Array().Sparse());
		nsdef_.AddIndex(getPath(kFieldArraySub), "tree", "string", IndexOpts().Array());
		nsdef_.AddIndex(getPath(kFieldArraySubSparse), "tree", "string", IndexOpts().Array().Sparse());
	}

	void RegisterAllCases();
	reindexer::Error Initialize() override;

private:
	reindexer::Item MakeItem(benchmark::State&) override;

	void Insert(State& state);

	void ScalarIndexLimit1(State& state);
	void ScalarIndexLimit1000(State& state);

	void ScalarSparseIndexLimit1(State& state);
	void ScalarSparseIndexLimit1000(State& state);

	void ScalarLimit1(State& state);
	void ScalarLimit1000(State& state);

	void ArrayIndexLimit1(State& state);
	void ArrayIndexLimit1000(State& state);

	void ArraySparseIndexLimit1(State& state);
	void ArraySparseIndexLimit1000(State& state);

	void ArrayLimit1(State& state);
	void ArrayLimit1000(State& state);

	void Array2IndexLimit1(State& state);
	void Array2IndexLimit1000(State& state);

	void Array2IndexSparseLimit1(State& state);
	void Array2IndexSparseLimit1000(State& state);

	void Array2Limit1(State& state);
	void Array2Limit1000(State& state);

	void ThreeIndexFieldLimit1(State& state);
	void ThreeIndexFieldLimit1000(State& state);

	void ThreeIndexSparseFieldLimit1(State& state);
	void ThreeIndexSparseFieldLimit1000(State& state);

	void ThreeFieldLimit1(State& state);
	void ThreeFieldLimit1000(State& state);

	void NineFieldLimit1(State& state);
	void NineFieldLimit1000(State& state);

private:
	void scalarIndex(State& state, unsigned int limit);
	void scalarSparseIndex(State& state, unsigned int limit);
	void scalar(State& state, unsigned int limit);

	void arrayIndex(State& state, unsigned int limit);
	void arraySparseIndex(State& state, unsigned int limit);
	void array(State& state, unsigned int limit);

	void array2Index(State& state, unsigned int limit);
	void array2IndexSparse(State& state, unsigned int limit);
	void array2(State& state, unsigned int limit);

	void threeIndexField(State& state, unsigned int limit);
	void threeIndexSparseField(State& state, unsigned int limit);
	void threeField(State& state, unsigned int limit);

	void nineField(State& state, unsigned int limit);

	std::string getPath(std::span<const std::string_view> data) {
		std::string path;
		bool isFirst = true;
		for (const auto& pp : data) {
			if (!isFirst) {
				path += '.';
			}
			path += pp;
			isFirst = false;
		}
		return path;
	}
	template <int N>
	std::string getPathIndex(const std::array<std::string_view, N>& data, const std::array<int, N>& index) {
		assert(data.size() == index.size());
		std::string path;
		bool isFirst = true;

		for (unsigned i = 0, sz = data.size(); i < sz; ++i) {
			if (!isFirst) {
				path += '.';
			}
			path += data[i];
			if (index[i] == -1) {
				path += "[*]";
			} else {
				path += '[';
				path += std::to_string(index[i]);
				path += ']';
			}
			isFirst = false;
		}
		return path;
	}

	reindexer::WrSerializer wrSer_;
	int id_ = 0;
	static const unsigned kMaxScalarValue = 10000;
	static const unsigned kCount1000 = 1000;
	static const unsigned kStringLen = 40;
	static const unsigned kMaxArrayLen = 100;
	static const unsigned kFirstArrayLen = 20;
	static const unsigned kSecondArrayMaxLen = 20;

	static constexpr std::string_view kFieldId = "id";
	static constexpr std::string_view kFieldScalar = "scalar";
	static constexpr std::string_view kFieldScalarSparse = "scalar_sparse";
	static constexpr std::string_view kFieldScalarNotindex = "scalar_notindex";
	static constexpr std::string_view kFieldArray = "array";
	static constexpr std::string_view kFieldArraySparse = "array_sparse";
	static constexpr std::string_view kFieldArrayNotindex = "array_notindex";
	static constexpr std::array<std::string_view, 2> kFieldArraySub = {"first", "second"};
	static constexpr std::array<std::string_view, 2> kFieldArraySubSparse = {"first_sparse", "second_sparse"};
	static constexpr std::array<std::string_view, 2> kFieldArraySubNotIndex = {"first_not_index", "second_not_index"};
};
