#include "gtests/tests/fixtures/hybrid.h"
#include <gmock/gmock.h>
#include "core/cjson/jsonbuilder.h"
#include "gtests/tools.h"
#include "tools/fsops.h"

static constexpr auto kBasicTimeout = std::chrono::seconds(200);

reindexer::Item HybridTest::newItem(int id) {
	reindexer::WrSerializer ser;
	{
		reindexer::JsonBuilder json(ser);
		json.Put(kFieldNameId, id);
		if (rand() % 10 != 0) {
			json.Put(kFieldNameFt, "trampampam " + rt.RandString());
		}
		std::array<float, kDimension> buf;
		for (const auto& fieldName : {kFieldNameIP, kFieldNameCos, kFieldNameL2}) {
			if (rand() % 10 != 0) {
				rndFloatVector(buf);
				json.Array(fieldName, std::span<const float>(buf));
			}
		}
	}
	auto item = rt.NewItem(kNsName);
	const auto err = item.FromJSON(ser.Slice());
	if (!err.ok()) {
		throw err;
	}
	return item;
}

void HybridTest::SetUp() {
	constexpr static size_t kM = 16;
	constexpr static size_t kEfConstruction = 200;

	auto dir = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "/HybridTest");
	std::ignore = reindexer::fs::RmDirAll(dir);
	rt.Connect("builtin://" + dir);
	rt.OpenNamespace(kNsName);
	rt.DefineNamespaceDataset(
		kNsName, {
					 IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts{}.PK(), 0},
					 IndexDeclaration{kFieldNameFt, "text", "string", IndexOpts(), 0},
					 IndexDeclaration{kFieldNameIP, "hnsw", "float_vector",
									  IndexOpts{}.SetFloatVector(IndexHnsw, FloatVectorIndexOpts{}
																				.SetDimension(kDimension)
																				.SetStartSize(100)
																				.SetM(kM)
																				.SetEfConstruction(kEfConstruction)
																				.SetMetric(reindexer::VectorMetric::InnerProduct)),
									  0},
					 IndexDeclaration{kFieldNameCos, "ivf", "float_vector",
									  IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																			   .SetDimension(kDimension)
																			   .SetNCentroids(kMaxElements / 50)
																			   .SetMetric(reindexer::VectorMetric::Cosine)),
									  0},
					 IndexDeclaration{kFieldNameL2, "ivf", "float_vector",
									  IndexOpts{}.SetFloatVector(IndexIvf, FloatVectorIndexOpts{}
																			   .SetDimension(kDimension)
																			   .SetNCentroids(kMaxElements / 50)
																			   .SetMetric(reindexer::VectorMetric::L2)),
									  0},
				 });

	for (size_t i = 0; i < kMaxElements; ++i) {
		auto item = newItem(i);
		rt.Upsert(kNsName, item);
	}
}

TEST_F(HybridTest, Queries) {
	static std::array<float, kDimension> buf;

	for (const auto& knnField : knnFields_) {
		rndFloatVector(buf);
		auto result = rt.Select(reindexer::Query{kNsName}
									.Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString())
									.WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params)
									.Sort(fmt::format("5 * rank({}) + 4 * rank({}) + 15", kFieldNameFt, knnField.name), false)
									.WithRank());

		rndFloatVector(buf);
		result = rt.Select(
			reindexer::Query{kNsName}
				.Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString())
				.Or()
				.WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params)
				.Sort(fmt::format("34 * 8 - 500 * rank({}, 0) - 29 + 1.0e+3 * rank({}, 1.0e+17) + 15", knnField.name, kFieldNameFt), false)
				.WithRank());

		rndFloatVector(buf);
		result = rt.Select(reindexer::Query{kNsName}
							   .Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString())
							   .WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params)
							   .Sort("RRF()", false)
							   .WithRank());

		rndFloatVector(buf);
		result = rt.Select(reindexer::Query{kNsName}
							   .Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString())
							   .Or()
							   .WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params)
							   .Sort("RRF(rank_const = 3)", false)
							   .WithRank());
	}
}

void HybridTest::check(const reindexer::Query& q) const {
	const auto qRes = rt.Select(q);
	const auto& sorting = q.GetSortingEntries();
	auto it = qRes.begin();
	const auto end = qRes.end();
	if (it == end) {
		return;
	}
	auto prevRank = it.GetItemRefRanked().Rank();
	++it;
	if (sorting.empty() || sorting[0].desc) {
		while (it != end) {
			const auto currRank = it.GetItemRefRanked().Rank();
			EXPECT_LE(currRank, prevRank) << q.GetSQL();
			prevRank = currRank;
			++it;
		}
	} else {
		while (it != end) {
			const auto currRank = it.GetItemRefRanked().Rank();
			EXPECT_GE(currRank, prevRank) << q.GetSQL();
			prevRank = currRank;
			++it;
		}
	}
}

std::string HybridTest::checkFailed(const reindexer::Query& q) const {
	reindexer::QueryResults qr;
	auto err = rt.reindexer->WithTimeout(kBasicTimeout).Select(q, qr);
	EXPECT_FALSE(err.ok()) << q.GetSQL();
	return err.what();
}

void HybridTest::checkFailed(const reindexer::Query& q, std::string_view expectErr) const {
	const auto err = checkFailed(q);
	EXPECT_EQ(err, expectErr) << q.GetSQL();
}

void HybridTest::checkFailedRegex(const reindexer::Query& q, std::string_view expectErrRegex) const {
	const auto err = checkFailed(q);
	EXPECT_THAT(err, testing::MatchesRegex(expectErrRegex)) << q.GetSQL();
}

reindexer::Query HybridTest::makeHybridQuery() {
	static std::array<float, kDimension> buf;
	rndFloatVector(buf);
	currentKnnField_ = rand() % std::size(knnFields_);
	const auto& knnField = knnFields_[currentKnnField_];
	auto q = reindexer::Query{kNsName}.WithRank().Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString());
	if (rand() % 2) {
		q.Or();
	}
	q.WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params);
	return q;
}

reindexer::Query HybridTest::makeKnnQuery() const {
	static std::array<float, kDimension> buf;
	rndFloatVector(buf);
	const auto& knnField = randOneOf(knnFields_);
	return reindexer::Query{kNsName}.WhereKNN(knnField.name, reindexer::ConstFloatVectorView{buf}, knnField.params).WithRank();
}

reindexer::Query HybridTest::makeFtQuery() const {
	return reindexer::Query{kNsName}.Where(kFieldNameFt, CondEq, "trampampam " + rt.RandString()).WithRank();
}

std::string HybridTest::rndReranker() const {
	std::stringstream reranker;
	if (rand() % 2) {
		reranker << "RRF(";
		if (rand() % 2) {
			reranker << "rank_const = ";
			reranker << std::to_string(1 + rand() % 1'000);
		}
		reranker << ')';
	} else {
		reranker << (rand() % 20'000 - 10'000);
		reranker << " * rank(" << kFieldNameFt << ") + ";
		reranker << (rand() % 20'000 - 10'000);
		const auto& knnField = knnFields_[currentKnnField_];
		reranker << " * rank(" << knnField.name << ") + ";
		reranker << (rand() % 20'000 - 10'000);
	}
	return reranker.str();
}

TEST_F(HybridTest, Merge) {
	check(makeHybridQuery().Merge(makeHybridQuery()));
	check(makeHybridQuery().Limit(10).Merge(makeHybridQuery()));
	check(makeHybridQuery().Offset(10).Merge(makeHybridQuery()));
	check(makeHybridQuery().Offset(10).Limit(10).Merge(makeHybridQuery()));
	check(makeHybridQuery().Sort(rndReranker(), true).Merge(makeHybridQuery()));
	check(makeHybridQuery().Merge(makeHybridQuery().Sort(rndReranker(), true)));
	check(makeHybridQuery().Sort(rndReranker(), false).Merge(makeHybridQuery().Sort(rndReranker(), false)));
	check(makeHybridQuery().Sort(rndReranker(), true).Merge(makeHybridQuery().Sort(rndReranker(), true)));

	checkFailed(makeHybridQuery().Merge(makeHybridQuery().Offset(10)), "Limit and offset in inner merge query is not allowed");
	checkFailed(makeHybridQuery().Merge(makeHybridQuery().Limit(10)), "Limit and offset in inner merge query is not allowed");

	checkFailed(makeHybridQuery().Sort(kFieldNameId, true).Merge(makeHybridQuery()),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
	checkFailed(makeHybridQuery().Merge(makeHybridQuery().Sort(kFieldNameId, true)),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
	checkFailed(makeHybridQuery().Sort(rndReranker(), true).Sort(kFieldNameId, true).Merge(makeHybridQuery()),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
	checkFailed(makeHybridQuery().Sort(kFieldNameId, true).Merge(makeHybridQuery().Sort(rndReranker(), true)),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
	checkFailed(makeHybridQuery().Merge(makeHybridQuery().Sort(rndReranker(), true).Sort(kFieldNameId, true)),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");
	checkFailed(makeHybridQuery().Sort(rndReranker(), true).Merge(makeHybridQuery().Sort(kFieldNameId, true)),
				"In hybrid query ordering expression should be 'RRF()' or in form 'a * rank(index1) + b * rank(index2) + c'");

	checkFailed(makeHybridQuery().Sort(rndReranker(), false).Merge(makeHybridQuery().Sort(rndReranker(), true)),
				"All merging queries should have the same ordering (ASC or DESC)");
	checkFailed(makeHybridQuery().Sort(rndReranker(), true).Merge(makeHybridQuery().Sort(rndReranker(), false)),
				"All merging queries should have the same ordering (ASC or DESC)");
	checkFailed(makeHybridQuery().Merge(makeHybridQuery().Sort(rndReranker(), false)),
				"All merging queries should have the same ordering (ASC or DESC)");
	checkFailed(makeHybridQuery().Sort(rndReranker(), false).Merge(makeHybridQuery()),
				"All merging queries should have the same ordering (ASC or DESC)");

	checkFailed(makeHybridQuery().Merge(makeFtQuery()),
				"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
				"time: 'hybrid query' VS 'fulltext query'");
	checkFailedRegex(
		makeHybridQuery().Merge(makeKnnQuery()),
		"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
		"time: 'hybrid query' VS 'knn with .* metric query'");
	checkFailed(makeFtQuery().Merge(makeHybridQuery()),
				"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
				"time: 'fulltext query' VS 'hybrid query'");
	checkFailedRegex(
		makeKnnQuery().Merge(makeHybridQuery()),
		"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
		"time: 'knn with .* metric query' VS 'hybrid query'");
	checkFailedRegex(
		makeKnnQuery().Merge(makeFtQuery()),
		"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
		"time: 'knn with .* metric query' VS 'fulltext query'");
	checkFailedRegex(
		makeFtQuery().Merge(makeKnnQuery()),
		"In merge query without sorting all subqueries should contain fulltext or knn with the same metric conditions at the same "
		"time: 'fulltext query' VS 'knn with .* metric query'");
}
