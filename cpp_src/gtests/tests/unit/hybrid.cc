#include "gtests/tests/fixtures/hybrid.h"
#include "core/cjson/jsonbuilder.h"
#include "gtests/tools.h"
#include "tools/fsops.h"

#if defined(REINDEX_WITH_TSAN) || defined(REINDEX_WITH_ASAN)
constexpr static size_t kDimension = 32;
constexpr static size_t kMaxElements = 500;
#else
constexpr static size_t kDimension = 512;
constexpr static size_t kMaxElements = 2'000;
#endif

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
	rx_unused = reindexer::fs::RmDirAll(dir);
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
	std::array<float, kDimension> buf;
	struct {
		std::string_view name;
		reindexer::KnnSearchParams params;
	} knnFields[]{{kFieldNameIP, reindexer::HnswSearchParams{}.K(kMaxElements / 4).Ef(kMaxElements / 4)},
				  {kFieldNameCos, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)},
				  {kFieldNameL2, reindexer::IvfSearchParams{}.K(kMaxElements / 4).NProbe(5)}};

	for (const auto& knnField : knnFields) {
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
