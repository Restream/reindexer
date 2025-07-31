#include <benchmark/benchmark.h>

#include "helpers.h"
#include "knn_fixture.h"

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char** argv) {
	using namespace std::string_view_literals;
	namespace knn_bench = reindexer::knn_bench;

	auto DB = InitBenchDB("knn_bench_test"sv);

	knn_bench::KnnBench<knn_bench::IndexType::Hnsw, reindexer::VectorMetric::L2> hnswL2(DB.get(), "hnsw_l2_bench"sv);
	auto err = hnswL2.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	knn_bench::KnnBench<knn_bench::IndexType::Hnsw, reindexer::VectorMetric::Cosine> hnswCosine(DB.get(), "hnsw_cosine_bench"sv);
	err = hnswCosine.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	knn_bench::KnnBench<knn_bench::IndexType::Hnsw, reindexer::VectorMetric::InnerProduct> hnswInnerProduct(DB.get(),
																											"hnsw_inner_product_bench"sv);
	err = hnswInnerProduct.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	knn_bench::KnnBench<knn_bench::IndexType::Ivf, reindexer::VectorMetric::L2> ivfL2(DB.get(), "ivf_l2_bench"sv);
	err = ivfL2.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	knn_bench::KnnBench<knn_bench::IndexType::Ivf, reindexer::VectorMetric::Cosine> ivfCosine(DB.get(), "ivf_cosine_bench"sv);
	err = ivfCosine.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	knn_bench::KnnBench<knn_bench::IndexType::Ivf, reindexer::VectorMetric::InnerProduct> ivfInnerProduct(DB.get(),
																										  "ivf_inner_product_bench"sv);
	err = ivfInnerProduct.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
		return 1;
	}

	hnswL2.RegisterAllCases();
	hnswCosine.RegisterAllCases();
	hnswInnerProduct.RegisterAllCases();
	ivfL2.RegisterAllCases();
	ivfCosine.RegisterAllCases();
	ivfInnerProduct.RegisterAllCases();

	::benchmark::RunSpecifiedBenchmarks();
}
