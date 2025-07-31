#pragma once

#include <random>

#include "base_fixture.h"
#include "ft_base.h"

namespace reindexer::knn_bench {

enum class [[nodiscard]] IndexType { Hnsw, Ivf };
enum class [[nodiscard]] KnnParams { K, K_Radius, Radius };

template <IndexType, VectorMetric>
class [[nodiscard]] KnnBench : private BaseFixture, private bench::FullTextBase {
public:
	~KnnBench() override = default;
	KnnBench(Reindexer* db, std::string_view name);

	Error Initialize() override {
		const auto err = BaseFixture::Initialize();
		if (!err.ok()) {
			return err;
		}
		return bench::FullTextBase::Initialize();
	}
	void RegisterAllCases();

private:
	template <KnnParams>
	class ItemsCounter;

	Item MakeItem(State&) override;
	Item MakeItem(State&, int id);

	void Fill(State&);
	void FillFtIndex(State&);
	void DeleteFtIndex(State&);
	void Insert(State&);
	void Update(State&);
	template <KnnParams>
	void Knn(State&);
	template <KnnParams>
	void KnnWithVectors(State&);
	template <KnnParams>
	void KnnWithCondition(State&);
	template <KnnParams>
	void KnnWith2Conditions(State&);
	template <KnnParams>
	void AndHybridRrf(State&);
	template <KnnParams>
	void OrHybridRrf(State&);
	template <KnnParams>
	void AndHybridLinear(State&);
	template <KnnParams>
	void OrHybridLinear(State&);
	void Sleep(State&);

	std::mt19937 randomEngine_{1};
	std::uniform_int_distribution<int> randomGenerator_{};
};

}  // namespace reindexer::knn_bench
