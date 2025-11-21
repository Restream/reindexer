#pragma once

#include <benchmark/benchmark.h>

#include <stddef.h>
#include <memory>

#include "allocs_tracker.h"
#include "core/namespacedef.h"
#include "core/reindexer.h"
#include "sequence.h"

using std::placeholders::_1;

using benchmark::State;
using benchmark::internal::Benchmark;

using reindexer::NamespaceDef;
using reindexer::Reindexer;

class [[nodiscard]] BaseFixture {
public:
	virtual ~BaseFixture() {
		assertrx(db_);
		auto err = db_->CloseNamespace(nsdef_.name);
		if (!err.ok()) {
			std::cerr << "Error while closing namespace '" << nsdef_.name << "'. Reason: " << err.what() << std::endl;
		}
	}

	BaseFixture(Reindexer* db, std::string_view name, size_t maxItems, size_t idStart = 1, bool useBenchamrkPrefixName = true)
		: db_(db),
		  nsdef_(name),
		  id_seq_(std::make_shared<Sequence>(idStart, maxItems, 1)),
		  useBenchamrkPrefixName_(useBenchamrkPrefixName) {}

	virtual reindexer::Error Initialize();

	void RegisterAllCases();
	struct [[nodiscard]] AllowEmptyResult {
		void operator()(const reindexer::QueryResults&) const noexcept {}
	};
	static constexpr AllowEmptyResult allowEmptyResult{};

protected:
	template <unsigned maxPercentage = 10>
	class [[nodiscard]] LowSelectivityItemsCounter {
	public:
		explicit LowSelectivityItemsCounter(State& state, unsigned int minIteration = 10) noexcept
			: state_{state}, minIteration_(minIteration) {}
		virtual ~LowSelectivityItemsCounter() {
			if (state_.iterations() < minIteration_) {
				return;
			}
			const auto percentageOfEmptyResults = (100 * emptyResultsCount_) / state_.iterations();
			if (percentageOfEmptyResults > maxPercentage) [[unlikely]] {
				const auto err = "Percentage of empty results " + std::to_string(percentageOfEmptyResults) + "% is more than " +
								 std::to_string(maxPercentage) + '%';
				state_.SkipWithError(err.c_str());
			}
		}
		void operator()(const reindexer::QueryResults& qres) noexcept {
			if (qres.Count() == 0) {
				++emptyResultsCount_;
			}
		}

	protected:
		State& state_;

	private:
		size_t emptyResultsCount_{0};
		unsigned int minIteration_{10};
	};

	void Insert(State& state);
	void Update(State& state);

	virtual reindexer::Item MakeItem(benchmark::State&) = 0;
	void WaitForOptimization();

	template <typename Fn, typename Cl>
	Benchmark* Register(const std::string& name, Fn fn, Cl* cl) {
		std::string tn(useBenchamrkPrefixName_ ? nsdef_.name + "/" : "");
		tn += name;
		return benchmark::RegisterBenchmark(tn.c_str(), std::bind(fn, cl, _1));
	}

	template <typename Fn, typename... Args>
	Benchmark* RegisterF(const std::string& name, Fn&& f, Args&&... args) {
		return benchmark::RegisterBenchmark(((useBenchamrkPrefixName_ ? nsdef_.name + "/" : "") + name).c_str(), std::forward<Fn>(f),
											std::forward<Args>(args)...);
	}

	RX_ALWAYS_INLINE static void checkNotEmpty(const reindexer::QueryResults& qres, benchmark::State& state) {
		if (!qres.Count()) [[unlikely]] {
			state.SkipWithError("Results does not contain any value");
		}
	}
	void benchQuery(const reindexer::Query& q, benchmark::State& state) {
		benchmark::AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			reindexer::QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) [[unlikely]] {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}
	}
	void benchQuery(auto queryGenerator, benchmark::State& state) {
		benchmark::AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			reindexer::QueryResults qres;
			auto err = db_->Select(queryGenerator(), qres);
			if (!err.ok()) [[unlikely]] {
				state.SkipWithError(err.what());
			}
			checkNotEmpty(qres, state);
		}
	}
	void benchQuery(const reindexer::Query& q, benchmark::State& state, auto& itemsCounter) {
		benchmark::AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			reindexer::QueryResults qres;
			auto err = db_->Select(q, qres);
			if (!err.ok()) [[unlikely]] {
				state.SkipWithError(err.what());
			}
			itemsCounter(qres);
		}
	}
	void benchQuery(auto queryGenerator, benchmark::State& state, auto& itemsCounter) {
		benchmark::AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			reindexer::QueryResults qres;
			auto err = db_->Select(queryGenerator(), qres);
			if (!err.ok()) [[unlikely]] {
				state.SkipWithError(err.what());
			}
			itemsCounter(qres);
		}
	}
	struct NoTotal {
		RX_ALWAYS_INLINE static void Apply(reindexer::Query&) noexcept {}
	};
	struct ReqTotal {
		RX_ALWAYS_INLINE static void Apply(reindexer::Query& q) noexcept { q.ReqTotal(); }
	};
	struct CachedTotal {
		RX_ALWAYS_INLINE static void Apply(reindexer::Query& q) noexcept { q.CachedTotal(); }
	};

	std::string RandString();

	const std::string letters = "abcdefghijklmnopqrstuvwxyz";
	Reindexer* db_;
	NamespaceDef nsdef_;
	std::shared_ptr<Sequence> id_seq_;
	bool useBenchamrkPrefixName_;
};
