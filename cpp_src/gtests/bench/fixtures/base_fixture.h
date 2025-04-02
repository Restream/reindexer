#pragma once

#include <benchmark/benchmark.h>

#include <stddef.h>
#include <memory>

#include "core/namespacedef.h"
#include "core/reindexer.h"
#include "sequence.h"

using std::placeholders::_1;

using benchmark::State;
using benchmark::internal::Benchmark;

using reindexer::NamespaceDef;
using reindexer::Reindexer;

class BaseFixture {
public:
	virtual ~BaseFixture() {
		assertrx(db_);
		auto err = db_->CloseNamespace(nsdef_.name);
		if (!err.ok()) {
			std::cerr << "Error while closing namespace '" << nsdef_.name << "'. Reason: " << err.what() << std::endl;
		}
	}

	BaseFixture(Reindexer* db, const std::string& name, size_t maxItems, size_t idStart = 1, bool useBenchamrkPrefixName = true)
		: db_(db),
		  nsdef_(name),
		  id_seq_(std::make_shared<Sequence>(idStart, maxItems, 1)),
		  useBenchamrkPrefixName_(useBenchamrkPrefixName) {}

	virtual reindexer::Error Initialize();

	void RegisterAllCases();

protected:
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

	std::string RandString();

	const std::string letters = "abcdefghijklmnopqrstuvwxyz";
	Reindexer* db_;
	NamespaceDef nsdef_;
	std::shared_ptr<Sequence> id_seq_;
	bool useBenchamrkPrefixName_;
};
