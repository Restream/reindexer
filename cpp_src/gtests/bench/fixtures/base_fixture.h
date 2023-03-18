#pragma once

#include <benchmark/benchmark.h>

#include <stddef.h>
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "allocs_tracker.h"
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
		assert(db_);
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

	virtual reindexer::Item MakeItem() = 0;
	void WaitForOptimization();

	template <typename Fn, typename Cl>
	Benchmark* Register(const std::string& name, Fn fn, Cl* cl) {
		return benchmark::RegisterBenchmark(((useBenchamrkPrefixName_ ? nsdef_.name + "/" : "") + name).c_str(), std::bind(fn, cl, _1));
	}

	std::string RandString();

protected:
	const std::string letters = "abcdefghijklmnopqrstuvwxyz";
	Reindexer* db_;
	NamespaceDef nsdef_;
	std::shared_ptr<Sequence> id_seq_;
	bool useBenchamrkPrefixName_;
};
