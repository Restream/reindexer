#pragma once

#include <benchmark/benchmark.h>

#include <stddef.h>
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "allocs_tracker.h"
#include "core/keyvalue/variant.h"
#include "core/namespacedef.h"
#include "core/reindexer.h"
#include "sequence.h"

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::placeholders::_1;

using benchmark::State;
using benchmark::internal::Benchmark;

using reindexer::Error;
using reindexer::IndexDef;
using reindexer::Item;
using reindexer::Variant;
using reindexer::VariantArray;
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

	BaseFixture(Reindexer* db, const string& name, size_t maxItems, size_t idStart = 1, bool useBenchamrkPrefixName = true)
		: db_(db),
		  nsdef_(name),
		  id_seq_(std::make_shared<Sequence>(idStart, maxItems, 1)),
		  useBenchamrkPrefixName_(useBenchamrkPrefixName) {}

	BaseFixture(Reindexer* db, NamespaceDef& nsdef, size_t maxItems, size_t idStart = 1, bool useBenchamrkPrefixName = true)
		: db_(db), nsdef_(nsdef), id_seq_(make_shared<Sequence>(idStart, maxItems, 1)), useBenchamrkPrefixName_(useBenchamrkPrefixName) {}

	virtual Error Initialize();

	virtual void RegisterAllCases();

protected:
	void Insert(State& state);
	void Update(State& state);

	virtual Item MakeItem() = 0;

	template <typename Fn, typename Cl>
	Benchmark* Register(const string& name, Fn fn, Cl* cl) {
		return benchmark::RegisterBenchmark(((useBenchamrkPrefixName_ ? nsdef_.name + "/" : "") + name).c_str(), std::bind(fn, cl, _1));
	}

protected:
	Reindexer* db_;
	NamespaceDef nsdef_;
	shared_ptr<Sequence> id_seq_;
	bool useBenchamrkPrefixName_;
};
