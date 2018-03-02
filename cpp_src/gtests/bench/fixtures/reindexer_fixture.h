#pragma once

#include <benchmark/benchmark.h>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>

#include "core/reindexer.h"

using std::string;
using std::make_shared;
using std::mutex;
using std::lock_guard;
using std::tuple;
using std::initializer_list;
using std::deque;
using std::shared_ptr;

using reindexer::Reindexer;
using reindexer::Item;
using reindexer::Error;
using reindexer::KeyRef;
using reindexer::KeyRefs;

using benchmark::State;
using benchmark::Fixture;

class Rndxr : public Fixture {
public:
	using IndexDeclaration = tuple<const char*, const char*, const char*, IndexOpts>;

	static string const defaultNamespace;				// = "test_items_bench";
	static string const defaultJoinNamespace;			// = "test_join_items"
	static string const defaultSimpleNamespace;			// = "test_items_simple"
	static string const defaultSimpleCmplxPKNamespace;  // = "test_items_simple_cmplx_pk"
	static string const defaultInsertNamespace;			// = "test_items_insert";
	static string const defaultStorage;					// = "/tmp/reindexer_bench"

public:
	Rndxr();

	void SetUp(State& state);
	void TearDown(State& state);

	shared_ptr<Reindexer> GetDB() { return reindexer_; }

	Error DefineNamespaceIndexes(const string& ns, initializer_list<const IndexDeclaration> fields);

protected:
	virtual Error FillTestItemsBench(unsigned start, unsigned count, int pkgsCount);
	virtual Error FillTestJoinItem(unsigned start, unsigned count);

	Error newTestItemBench(int id, int pkgCount, Item& item);
	Error newTestJoinItem(int id, Item& item);
	Error newTestSimpleItem(int id, Item& item);
	Error newTestSimpleCmplxPKItem(int id, Item& item);
	Error newTestInsertItem(int id, Item& item);

	KeyRefs randIntArr(int cnt, int start, int rng);
	string randString(const string prefix = "");

	const char* randName();
	const char* randDevice();
	const char* randLocation();
	const char* randAdjectives();

	Error PrepareDefaultNamespace();
	Error PrepareJoinNamespace();
	Error PrepareSimpleNamespace();
	Error PrepareSimpleCmplxPKNamespace();
	Error PrepareInsertNamespace();

protected:
	vector<string> locations_;
	vector<string> names_;
	vector<string> adjectives_;
	vector<string> devices_;
	vector<KeyRefs> pkgs_;
	vector<KeyRefs> priceIDs_;

private:
	void init(State& state);

private:
	static shared_ptr<Reindexer> reindexer_;
};
