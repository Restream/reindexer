#pragma once

#include <benchmark/benchmark.h>
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

using reindexer::Reindexer;
using reindexer::Item;
using reindexer::Error;
using reindexer::KeyRef;
using reindexer::KeyRefs;

using benchmark::State;
using benchmark::Fixture;

class Rndxr : public Fixture {
public:
	typedef tuple<const char*, const char*, const char*, IndexOpts> IndexDeclaration;
	typedef shared_ptr<Item> ItemPtr;

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

	Error newTestItemBench(int id, int pkgCount, ItemPtr& item);
	Error newTestJoinItem(int id, ItemPtr& item);
	Error newTestSimpleItem(int id, ItemPtr& item);
	Error newTestSimpleCmplxPKItem(int id, ItemPtr& item);
	Error newTestInsertItem(int id, ItemPtr& item);

	string randLocation();
	KeyRefs randIntArr(int cnt, int start, int rng);
	string randString(const string prefix = "");
	string randDevice();

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
