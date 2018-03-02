#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <vector>

#include "reindexer_fixture.h"

using std::get;
using std::vector;
using std::call_once;
using std::once_flag;

using reindexer::NamespaceDef;
using reindexer::Error;
using reindexer::make_key_string;
using reindexer::p_string;

string const Rndxr::defaultStorage = "/tmp/reindexer_bench/";
string const Rndxr::defaultNamespace = "test_items_bench";
string const Rndxr::defaultJoinNamespace = "test_join_items";
string const Rndxr::defaultSimpleNamespace = "test_items_simple";
string const Rndxr::defaultSimpleCmplxPKNamespace = "test_items_simple_cmplx_pk";
string const Rndxr::defaultInsertNamespace = "test_insert_items";

static once_flag prepared;

static const char* storagePath = "/tmp/reindexer_bench";
shared_ptr<Reindexer> Rndxr::reindexer_ = make_shared<Reindexer>();

namespace aux {
constexpr inline int mkID(int i) { return i * 17 + 8000000; }

vector<int> randIntArr(int cnt, int start, int rng) {
	vector<int> arr;
	if (cnt == 0) return arr;

	for (int i = 0; i < cnt; i++) arr.emplace_back(start + rand() % rng);

	return arr;
}

Error rmDirForce(const char* dirname) {
	DIR* dir;
	struct dirent* entry;
	char path[PATH_MAX];

	dir = opendir(dirname);
	if (dir == NULL) return 0;

	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, "..")) {
			snprintf(path, static_cast<size_t>(PATH_MAX), "%s/%s", dirname, entry->d_name);
			if (entry->d_type == DT_DIR) {
				rmDirForce(path);
			}
			if (remove(path) && errno != ENOENT) {
				return Error(errLogic, strerror(errno));
			}
		}
	}
	closedir(dir);
	if (remove(dirname) && errno != ENOENT) {
		return Error(errLogic, strerror(errno));
	}
	return 0;
}

}  // namespace aux

Rndxr::Rndxr()
	: locations_{"mos", "ct", "dv", "sth", "vlg", "sib", "ural"},
	  names_{"ox",   "ant",  "ape",  "asp",  "bat",  "bee",  "boa",  "bug",  "cat",  "cod",  "cow",  "cub",  "doe",  "dog",  "eel",  "eft",
			 "elf",  "elk",  "emu",  "ewe",  "fly",  "fox",  "gar",  "gnu",  "hen",  "hog",  "imp",  "jay",  "kid",  "kit",  "koi",  "lab",
			 "man",  "owl",  "pig",  "pug",  "pup",  "ram",  "rat",  "ray",  "yak",  "bass", "bear", "bird", "boar", "buck", "bull", "calf",
			 "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal",
			 "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon",
			 "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad",
			 "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"},
	  adjectives_{"able",	 "above",   "absolute", "balanced", "becoming", "beloved", "calm",		"capable",  "capital",  "destined",
				  "devoted",  "direct",  "enabled",  "enabling", "endless",  "factual", "fair",		"faithful", "grand",	"grateful",
				  "great",	"humane",  "humble",   "humorous", "ideal",	"immense", "immortal", "joint",	"just",		"keen",
				  "key",	  "kind",	"logical",  "loved",	"loving",   "mint",	"model",	"modern",   "nice",		"noble",
				  "normal",   "one",	 "open",	 "optimal",  "polite",   "popular", "positive", "quality",  "quick",	"quiet",
				  "rapid",	"rare",	"rational", "sacred",   "safe",	 "saved",   "tight",	"together", "tolerant", "unbiased",
				  "uncommon", "unified", "valid",	"valued",   "vast",	 "wealthy", "welcome"},
	  devices_{"iphone", "android", "smarttv", "stb", "ottstb"} {
	for (int i = 0; i < 10; i++) pkgs_.emplace_back(randIntArr(20, 10000, 10));

	for (int i = 0; i < 20; i++) priceIDs_.emplace_back(randIntArr(10, 7000, 50));
}

void Rndxr::SetUp(benchmark::State& state) { call_once(prepared, &Rndxr::init, this, std::ref(state)); }

void Rndxr::init(State& state) {
	srand(time(nullptr));

	auto err = aux::rmDirForce(storagePath);
	if (err) {
		state.SkipWithError(err.what().c_str());
		return;
	}

	err = GetDB()->EnableStorage(storagePath);
	if (!err.ok()) state.SkipWithError(err.what().c_str());

	err = PrepareDefaultNamespace();
	if (!err.ok()) state.SkipWithError(err.what().c_str());

	err = PrepareJoinNamespace();
	if (!err.ok()) state.SkipWithError(err.what().c_str());

	err = PrepareSimpleNamespace();
	if (!err.ok()) state.SkipWithError(err.what().c_str());

	err = PrepareSimpleCmplxPKNamespace();
	if (err) state.SkipWithError(err.what().c_str());

	err = PrepareInsertNamespace();
	if (err) state.SkipWithError(err.what().c_str());
}

void Rndxr::TearDown(benchmark::State& state) { (void)state; }

reindexer::Error Rndxr::DefineNamespaceIndexes(const string& ns, initializer_list<const IndexDeclaration> fields) {
	auto err = Error();
	for (auto field : fields) {
		err = GetDB()->AddIndex(ns, {get<0>(field), "", get<1>(field), get<2>(field), get<3>(field)});
		if (!err.ok()) return err;
	}
	err = GetDB()->Commit(ns);
	return err;
}

Error Rndxr::FillTestItemsBench(unsigned /*start*/, unsigned count, int pkgsCount) {
	// TODO: possible error - start was not used
	Error err;
	for (unsigned i = 0; i < count; i++) {
		Item item;
		if ((err = newTestItemBench(aux::mkID(i), pkgsCount, item))) return err;
		if ((err = GetDB()->Upsert(defaultNamespace, item))) return err;
	}
	//	GetDB()->Commit(defaultNamespace);
	return 0;
}

Error Rndxr::FillTestJoinItem(unsigned /*start*/, unsigned count) {
	// TODO: possible error - start was not used
	Error err;

	for (unsigned i = 0; i < count; i++) {
		Item item;
		if ((err = newTestJoinItem(aux::mkID(i), item))) return err;
		if ((err = GetDB()->Upsert(defaultJoinNamespace, item))) return err;
	}
	//	GetDB()->Commit(defaultJoinNamespace);
	return 0;
}

Error Rndxr::newTestJoinItem(int id, Item& item) {
	Error err;
	if (!item) {
		item = GetDB()->NewItem(defaultJoinNamespace);
		err = item.Status();
		if (err) return err;
	}

	item["id"] = id;
	item["name"] = randName();
	item["location"] = randLocation();
	item["device"] = randDevice();

	return 0;
}

Error Rndxr::newTestSimpleItem(int id, Item& item) {
	Error err;
	if (!item) {
		item = GetDB()->NewItem(defaultSimpleNamespace);
		err = item.Status();
		if (!err.ok()) return err;
	}

	item["id"] = id;
	item["year"] = rand() % 1000 + 10;
	item["name"] = randName();

	return 0;
}

reindexer::Error Rndxr::newTestSimpleCmplxPKItem(int id, Item& item) {
	Error err;
	if (!item) {
		item = GetDB()->NewItem(defaultSimpleCmplxPKNamespace);
		err = item.Status();
		if (err) return err;
	}

	item["subid"] = randString().c_str();
	item.Unsafe();
	item["id"] = id;
	item["year"] = rand() % 1000 + 10;
	item["name"] = randName();

	return 0;
}

reindexer::Error Rndxr::newTestInsertItem(int id, Item& item) {
	Error err;
	if (!item) {
		item = GetDB()->NewItem(defaultInsertNamespace);
		err = item.Status();
		if (err) return err;
	}

	int startTime = rand() % 50000;
	int endTime = startTime + (rand() % 5) * 1000;

	item["description"] = randString().c_str();
	item["actor"] = randString().c_str();

	// Turn on unsafe mode. The following fields are holded by bench
	item.Unsafe(true)["id"] = id;
	item["year"] = rand() % 1000 + 10;
	item["genre"] = static_cast<int64_t>(rand() % 50);
	item["name"] = randName();
	item["age"] = rand() % 5;
	item["packages"] = randIntArr(10, 10000, 50);
	item["rate"] = static_cast<double>((rand() % 100) / 10.0);
	item["isdeleted"] = rand() % 2;
	item["price_id"] = priceIDs_[static_cast<size_t>(rand()) % priceIDs_.size()];
	item["location_id"] = randLocation();
	item["start_time"] = startTime;
	item["end_time"] = endTime;

	return 0;
}

Error Rndxr::newTestItemBench(int id, int pkgCount, Item& item) {
	Error err;
	if (!item) {
		item = GetDB()->NewItem(defaultNamespace);
		err = item.Status();
		if (!err.ok()) return err;
	}

	int startTime = rand() % 50000;
	int endTime = startTime + (rand() % 5) * 1000;

	item.Unsafe()["id"] = id;
	item["year"] = rand() % 50 + 2000;
	item["genre"] = static_cast<int64_t>(rand() % 50);
	item["age"] = rand() % 5;
	//	item["countries"] = {countries});
	item["packages"] = randIntArr(pkgCount, 10000, 50);
	item["price_id"] = priceIDs_[static_cast<size_t>(rand()) % priceIDs_.size()];
	item["location"] = randLocation();
	item["start_time"] = startTime;
	item["end_time"] = endTime;

	return 0;
}

KeyRefs Rndxr::randIntArr(int cnt, int start, int rng) {
	KeyRefs arr;
	if (cnt == 0) return arr;

	for (int i = 0; i < cnt; i++) arr.push_back(KeyRef(static_cast<int>(start + rand() % rng)));

	return arr;
}

string Rndxr::randString(const string prefix) {
	string value;
	value += prefix;
	value += "_";
	value += adjectives_[static_cast<size_t>(rand()) % adjectives_.size()];
	value += "_";
	value += names_[static_cast<size_t>(rand()) % names_.size()];
	return value;
}

const char* Rndxr::randLocation() { return locations_[static_cast<size_t>(rand()) % locations_.size()].c_str(); }

const char* Rndxr::randDevice() { return devices_[static_cast<size_t>(rand()) % devices_.size()].c_str(); }

const char* Rndxr::randAdjectives() { return adjectives_[static_cast<size_t>(rand()) % adjectives_.size()].c_str(); }

const char* Rndxr::randName() { return names_[static_cast<size_t>(rand()) % names_.size()].c_str(); }

reindexer::Error Rndxr::PrepareDefaultNamespace() {
	auto opts = StorageOpts().Enabled().DropOnFileFormatError().CreateIfMissing();
	Error err = GetDB()->OpenNamespace(defaultNamespace, opts);
	if (err) return err;

	err = DefineNamespaceIndexes(
		defaultNamespace,
		{IndexDeclaration{"id", "", "int", IndexOpts().PK()},  // IsPK = true
		 IndexDeclaration{"genre", "tree", "int64", IndexOpts()}, IndexDeclaration{"year", "tree", "int", IndexOpts()},
		 IndexDeclaration{"packages", "hash", "int", IndexOpts().Array()},
		 IndexDeclaration{"countries", "tree", "string", IndexOpts().Array()}, IndexDeclaration{"age", "hash", "int", IndexOpts()},
		 IndexDeclaration{"price_id", "", "int", IndexOpts().Array()}, IndexDeclaration{"location", "", "string", IndexOpts()},
		 IndexDeclaration{"end_time", "", "int", IndexOpts()}, IndexDeclaration{"start_time", "tree", "int", IndexOpts()}});

	return err;
}

reindexer::Error Rndxr::PrepareJoinNamespace() {
	Error err = GetDB()->OpenNamespace(defaultJoinNamespace, StorageOpts().Enabled().DropOnFileFormatError().CreateIfMissing());
	if (!err.ok()) return err;

	err = DefineNamespaceIndexes(defaultJoinNamespace, {
														   IndexDeclaration{"id", "", "int", IndexOpts().PK()},
														   IndexDeclaration{"name", "tree", "string", IndexOpts()},
														   IndexDeclaration{"location", "", "string", IndexOpts()},
														   IndexDeclaration{"device", "", "string", IndexOpts()},
													   });

	return err;
}

reindexer::Error Rndxr::PrepareSimpleNamespace() {
	Error err = GetDB()->OpenNamespace(defaultSimpleNamespace, StorageOpts().Enabled().DropOnFileFormatError().CreateIfMissing());
	if (!err.ok()) return err;

	err = DefineNamespaceIndexes(defaultSimpleNamespace,
								 {IndexDeclaration{"id", "", "int", IndexOpts().PK()}, IndexDeclaration{"year", "tree", "int", IndexOpts()},
								  IndexDeclaration{"name", "", "string", IndexOpts()}});

	return err;
}

reindexer::Error Rndxr::PrepareSimpleCmplxPKNamespace() {
	Error err = GetDB()->OpenNamespace(defaultSimpleCmplxPKNamespace, StorageOpts().Enabled().DropOnFileFormatError().CreateIfMissing());
	if (err) return err;

	err = DefineNamespaceIndexes(
		defaultSimpleCmplxPKNamespace,
		{IndexDeclaration{"id", "", "int", IndexOpts().PK()}, IndexDeclaration{"year", "tree", "int", IndexOpts()},
		 IndexDeclaration{"name", "", "string", IndexOpts()}, IndexDeclaration{"subid", "", "string", IndexOpts().PK()}});
	return err;
}

reindexer::Error Rndxr::PrepareInsertNamespace() {
	Error err = GetDB()->OpenNamespace(defaultInsertNamespace, StorageOpts().Enabled().DropOnFileFormatError().CreateIfMissing());
	if (err) return err;

	err = DefineNamespaceIndexes(
		defaultInsertNamespace,
		{IndexDeclaration{"id", "", "int", IndexOpts().PK()}, IndexDeclaration{"genre", "tree", "int64", IndexOpts()},
		 IndexDeclaration{"year", "tree", "int", IndexOpts()}, IndexDeclaration{"packages", "hash", "int", IndexOpts().Array()},
		 IndexDeclaration{"name", "tree", "string", IndexOpts()}, IndexDeclaration{"countries", "tree", "string", IndexOpts().Array()},
		 IndexDeclaration{"age", "hash", "int", IndexOpts()}, IndexDeclaration{"description", "fulltext", "string", IndexOpts()},
		 IndexDeclaration{"rate", "tree", "double", IndexOpts()}, IndexDeclaration{"isdeleted", "", "bool", IndexOpts()},
		 IndexDeclaration{"actor", "", "string", IndexOpts()}, IndexDeclaration{"price_id", "", "int", IndexOpts().Array()},
		 IndexDeclaration{"location_id", "", "string", IndexOpts()}, IndexDeclaration{"end_time", "", "int", IndexOpts()},
		 IndexDeclaration{"start_time", "tree", "int", IndexOpts()}, IndexDeclaration{"tmp", "", "string", IndexOpts().PK()},
		 IndexDeclaration{"id+tmp", "", "composite", IndexOpts()}, IndexDeclaration{"age+genre", "", "composite", IndexOpts()}});

	return err;
}
