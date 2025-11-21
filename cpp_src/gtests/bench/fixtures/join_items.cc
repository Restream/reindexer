#include "join_items.h"
#include "helpers.h"

reindexer::Error JoinItems::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}

	adjectives_ = {"able",	   "above",	  "absolute", "balanced", "becoming", "beloved", "calm",	 "capable",	 "capital",	 "destined",
				   "devoted",  "direct",  "enabled",  "enabling", "endless",  "factual", "fair",	 "faithful", "grand",	 "grateful",
				   "great",	   "humane",  "humble",	  "humorous", "ideal",	  "immense", "immortal", "joint",	 "just",	 "keen",
				   "key",	   "kind",	  "logical",  "loved",	  "loving",	  "mint",	 "model",	 "modern",	 "nice",	 "noble",
				   "normal",   "one",	  "open",	  "optimal",  "polite",	  "popular", "positive", "quality",	 "quick",	 "quiet",
				   "rapid",	   "rare",	  "rational", "sacred",	  "safe",	  "saved",	 "tight",	 "together", "tolerant", "unbiased",
				   "uncommon", "unified", "valid",	  "valued",	  "vast",	  "wealthy", "welcome"};

	devices_ = {"iphone", "android", "smarttv", "stb", "ottstb"};

	locations_ = {"mos", "ct", "dv", "sth", "vlg", "sib", "ural"};

	names_ = {"ox",	  "ant",  "ape",  "asp",  "bat",  "bee",  "boa",  "bug",  "cat",  "cod",  "cow",  "cub",  "doe",  "dog",
			  "eel",  "eft",  "elf",  "elk",  "emu",  "ewe",  "fly",  "fox",  "gar",  "gnu",  "hen",  "hog",  "imp",  "jay",
			  "kid",  "kit",  "koi",  "lab",  "man",  "owl",  "pig",  "pug",  "pup",  "ram",  "rat",  "ray",  "yak",  "bass",
			  "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo",
			  "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull",
			  "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite",
			  "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole",
			  "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti"};

	return {};
}

void JoinItems::RegisterAllCases() { BaseFixture::RegisterAllCases(); }

reindexer::Item JoinItems::MakeItem(benchmark::State&) {
	reindexer::Item item = db_->NewItem(nsdef_.name);
	if (item.Status().ok()) {
		item["id"] = id_seq_->Next();
		item["name"] = randomString("price");
		// All strings passed in unsafe mode to item must be holded by app
		std::ignore = item.Unsafe();
		item["location"] = locations_.at(random<size_t>(0, locations_.size() - 1));
		item["device"] = devices_.at(random<size_t>(0, devices_.size() - 1));
	}
	return item;
}

std::string JoinItems::randomString(const std::string& prefix) {
	std::string result;
	if (!prefix.empty()) {
		result += prefix + "_";
	}
	result += adjectives_.at(random<size_t>(0, adjectives_.size() - 1));
	result += "_";
	result += names_.at(random<size_t>(0, names_.size() - 1));
	return result;
}
