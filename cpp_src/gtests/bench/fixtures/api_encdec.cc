#include "api_encdec.h"
#include "allocs_tracker.h"
#include "core/cjson/jsonbuilder.h"
#include "estl/gift_str.h"
#include "helpers.h"
#include "tools/jsontools.h"

using benchmark::AllocsTracker;

ApiEncDec::ApiEncDec(Reindexer* db, std::string&& name) : db_(db), benchName_(std::move(name)) {}

ApiEncDec::~ApiEncDec() {
	assertrx(db_);
	auto err = db_->CloseNamespace(nsName_);
	if (!err.ok()) {
		std::cerr << "Error while closing namespace '" << nsName_ << "'. Reason: " << err.what() << std::endl;
	}
}

void ApiEncDec::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("FromCJSON", &ApiEncDec::FromCJSON, this);
	Register("FromCJSONPKOnly", &ApiEncDec::FromCJSONPKOnly, this);
	Register("GetCJSON", &ApiEncDec::GetCJSON, this);
	Register("ExtractField", &ApiEncDec::ExtractField, this);
	Register("FromJSON", &ApiEncDec::FromJSON, this);
	Register("GetJSON", &ApiEncDec::GetJSON, this);
	Register("FromPrettyJSON", &ApiEncDec::FromPrettyJSON, this);
	Register("GetPrettyJSON", &ApiEncDec::GetPrettyJSON, this);
	Register("FromMsgPack", &ApiEncDec::FromMsgPack, this);
	Register("GetMsgPack", &ApiEncDec::GetMsgPack, this);
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

reindexer::Error ApiEncDec::Initialize() {
	assertrx(db_);
	return prepareBenchData();
}

reindexer::Error ApiEncDec::prepareBenchData() {
	NamespaceDef nsDef{nsName_};
	nsDef.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("bool_-_index", "-", "bool", IndexOpts())
		.AddIndex("int_-_index", "-", "int", IndexOpts())
		.AddIndex("int_hash_index", "hash", "int", IndexOpts())
		.AddIndex("int_tree_index", "tree", "int", IndexOpts())
		.AddIndex("int64_-_index", "-", "int64", IndexOpts())
		.AddIndex("int64_hash_index", "hash", "int64", IndexOpts())
		.AddIndex("int64_tree_index", "tree", "int64", IndexOpts())
		.AddIndex("double_-_index", "-", "double", IndexOpts())
		.AddIndex("double_tree_index", "tree", "double", IndexOpts())
		.AddIndex("string_-_index", "-", "string", IndexOpts())
		.AddIndex("string_hash_index", "hash", "string", IndexOpts())
		.AddIndex("string_tree_index", "tree", "string", IndexOpts())
		.AddIndex("string_text_index", "text", "string", IndexOpts())
		.AddIndex("bool_-_array_index", "-", "bool", IndexOpts().Array())
		.AddIndex("int_-_array_index", "-", "int", IndexOpts().Array())
		.AddIndex("int_hash_array_index", "hash", "int", IndexOpts().Array())
		.AddIndex("int_tree_array_index", "tree", "int", IndexOpts().Array())
		.AddIndex("int64_-_array_index", "-", "int64", IndexOpts().Array())
		.AddIndex("int64_hash_array_index", "hash", "int64", IndexOpts().Array())
		.AddIndex("int64_tree_array_index", "tree", "int64", IndexOpts().Array())
		.AddIndex("double_-_array_index", "-", "double", IndexOpts().Array())
		.AddIndex("double_tree_array_index", "tree", "double", IndexOpts().Array())
		.AddIndex("string_-_array_index", "-", "string", IndexOpts().Array())
		.AddIndex("string_hash_array_index", "hash", "string", IndexOpts().Array())
		.AddIndex("string_tree_array_index", "tree", "string", IndexOpts().Array());
	auto err = db_->AddNamespace(nsDef);
	if (!err.ok()) {
		return err;
	}

	fieldsToExtract_.clear();
	itemForCjsonBench_ = std::make_unique<reindexer::Item>(db_->NewItem(nsName_));
	if (!itemForCjsonBench_->Status().ok()) {
		return itemForCjsonBench_->Status();
	}
	reindexer::WrSerializer wser;
	reindexer::JsonBuilder bld(wser);
	constexpr size_t len = 10;
	bld.Put("id", kCjsonBenchItemID);
	bld.Put("bool_-_index", rand() % 2);
	bld.Put("int_-_index", rand());
	bld.Put("int_hash_index", rand());
	bld.Put("int_tree_index", rand());
	bld.Put("int64_-_index", rand());
	bld.Put("int64_hash_index", rand());
	bld.Put("int64_tree_index", rand());
	bld.Put("double_-_index", rand() / double(rand() + 1));
	bld.Put("double_tree_index", rand() / double(rand() + 1));
	bld.Put("string_-_index", randString(len));
	bld.Put("string_hash_index", randString(len));
	bld.Put("string_tree_index", randString(len));
	bld.Put("string_text_index", randString(len));
	bld.Array("bool_-_array_index", randBoolArray<len>());
	bld.Array("int_-_array_index", randIntArray<len>());
	bld.Array("int_hash_array_index", randIntArray<len>());
	bld.Array("int_tree_array_index", randIntArray<len>());
	bld.Array("int64_-_array_index", randInt64Array<len>());
	bld.Array("int64_hash_array_index", randInt64Array<len>());
	bld.Array("int64_tree_array_index", randInt64Array<len>());
	bld.Array("double_-_array_index", randDoubleArray<len>());
	bld.Array("double_tree_array_index", randDoubleArray<len>());
	bld.Array("string_-_array_index", randStringArray<len>());
	bld.Array("string_hash_array_index", randStringArray<len>());
	bld.Array("string_tree_array_index", randStringArray<len>());
	for (size_t i = 0; i < 10; ++i) {
		const std::string i_str = std::to_string(i);
		fieldsToExtract_.emplace_back("bool_field_" + i_str);
		bld.Put("bool_field_" + i_str, rand() % 2);
		fieldsToExtract_.emplace_back("int_field_" + i_str);
		bld.Put("int_field_" + i_str, rand());
		fieldsToExtract_.emplace_back("double_field_" + i_str);
		bld.Put("double_field_" + i_str, rand() / double(rand() + 1));
		fieldsToExtract_.emplace_back("string_field_" + i_str);
		bld.Put("string_field_" + i_str, randString(len));
		bld.Array("bool_array_field_" + i_str, randBoolArray<len>());
		bld.Array("int_array_field_" + i_str, randIntArray<len>());
		bld.Array("double_array_field_" + i_str, randDoubleArray<len>());
		bld.Array("string_array_field_" + i_str, randStringArray<len>());
		{
			const std::string nestedBase("nested_obj_" + i_str);
			auto obj = bld.Object(nestedBase);
			obj.Put("bool_field", rand() % 2);
			fieldsToExtract_.emplace_back(nestedBase + ".bool_field");
			obj.Put("int_field", rand());
			fieldsToExtract_.emplace_back(nestedBase + ".int_field");
			obj.Put("double_field", rand() / double(rand() + 1));
			fieldsToExtract_.emplace_back(nestedBase + ".double_field");
			obj.Put("string_field", randString(len));
			fieldsToExtract_.emplace_back(nestedBase + ".string_field");
			obj.Array("bool_array_field", randBoolArray<len>());
			obj.Array("int_array_field", randIntArray<len>());
			obj.Array("double_array_field", randDoubleArray<len>());
			obj.Array("string_array_field", randStringArray<len>());
		}
		{
			auto arr = bld.Array("nested_arr_" + i_str);
			for (size_t j = 0; j < len; ++j) {
				auto obj = arr.Object();
				obj.Put("bool_field", rand() % 2);
				obj.Put("int_field", rand());
				obj.Put("double_field", rand() / double(rand() + 1));
				obj.Put("string_field", randString(len));
				obj.Array("bool_array_field", randBoolArray<len>());
				obj.Array("int_array_field", randIntArray<len>());
				obj.Array("double_array_field", randDoubleArray<len>());
				obj.Array("string_array_field", randStringArray<len>());
			}
		}
	}
	bld.End();
	err = itemForCjsonBench_->FromJSON(wser.Slice());
	if (!err.ok()) {
		return err;
	}
	if (!itemForCjsonBench_->Status().ok()) {
		return itemForCjsonBench_->Status();
	}
	err = db_->Insert(nsName_, *itemForCjsonBench_);
	if (!err.ok()) {
		return err;
	}
	itemCJSON_ = itemForCjsonBench_->GetCJSON();
	itemJSON_ = itemForCjsonBench_->GetJSON();
	wser.Reset();
	reindexer::prettyPrintJSON(std::string_view(itemJSON_), wser);
	itemPrettyJSON_ = wser.Slice();
	itemMsgPack_ = itemForCjsonBench_->GetMsgPack();
	return {};
}

void ApiEncDec::FromCJSON(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsName_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto err = item.FromCJSON(itemCJSON_);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}
	}
}

void ApiEncDec::FromCJSONPKOnly(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsName_);
	{
		AllocsTracker allocsTracker(state);
		for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
			const auto err = item.FromCJSON(itemCJSON_, true);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
			}
		}
	}
	assertrx(item["id"].Get<int>() == kCjsonBenchItemID);
}

void ApiEncDec::GetCJSON(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto ret = itemForCjsonBench_->GetCJSON();
		benchmark::DoNotOptimize(ret);
	}
}

void ApiEncDec::ExtractField(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	assertrx(fieldsToExtract_.size());
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto& fieldName = fieldsToExtract_[rand() % fieldsToExtract_.size()];
		const auto va = VariantArray((*itemForCjsonBench_)[fieldName]);
		if (va.size() != 1) {
			state.SkipWithError(fmt::format("Unexpected result size: {}", va.size()).c_str());
		}
	}
}

void ApiEncDec::FromJSON(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsName_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto err = item.FromJSON(itemJSON_);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}
	}
}

void ApiEncDec::GetJSON(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto ret = itemForCjsonBench_->GetJSON();
		benchmark::DoNotOptimize(ret);
	}
}

void ApiEncDec::FromPrettyJSON(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsName_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto err = item.FromJSON(itemPrettyJSON_);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}
	}
}

void ApiEncDec::GetPrettyJSON(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	AllocsTracker allocsTracker(state);
	reindexer::WrSerializer wser;
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		wser.Reset();
		auto ret = itemForCjsonBench_->GetJSON();
		reindexer::prettyPrintJSON(reindexer::giftStr(ret), wser);
	}
}

void ApiEncDec::FromMsgPack(benchmark::State& state) {
	reindexer::Item item = db_->NewItem(nsName_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		size_t offset = 0;
		const auto err = item.FromMsgPack(itemMsgPack_, offset);
		if (!err.ok()) {
			state.SkipWithError(err.what());
		}
		if (!item.Status().ok()) {
			state.SkipWithError(item.Status().what());
		}
	}
}

void ApiEncDec::GetMsgPack(benchmark::State& state) {
	assertrx(itemForCjsonBench_);
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		const auto ret = itemForCjsonBench_->GetMsgPack();
		benchmark::DoNotOptimize(ret);
	}
}
