#pragma once

#include <benchmark/benchmark.h>
#include "core/reindexer.h"

using benchmark::State;
using benchmark::internal::Benchmark;

using reindexer::NamespaceDef;
using reindexer::Reindexer;

class [[nodiscard]] ApiEncDec {
public:
	ApiEncDec(Reindexer* db, std::string&& name);
	~ApiEncDec();

	void RegisterAllCases();
	reindexer::Error Initialize();

private:
	template <typename Fn, typename Cl>
	Benchmark* Register(const std::string& name, Fn fn, Cl* cl) {
		std::string tn(benchName_);
		tn.append("/").append(name);
		return benchmark::RegisterBenchmark(tn.c_str(), std::bind(fn, cl, std::placeholders::_1));
	}
	reindexer::Error prepareBenchData();

	void FromCJSON(State&);
	void FromCJSONPKOnly(State&);
	void GetCJSON(State&);
	void ExtractField(State&);
	void FromJSON(State&);
	void GetJSON(State&);
	void FromPrettyJSON(State&);
	void GetPrettyJSON(State&);
	void FromMsgPack(State&);
	void GetMsgPack(State&);

	Reindexer* db_;
	const std::string nsName_{"cjson_ns_name"};
	const std::string benchName_;
	std::unique_ptr<reindexer::Item> itemForCjsonBench_;
	std::vector<std::string> fieldsToExtract_;
	std::string itemCJSON_;
	std::string itemJSON_;
	std::string itemPrettyJSON_;
	std::string itemMsgPack_;
	constexpr static int kCjsonBenchItemID = 9973;
};
