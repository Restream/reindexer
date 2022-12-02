#pragma once
#include <limits>
#include "core/cjson/jsonbuilder.h"
#include "core/ft/config/ftfastconfig.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer.h"
#include "reindexertestapi.h"

class FTApi : public ::testing::TestWithParam<reindexer::FtFastConfig::Optimization> {
public:
	enum { NS1 = 1, NS2 = 2, NS3 = 4 };
	void Init(const reindexer::FtFastConfig& ftCfg, unsigned nses = NS1, const std::string& storage = std::string()) {
		rt.reindexer.reset(new reindexer::Reindexer);
		if (!storage.empty()) {
			auto err = rt.reindexer->Connect("builtin://" + storage);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		if (nses & NS1) {
			const reindexer::Error err = rt.reindexer->OpenNamespace("nm1");
			ASSERT_TRUE(err.ok()) << err.what();
			rt.DefineNamespaceDataset("nm1", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
											  IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
											  IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
											  IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
			SetFTConfig(ftCfg);
		}
		if (nses & NS2) {
			const reindexer::Error err = rt.reindexer->OpenNamespace("nm2");
			ASSERT_TRUE(err.ok()) << err.what();
			rt.DefineNamespaceDataset("nm2", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
											  IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
											  IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
											  IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		}
		if (nses & NS3) {
			reindexer::Error err = rt.reindexer->OpenNamespace("nm3");
			ASSERT_TRUE(err.ok()) << err.what();
			rt.DefineNamespaceDataset(
				"nm3",
				{IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
				 IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0}, IndexDeclaration{"ft3", "text", "string", IndexOpts(), 0},
				 IndexDeclaration{"ft1+ft2+ft3=ft", "text", "composite", IndexOpts(), 0}});
			err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	reindexer::FtFastConfig GetDefaultConfig(size_t fieldsCount = 2) {
		reindexer::FtFastConfig cfg(fieldsCount);
		cfg.enableNumbersSearch = true;
		cfg.enableWarmupOnNsCopy = true;
		cfg.logLevel = 5;
		cfg.mergeLimit = 20000;
		cfg.maxStepSize = 100;
		cfg.optimization = GetParam();
		return cfg;
	}

	void SetFTConfig(const reindexer::FtFastConfig& ftCfg) {
		const reindexer::Error err = SetFTConfig(ftCfg, "nm1", "ft3", {"ft1", "ft2"});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	reindexer::Error SetFTConfig(const reindexer::FtFastConfig& ftCfg, const std::string& ns, const std::string& index,
								 const std::vector<std::string>& fields) {
		assert(!ftCfg.fieldsCfg.empty());
		assert(ftCfg.fieldsCfg.size() >= fields.size());
		reindexer::fast_hash_map<std::string, int> fieldsMap;
		for (size_t i = 0, size = fields.size(); i < size; ++i) {
			fieldsMap.emplace(fields[i], i);
		}
		std::vector<reindexer::NamespaceDef> nses;
		rt.reindexer->EnumNamespaces(nses, reindexer::EnumNamespacesOpts().WithFilter(ns));
		const auto it = std::find_if(nses[0].indexes.begin(), nses[0].indexes.end(),
									 [&index](const reindexer::IndexDef& idef) { return idef.name_ == index; });
		it->opts_.SetConfig(ftCfg.GetJson(fieldsMap));

		return rt.reindexer->UpdateIndex(ns, *it);
	}

	void FillData(int64_t count) {
		for (int i = 0; i < count; ++i) {
			reindexer::Item item = rt.NewItem(default_namespace);
			item["id"] = counter_;
			auto ft1 = rt.RandString();

			counter_++;

			item["ft1"] = ft1;

			rt.Upsert(default_namespace, item);
			rt.Commit(default_namespace);
		}
	}
	void Add(std::string_view ft1, std::string_view ft2, unsigned nses = NS1) {
		using namespace std::string_view_literals;
		if (nses & NS1) {
			Add("nm1"sv, ft1, ft2);
		}
		if (nses & NS2) {
			Add("nm2"sv, ft1, ft2);
		}
	}

	std::pair<std::string_view, int> Add(std::string_view ft1) {
		reindexer::Item item = rt.NewItem("nm1");
		item["id"] = counter_;
		counter_++;
		item["ft1"] = std::string{ft1};

		rt.Upsert("nm1", item);
		rt.Commit("nm1");
		return make_pair(ft1, counter_ - 1);
	}
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2) {
		reindexer::Item item = rt.NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = std::string{ft1};
		item["ft2"] = std::string{ft2};

		rt.Upsert(ns, item);
		rt.Commit(ns);
	}
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2, std::string_view ft3) {
		reindexer::Item item = rt.NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = std::string{ft1};
		item["ft2"] = std::string{ft2};
		item["ft3"] = std::string{ft3};

		rt.Upsert(ns, item);
		rt.Commit(ns);
	}

	void AddInBothFields(std::string_view w1, std::string_view w2, unsigned nses = NS1) {
		using namespace std::string_view_literals;
		if (nses & NS1) {
			AddInBothFields("nm1"sv, w1, w2);
		}
		if (nses & NS2) {
			AddInBothFields("nm2"sv, w1, w2);
		}
	}

	void AddInBothFields(std::string_view ns, std::string_view w1, std::string_view w2) {
		reindexer::Item item = rt.NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = std::string{w1};
		item["ft2"] = std::string{w1};
		rt.Upsert(ns, item);

		item = rt.NewItem(ns);
		item["id"] = counter_;
		++counter_;
		item["ft1"] = std::string{w2};
		item["ft2"] = std::string{w2};
		rt.Upsert(ns, item);

		rt.Commit(ns);
	}

	reindexer::QueryResults SimpleSelect(std::string word) {
		auto qr{reindexer::Query("nm1").Where("ft3", CondEq, std::move(word))};
		reindexer::QueryResults res;
		qr.AddFunction("ft3 = highlight(!,!)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}

	reindexer::QueryResults SimpleSelect3(std::string word) {
		auto qr{reindexer::Query("nm3").Where("ft", CondEq, std::move(word))};
		reindexer::QueryResults res;
		qr.AddFunction("ft = highlight(!,!)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}

	void Delete(int id) {
		reindexer::Item item = rt.NewItem("nm1");
		item["id"] = id;

		this->rt.reindexer->Delete("nm1", item);
	}
	reindexer::QueryResults SimpleCompositeSelect(std::string word) {
		auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
		reindexer::QueryResults res;
		auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, std::move(word))};
		mqr.AddFunction("ft1 = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.emplace_back(Merge, std::move(mqr));
		qr.AddFunction("ft3 = highlight(<b>,</b>)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	reindexer::QueryResults CompositeSelectField(const std::string& field, std::string word) {
		word = '@' + field + ' ' + word;
		auto qr{reindexer::Query("nm1").Where("ft3", CondEq, word)};
		reindexer::QueryResults res;
		auto mqr{reindexer::Query("nm2").Where("ft3", CondEq, std::move(word))};
		mqr.AddFunction(field + " = snippet(<b>,\"\"</b>,3,2,,d)");

		qr.mergeQueries_.emplace_back(Merge, std::move(mqr));
		qr.AddFunction(field + " = highlight(<b>,</b>)");
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	reindexer::QueryResults StressSelect(std::string word) {
		const auto qr{reindexer::Query("nm1").Where("ft3", CondEq, std::move(word))};
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	std::vector<std::string> CreateAllPermutatedQueries(const std::string& queryStart, std::vector<std::string> words,
														const std::string& queryEnd, const std::string& sep = " ") {
		std::vector<std::pair<size_t, std::string>> indexedWords;
		indexedWords.reserve(words.size());
		for (std::string& w : words) {
			indexedWords.emplace_back(indexedWords.size(), std::move(w));
		}
		std::vector<std::string> result;
		do {
			result.push_back(queryStart);
			std::string& query = result.back();
			for (auto it = indexedWords.cbegin(); it != indexedWords.cend(); ++it) {
				if (it != indexedWords.cbegin()) query += sep;
				query += it->second;
			}
			query += queryEnd;
		} while (std::next_permutation(
			indexedWords.begin(), indexedWords.end(),
			[](const std::pair<size_t, std::string>& a, const std::pair<size_t, std::string>& b) { return a.first < b.first; }));
		return result;
	}
	void CheckAllPermutations(const std::string& queryStart, const std::vector<std::string>& words, const std::string& queryEnd,
							  const std::vector<std::tuple<std::string, std::string>>& expectedResults, bool withOrder = false,
							  const std::string& sep = " ") {
		for (const auto& query : CreateAllPermutatedQueries(queryStart, words, queryEnd, sep)) {
			CheckResults(query, expectedResults, withOrder);
		}
	}

	void CheckResults(const std::string& query, std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder) {
		const auto qr = SimpleSelect(query);
		CheckResults(query, qr, expectedResults, withOrder);
	}

	void CheckResults(const std::string& query, const reindexer::QueryResults& qr,
					  std::vector<std::tuple<std::string, std::string, std::string>> expectedResults, bool withOrder) {
		CheckResults<std::tuple<std::string, std::string, std::string>>(query, qr, expectedResults, withOrder);
	}

	template <typename ResType>
	void CheckResults(const std::string& query, const reindexer::QueryResults& qr, std::vector<ResType>& expectedResults, bool withOrder) {
		constexpr bool kTreeFields = std::tuple_size<ResType>{} == 3;
		EXPECT_EQ(qr.Count(), expectedResults.size()) << "Query: " << query;
		for (auto itRes : qr) {
			const auto item = itRes.GetItem(false);
			const auto it = std::find_if(expectedResults.begin(), expectedResults.end(), [&item](const ResType& p) {
				if constexpr (kTreeFields) {
					return std::get<0>(p) == item["ft1"].As<std::string>() && std::get<1>(p) == item["ft2"].As<std::string>() &&
						   std::get<2>(p) == item["ft3"].As<std::string>();
				}
				return std::get<0>(p) == item["ft1"].As<std::string>() && std::get<1>(p) == item["ft2"].As<std::string>();
			});
			if (it == expectedResults.end()) {
				if constexpr (kTreeFields) {
					ADD_FAILURE() << "Found not expected: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>()
								  << "\" \"" << item["ft3"].As<std::string>() << "\"\nQuery: " << query;
				} else {
					ADD_FAILURE() << "Found not expected: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>()
								  << "\"\nQuery: " << query;
				}
			} else {
				if (withOrder) {
					if constexpr (kTreeFields) {
						EXPECT_EQ(it, expectedResults.begin())
							<< "Found not in order: \"" << item["ft1"].As<std::string>() << "\" \"" << item["ft2"].As<std::string>()
							<< "\" \"" << item["ft3"].As<std::string>() << "\"\nQuery: " << query;
					} else {
						EXPECT_EQ(it, expectedResults.begin()) << "Found not in order: \"" << item["ft1"].As<std::string>() << "\" \""
															   << item["ft2"].As<std::string>() << "\"\nQuery: " << query;
					}
				}
				expectedResults.erase(it);
			}
		}
		for (const auto& expected : expectedResults) {
			if constexpr (kTreeFields) {
				ADD_FAILURE() << "Not found: \"" << std::get<0>(expected) << "\" \"" << std::get<1>(expected) << "\" \""
							  << std::get<2>(expected) << "\"\nQuery: " << query;
			} else {
				ADD_FAILURE() << "Not found: \"" << std::get<0>(expected) << "\" \"" << std::get<1>(expected) << "\"\nQuery: " << query;
			}
		}
		if (!expectedResults.empty()) {
			ADD_FAILURE() << "Query: " << query;
		}
	}

protected:
	static constexpr int kMaxMergeLimitValue = 65000;
	static constexpr int kMinMergeLimitValue = 0;

	struct Data {
		std::string ft1;
		std::string ft2;
	};
	struct FTDSLQueryParams {
		reindexer::fast_hash_map<std::string, int> fields;
		reindexer::fast_hash_set<std::string, reindexer::hash_str, reindexer::equal_str> stopWords;
		std::string extraWordSymbols = "-/+";
	};
	int counter_ = 0;
	const std::string default_namespace = "test_namespace";
	ReindexerTestApi<reindexer::Reindexer> rt;
};
