#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/queryresults/queryresults.h"
#include "core/reindexer.h"
#include "reindexertestapi.h"

class FTApi : public ::testing::TestWithParam<reindexer::FtFastConfig::Optimization> {
public:
	enum { NS1 = 1, NS2 = 2, NS3 = 4 };
	void Init(const reindexer::FtFastConfig& ftCfg, unsigned nses = NS1, const std::string& storage = std::string());

	reindexer::FtFastConfig GetDefaultConfig(size_t fieldsCount = 2);

	void SetFTConfig(const reindexer::FtFastConfig& ftCfg);

	reindexer::Error SetFTConfig(const reindexer::FtFastConfig& ftCfg, const std::string& ns, const std::string& index,
								 const std::vector<std::string>& fields);

	void FillData(int64_t count);
	void Add(std::string_view ft1, std::string_view ft2, unsigned nses = NS1);

	std::pair<std::string_view, int> Add(std::string_view ft1);
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2);
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2, std::string_view ft3);

	void AddInBothFields(std::string_view w1, std::string_view w2, unsigned nses = NS1);

	void AddInBothFields(std::string_view ns, std::string_view w1, std::string_view w2);

	reindexer::QueryResults SimpleSelect(std::string word, bool withHighlight = true);

	reindexer::QueryResults SimpleSelect3(std::string word);

	reindexer::Error Delete(int id);
	reindexer::QueryResults SimpleCompositeSelect(std::string word);
	reindexer::QueryResults CompositeSelectField(const std::string& field, std::string word);
	reindexer::QueryResults StressSelect(std::string word);
	std::vector<std::string> CreateAllPermutatedQueries(const std::string& queryStart, std::vector<std::string> words,
														const std::string& queryEnd, const std::string& sep = " ");
	void CheckAllPermutations(const std::string& queryStart, const std::vector<std::string>& words, const std::string& queryEnd,
							  const std::vector<std::tuple<std::string, std::string>>& expectedResults, bool withOrder = false,
							  const std::string& sep = " ", bool withHighlight = true);

	void CheckResults(const std::string& query, std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder,
					  bool withHighlight = true);

	void CheckResults(const std::string& query, const reindexer::QueryResults& qr,
					  std::vector<std::tuple<std::string, std::string, std::string>> expectedResults, bool withOrder);

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
	void CheckResultsByField(const reindexer::QueryResults& res, const std::set<std::string>& expected, std::string_view fieldName,
							 std::string_view description);

	std::vector<std::tuple<std::string, std::string>>& DelHighlightSign(std::vector<std::tuple<std::string, std::string>>& in);

protected:
	static constexpr int kMaxMergeLimitValue = 65000;
	static constexpr int kMinMergeLimitValue = 0;
	virtual std::string_view GetDefaultNamespace() noexcept = 0;

	struct Data {
		std::string ft1;
		std::string ft2;
	};
	struct FTDSLQueryParams {
		reindexer::RHashMap<std::string, int> fields;
		reindexer::fast_hash_set<reindexer::StopWord, reindexer::hash_str, reindexer::equal_str, reindexer::less_str> stopWords;
		std::string extraWordSymbols = "-/+";
	};
	int counter_ = 0;
	ReindexerTestApi<reindexer::Reindexer> rt;
};
