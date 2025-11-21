#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/queryresults/queryresults.h"
#include "reindexertestapi.h"

class [[nodiscard]] FTApi : public ::testing::TestWithParam<reindexer::FtFastConfig::Optimization> {
public:
	enum { NS1 = 1, NS2 = 2, NS3 = 4, NS4 = 8 };
	void Init(const reindexer::FtFastConfig& ftCfg, unsigned nses = NS1, const std::string& storage = std::string());

	virtual reindexer::FtFastConfig GetDefaultConfig(size_t fieldsCount = 2);

	void SetFTConfig(const reindexer::FtFastConfig& ftCfg);

	reindexer::Error SetFTConfig(const reindexer::FtFastConfig& ftCfg, std::string_view ns, const std::string& index,
								 const std::vector<std::string>& fields);
	std::string GetFTConfigJSON(const reindexer::FtFastConfig& ftCfg, const std::vector<std::string>& fields);

	void FillData(int64_t count);
	void Add(std::string_view ft1, std::string_view ft2, unsigned nses = NS1);

	std::pair<std::string_view, int> Add(std::string_view ft1);
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2);
	void Add(std::string_view ns, std::string_view ft1, std::string_view ft2, std::string_view ft3);
	void AddNs4(const std::string& ft1, const std::vector<std::string>& ft2, const std::vector<std::string>& ft3);

	void AddInBothFields(std::string_view w1, std::string_view w2, unsigned nses = NS1);

	void AddInBothFields(std::string_view ns, std::string_view w1, std::string_view w2);

	reindexer::QueryResults SimpleSelect(std::string_view ns, std::string_view index, std::string_view dsl, bool withHighlight);
	reindexer::QueryResults SimpleSelect(std::string_view word, bool withHighlight = true) {
		return SimpleSelect("nm1", "ft3", word, withHighlight);
	}
	reindexer::QueryResults SimpleSelect3(std::string_view word) { return SimpleSelect("nm3", "ft", word, true); }

	void Delete(int id);
	reindexer::QueryResults SimpleCompositeSelect(std::string_view word);
	reindexer::QueryResults CompositeSelectField(const std::string& field, std::string_view word);
	reindexer::QueryResults StressSelect(std::string_view word);
	std::vector<std::string> CreateAllPermutatedQueries(const std::string& queryStart, std::vector<std::string> words,
														const std::string& queryEnd, const std::string& sep = " ");
	void CheckAllPermutations(const std::string& queryStart, const std::vector<std::string>& words, const std::string& queryEnd,
							  const std::vector<std::tuple<std::string, std::string>>& expectedResults, bool withOrder = false,
							  const std::string& sep = " ", bool withHighlight = true);

	void CheckResults(const std::string& query, std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder,
					  bool withHighlight = true);

	void CheckResults(const std::string& query, const reindexer::QueryResults& qr,
					  std::vector<std::tuple<std::string, std::string, std::string>> expectedResults, bool withOrder);
	void CheckResults(const std::string& query, const reindexer::QueryResults& qr,
					  std::vector<std::tuple<std::string, std::string>> expectedResults, bool withOrder);

	template <typename ResType>
	void CheckResults(const std::string& query, const reindexer::QueryResults& qr, std::vector<ResType>& expectedResults, bool withOrder);
	void CheckResultsByField(const reindexer::QueryResults& res, const std::set<std::string>& expected, std::string_view fieldName,
							 std::string_view description);

	std::vector<std::tuple<std::string, std::string>>& DelHighlightSign(std::vector<std::tuple<std::string, std::string>>& in);

protected:
	virtual std::string_view GetDefaultNamespace() noexcept = 0;

	struct [[nodiscard]] Data {
		std::string ft1;
		std::string ft2;
	};
	struct [[nodiscard]] FTDSLQueryParams {
		reindexer::RHashMap<std::string, reindexer::FtIndexFieldPros> fields;
		reindexer::StopWordsSetT stopWords;
	};
	int counter_ = 0;
	ReindexerTestApi<reindexer::Reindexer> rt;
};
