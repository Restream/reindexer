#include <gtest/gtest-param-test.h>
#include "ft_api.h"

class [[nodiscard]] FTMinRelevanceApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_min_relevance"; }
	void CreateNs() {
		const std::string_view nmName = GetDefaultNamespace();
		rt.OpenNamespace(nmName);
		rt.DefineNamespaceDataset(nmName, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
										   IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0}});
		reindexer::FtFastConfig cfg(0);
		cfg.enableNumbersSearch = true;
		cfg.logLevel = 5;
		cfg.maxStepSize = 100;
		auto err = SetFTConfig(cfg, nmName, "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
	}
	std::set<size_t> SelectIDs(std::string_view str) {
		auto qr = SimpleSelect(GetDefaultNamespace(), "ft1", str, false);
		std::set<size_t> selectDocs;
		for (auto v : qr) {
			reindexer::Item it = v.GetItem();
			selectDocs.insert(it["id"].As<int>());
		}
		return selectDocs;
	}
	void DelDocuments() {
		auto deleted = rt.Delete(reindexer::Query(GetDefaultNamespace()).Where("ft1", CondEq, words_[deleteWordIndex_]));
		ASSERT_GT(deleted, 0);
	}

	const size_t deleteWordIndex_ = 1;
	const std::vector<std::string> words_{"машина", "стол", "велосипед", "автобус"};
};

TEST_F(FTMinRelevanceApi, CorrectDocWithMinRelevanceAndEmptyDoc) {
	CreateNs();

	// Fill namespace
	std::map<int, std::map<int, std::pair<std::string, bool>>> docs;
	std::set<size_t> deleteDocs;
	int id = 0;
	constexpr int vDocCount = 300;
	const std::string_view nmName = GetDefaultNamespace();
	for (int i = 0; i < vDocCount; i++) {
		auto [it, _] = docs.insert(std::make_pair(i, std::map<int, std::pair<std::string, bool>>()));
		for (int k = 0; k < (i % 10) + 1; k++) {
			const size_t w1 = rand() % words_.size();
			const size_t w2 = rand() % words_.size();
			if (w1 == deleteWordIndex_ || w2 == deleteWordIndex_) {
				deleteDocs.insert(id);
			}
			std::string doc = words_[w1] + " " + words_[w2] + " " + std::to_string(i);
			it->second.insert(std::make_pair(id, std::make_pair(doc, true)));
			reindexer::Item item = rt.NewItem(nmName);
			item["id"] = id;
			item["ft1"] = doc;
			rt.Upsert(nmName, item);
			id++;
		}
		SelectIDs("build");
	}
	// Delete documents with some unique word
	DelDocuments();

	// Check, that there are no deleted docs in results
	const auto selectDocs = SelectIDs("машина автобус");
	std::vector<size_t> intersection;
	std::set_intersection(deleteDocs.begin(), deleteDocs.end(), selectDocs.begin(), selectDocs.end(), std::back_inserter(intersection));
	if (!intersection.empty()) {
		std::stringstream ss;
		for (auto& v : intersection) {
			ss << v << " ";
		}
		ASSERT_TRUE(false) << "Intersection must be empty: " << ss.str();
	}
}
