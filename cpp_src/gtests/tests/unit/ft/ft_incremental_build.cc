#include "ft_api.h"
#include "gtests/tests/gtest_cout.h"

using namespace std::string_view_literals;

class [[nodiscard]] FTIncrementalBuildApi : public FTApi {
public:
	enum class [[nodiscard]] QueryType { Simple, WithTypo, WithPrefix, WithSuffix };
	template <typename K, typename V>
	using MapT = reindexer::fast_hash_map<K, V>;
	template <typename K>
	using SetT = reindexer::fast_hash_set<K>;
	constexpr static int kMaxWordLen = 30;

	struct [[nodiscard]] StepInfo {
		unsigned wordsCnt;
		unsigned wordsInDoc;
	};

	struct [[nodiscard]] ExpectedDocs {
		unsigned totalCount = 0;
		MapT<std::string, unsigned> map;
	};

	struct [[nodiscard]] WordsData {
		MapT<std::string, MapT<std::string, unsigned>> docsByWords;
		MapT<unsigned, SetT<std::string>> wordsBySteps;
	};

	enum class [[nodiscard]] StrictSuffixValidation { No, Yes };

	void Init(const reindexer::FtFastConfig& ftCfg) {
		rt.reindexer = std::make_shared<reindexer::Reindexer>();
		rt.Connect("builtin://");
		rt.OpenNamespace(GetDefaultNamespace());
		rt.DefineNamespaceDataset(GetDefaultNamespace(), {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
														  IndexDeclaration{"ft1", "text", "string", IndexOpts(), 0},
														  IndexDeclaration{"ft2", "text", "string", IndexOpts(), 0},
														  IndexDeclaration{"ft1+ft2=ft3", "text", "composite", IndexOpts(), 0}});
		auto err = SetFTConfig(ftCfg, GetDefaultNamespace(), "ft3", {"ft1", "ft2"});
		ASSERT_TRUE(err.ok()) << err.what();
	}

	class [[nodiscard]] IWordGenerator {
	public:
		virtual std::string NewWord(unsigned step, ReindexerTestApi<reindexer::Reindexer>& rt) const = 0;
		virtual ~IWordGenerator() {}
	};
	class [[nodiscard]] RandWordGenerator : public IWordGenerator {
	public:
		std::string NewWord(unsigned /*step*/, ReindexerTestApi<reindexer::Reindexer>& rt) const override final {
			return rt.RandString(5, 5);
		}
	};
	class [[nodiscard]] PoolWordGenerator : public IWordGenerator {
	public:
		PoolWordGenerator(std::vector<std::string>&& pool) noexcept : wordsPool_(std::move(pool)) { assertrx(wordsPool_.size()); }
		std::string NewWord(unsigned /*step*/, ReindexerTestApi<reindexer::Reindexer>& /*rt*/) const override final {
			return wordsPool_[rand() % wordsPool_.size()];
		}

	private:
		std::vector<std::string> wordsPool_;
	};
	class [[nodiscard]] UniqueWordGenerator : public IWordGenerator {
	public:
		std::string NewWord(unsigned step, ReindexerTestApi<reindexer::Reindexer>& rt) const override final {
			// Word contains unique prefix/suffix for the further strict results validation
			return fmt::format("wrst{}{}st{}wr", step, rt.RandString(5, 1), step);
		}
	};

	class [[nodiscard]] PhraseGenerator : public IWordGenerator {
	public:
		PhraseGenerator(const IWordGenerator& wordsGen) : wordsGen_(wordsGen) {}

		std::string NewWord(unsigned step, ReindexerTestApi<reindexer::Reindexer>& rt) const override final {
			// can use only 2 different words, remove it after fixing #2148
			return wordsGen_.NewWord(step, rt) + "1 " + wordsGen_.NewWord(step, rt) + "2";
		}

	private:
		const IWordGenerator& wordsGen_;
	};

	class [[nodiscard]] DataDumpGuard {
	public:
		DataDumpGuard(const WordsData& wordsData) noexcept : wordsData_(wordsData) {}
		~DataDumpGuard() {
			if (::testing::Test::HasFailure()) {
				TestCout() << "Additional info:\n";
				TestCout() << "===docs by words:===\n";
				for (auto& it : wordsData_.docsByWords) {
					TestCout() << fmt::format("word: '{}';\n", it.first);
					TestCout() << DumpMap("DocsMap", it.second) << "\n";
				}
				TestCout() << "===step words:===\n";
				for (auto& it : wordsData_.wordsBySteps) {
					TestCout() << fmt::format("step: '{}';\n", it.first);
					TestCout() << DumpContainer("WordsSet", it.second) << "\n";
				}
				TestCout() << "======" << std::endl;
			}
		}

	private:
		const WordsData& wordsData_;
	};

	WordsData FillWithSteps(const std::vector<StepInfo>& steps, const IWordGenerator& wordsGen, const std::string& separator = "") {
		EXPECT_GT(steps.size(), 0);
		WordsData d;

		SetT<std::string> words;
		std::string doc;
		for (unsigned stID = 0; stID < steps.size(); ++stID) {
			auto& st = steps[stID];
			EXPECT_GT(st.wordsCnt, 0);
			EXPECT_GT(st.wordsInDoc, 0);
			for (unsigned i = 0; i < st.wordsCnt; ++i) {
				doc.clear();
				words.clear();
				for (unsigned j = 0; j < st.wordsInDoc; ++j) {
					if (!doc.empty()) {
						doc.append(" ");
						if (!separator.empty()) {
							doc.append(separator);
							doc.append(" ");
						}
					}
					std::string word = wordsGen.NewWord(stID, rt);
					EXPECT_LE(word.size(), kMaxWordLen) << word;
					doc.append(word);
					d.wordsBySteps[stID].emplace(word);
					words.emplace(std::move(word));
				}

				auto item = rt.NewItem(GetDefaultNamespace());
				item["id"] = counter_++;
				item["ft1"] = std::string();
				item["ft2"] = doc;
				rt.Upsert(GetDefaultNamespace(), item);

				for (auto& w : words) {
					auto& docsMap = d.docsByWords[w];
					auto [it, emplaced] = docsMap.emplace(doc, 1);
					if (!emplaced) {
						it->second += 1;
					}
				}
			}
			std::ignore = FTIncrementalBuildApi::SimpleSelect("build step");
		}
		return d;
	}
	template <QueryType qt>
	std::string BuildQuery(const SetT<std::string>& words, unsigned cnt, std::vector<std::string>& outWords) {
		std::string query;
		assertrx(!words.empty());
		outWords.clear();
		for (unsigned i = 0; i < cnt; ++i) {
			if (!query.empty()) {
				query.append(" ");
			}
			auto randWid = rand() % words.size();
			unsigned wid = 0;
			for (auto& wIt : words) {
				if (wid++ == randWid) {
					outWords.emplace_back(wIt);
					query.append("=");
					if constexpr (qt == QueryType::WithTypo) {
						auto wordWithTypo = wIt;
						auto letterID = rand() % wordWithTypo.size();
						wordWithTypo[letterID] = (wordWithTypo[letterID] == 'Z' ? 'X' : 'Z');
						wordWithTypo.append("~");
						query.append(wordWithTypo);
					} else if constexpr (qt == QueryType::WithSuffix) {
						EXPECT_GE(wIt.size(), 2) << wIt;
						query.append("*").append(wIt.begin() + 1, wIt.end());
					} else if constexpr (qt == QueryType::WithPrefix) {
						EXPECT_GE(wIt.size(), 2) << wIt;
						query.append(wIt);
						query.back() = '*';
					} else {
						query.append(wIt);
					}
					break;
				}
			}
		}
		return query;
	}

	std::string BuildPhraseQuery(const SetT<std::string>& phrases, std::string& outPhrase) {
		std::string query;
		assertrx(!phrases.empty());
		outPhrase.clear();

		auto randId = rand() % phrases.size();
		unsigned id = 0;
		for (const auto& p : phrases) {
			if (id++ == randId) {
				outPhrase = p;
				query = "'" + p + "'";
				break;
			}
		}

		return query;
	}

	reindexer::QueryResults SimpleSelect(std::string_view query) {
		auto q = reindexer::Query(GetDefaultNamespace()).Where("ft3", CondEq, query).WithRank();
		return rt.Select(q);
	}

	reindexer::FtFastConfig CreateConfig() {
		reindexer::FtFastConfig cfg(2);
		cfg.logLevel = 5;
		cfg.maxStepSize = 10;
		cfg.maxRebuildSteps = GetStepsCount();
		cfg.maxTypoLen = kMaxWordLen;
		cfg.stopWords = {reindexer::StopWord{"no", reindexer::StopWord::Type::Morpheme}};
		return cfg;
	}

	ExpectedDocs GetExpectedDocs(const std::vector<std::string>& words, const WordsData& wordsData) {
		ExpectedDocs expectedDocs;
		for (auto& w : words) {
			auto docsInt = wordsData.docsByWords.find(w);
			EXPECT_TRUE(docsInt != wordsData.docsByWords.end()) << "Unknown word (no documents found): " << w;
			auto& docsMap = docsInt->second;
			for (auto& docP : docsMap) {
				EXPECT_GT(docP.second, 0);
				auto [it, emplaced] = expectedDocs.map.emplace(docP.first, docP.second);
				if (emplaced) {
					expectedDocs.totalCount += docP.second;
				} else {
					EXPECT_EQ(it->second, docP.second)
						<< fmt::format("Inconsistent docs count: {} vs {}. Doc: '{}'. Cur word: '{}'. Words: {}", docP.second, it->second,
									   docP.first, w, DumpContainer("words", words));
				}
			}
		}
		return expectedDocs;
	}

	template <typename MapT>
	static std::string DumpMap(std::string_view name, const MapT& map) {
		std::stringstream ss;
		ss << fmt::format("==='{}'-map data:\n{{", name);
		int cnt = 0;
		for (auto& it : map) {
			ss << fmt::format(" '{}':'{}' ", it.first, it.second);
			if (++cnt % 10 == 0) {
				ss << "\n";
			}
		}
		ss << "}";
		return ss.str();
	}
	template <typename ContT>
	static std::string DumpContainer(std::string_view name, const ContT& vec) {
		std::stringstream ss;
		ss << fmt::format("\n'{}'-container data:\n{{", name);
		int cnt = 0;
		for (auto& v : vec) {
			ss << fmt::format(" '{}' ", v);
			if (++cnt % 10 == 0) {
				ss << "\n";
			}
		}
		ss << "}";
		return ss.str();
	}

	void ValidateExactResults(const std::vector<std::string>& words, const WordsData& wordsData, const reindexer::QueryResults& qr) {
		auto expectedDocs = GetExpectedDocs(words, wordsData);
		ASSERT_GT(expectedDocs.map.size(), 0) << "This method works with existing docs only";
		EXPECT_EQ(qr.Count(), expectedDocs.totalCount);
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			auto doc = item["ft2"].As<std::string>();
			auto docIt = expectedDocs.map.find(doc);
			ASSERT_TRUE(docIt != expectedDocs.map.end()) << fmt::format("Unexpected doc in QR: '{}'", doc);
			if (docIt->second == 1) {
				expectedDocs.map.erase(doc);
			} else {
				docIt->second -= 1;
			}
		}
		ASSERT_TRUE(expectedDocs.map.empty()) << "Missing docs: " << DumpMap("expectedDocs", expectedDocs.map);
	}

	void ValidateRequiredResults(const std::vector<std::string>& words, const WordsData& wordsData, const reindexer::QueryResults& qr) {
		auto expectedDocs = GetExpectedDocs(words, wordsData);
		ASSERT_GT(expectedDocs.map.size(), 0) << "This method works with existing docs only";
		EXPECT_GE(qr.Count(), expectedDocs.totalCount);
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			auto doc = item["ft2"].As<std::string>();
			if (auto docIt = expectedDocs.map.find(doc); docIt != expectedDocs.map.end()) {
				if (docIt->second == 1) {
					expectedDocs.map.erase(doc);
				} else {
					docIt->second -= 1;
				}
			} else {
				// Document may not exist and it's fine
			}
		}
		ASSERT_TRUE(expectedDocs.map.empty()) << "Missing docs: " << DumpMap("expectedDocs", expectedDocs.map);
	}

	template <StrictSuffixValidation strictSuffixValidation>
	void CheckStepsSelection(const WordsData& wordsData, const std::vector<StepInfo>& steps) {
		{
			SCOPED_TRACE("Select some words from the each step");
			std::vector<std::string> outWords;
			for (unsigned i = 0; i < steps.size(); ++i) {
				SCOPED_TRACE(fmt::format("Step {}", i));
				auto wordsInStep = wordsData.wordsBySteps.find(i);
				ASSERT_TRUE(wordsInStep != wordsData.wordsBySteps.end());
				auto query = BuildQuery<QueryType::Simple>(wordsInStep->second, 2, outWords);
				SCOPED_TRACE(fmt::format("Query '{}'; words: {}", query, DumpContainer("outWords", outWords)));
				auto res = FTIncrementalBuildApi::SimpleSelect(query);
				ValidateExactResults(outWords, wordsData, res);
				ASSERT_FALSE(::testing::Test::HasFailure());
			}
		}

		{
			SCOPED_TRACE("Select some words with typos from the each step");
			std::vector<std::string> outWords;
			for (unsigned i = 0; i < steps.size(); ++i) {
				SCOPED_TRACE(fmt::format("Step {}", i));
				auto wordsInStep = wordsData.wordsBySteps.find(i);
				ASSERT_TRUE(wordsInStep != wordsData.wordsBySteps.end());
				auto query = BuildQuery<QueryType::WithTypo>(wordsInStep->second, 2, outWords);
				SCOPED_TRACE(fmt::format("Query '{}'; words: {}", query, DumpContainer("outWords", outWords)));
				auto res = FTIncrementalBuildApi::SimpleSelect(query);
				ValidateRequiredResults(outWords, wordsData, res);
				ASSERT_FALSE(::testing::Test::HasFailure());
			}
		}

		{
			SCOPED_TRACE("Select some words with prefix from the each step");
			std::vector<std::string> outWords;
			for (unsigned i = 0; i < steps.size(); ++i) {
				SCOPED_TRACE(fmt::format("Step {}", i));
				auto wordsInStep = wordsData.wordsBySteps.find(i);
				ASSERT_TRUE(wordsInStep != wordsData.wordsBySteps.end());
				auto query = BuildQuery<QueryType::WithPrefix>(wordsInStep->second, 2, outWords);
				SCOPED_TRACE(fmt::format("Query '{}'; words: {}", query, DumpContainer("outWords", outWords)));
				auto res = FTIncrementalBuildApi::SimpleSelect(query);
				if constexpr (strictSuffixValidation == StrictSuffixValidation::Yes) {
					ValidateExactResults(outWords, wordsData, res);
				} else {
					ValidateRequiredResults(outWords, wordsData, res);
				}
				ASSERT_FALSE(::testing::Test::HasFailure());
			}
		}

		{
			SCOPED_TRACE("Select some words with suffix from the each step");
			std::vector<std::string> outWords;
			for (unsigned i = 0; i < steps.size(); ++i) {
				SCOPED_TRACE(fmt::format("Step {}", i));
				auto wordsInStep = wordsData.wordsBySteps.find(i);
				ASSERT_TRUE(wordsInStep != wordsData.wordsBySteps.end());
				auto query = BuildQuery<QueryType::WithSuffix>(wordsInStep->second, 2, outWords);
				SCOPED_TRACE(fmt::format("Query '{}'; words: {}", query, DumpContainer("outWords", outWords)));
				auto res = FTIncrementalBuildApi::SimpleSelect(query);
				if constexpr (strictSuffixValidation == StrictSuffixValidation::Yes) {
					ValidateExactResults(outWords, wordsData, res);
				} else {
					ValidateRequiredResults(outWords, wordsData, res);
				}
				ASSERT_FALSE(::testing::Test::HasFailure());
			}
		}
	}

	void CheckPhrasesSelection(const WordsData& wordsData, const std::vector<StepInfo>& steps) {
		SCOPED_TRACE("Select some phrases from the each step");
		std::string outPhrase;
		for (unsigned i = 0; i < steps.size(); ++i) {
			SCOPED_TRACE(fmt::format("Step {}", i));
			auto phrasesInStep = wordsData.wordsBySteps.find(i);
			ASSERT_TRUE(phrasesInStep != wordsData.wordsBySteps.end());
			auto query = BuildPhraseQuery(phrasesInStep->second, outPhrase);
			SCOPED_TRACE(fmt::format("Query '{}'; phrase: {}", query, outPhrase));
			auto res = FTIncrementalBuildApi::SimpleSelect(query);
			ValidateExactResults(std::vector<std::string>(1, outPhrase), wordsData, res);
			ASSERT_FALSE(::testing::Test::HasFailure());
		}
	}

	std::vector<std::string> CreateWordsPool(unsigned size) {
		std::vector<std::string> pool;
		SetT<std::string> poolSet;
		poolSet.reserve(size);

		while (poolSet.size() < size) {
			poolSet.emplace(rt.RandString(5, 5));
		}
		pool.resize(size);
		std::copy(poolSet.begin(), poolSet.end(), pool.begin());
		return pool;
	}

	std::vector<FTIncrementalBuildApi::StepInfo> InitIncrementalIndexIncreasingSteps() {
		const auto ftCfg = CreateConfig();
		FTIncrementalBuildApi::Init(ftCfg);

		// Create steps config
		std::vector<FTIncrementalBuildApi::StepInfo> steps;
		steps.reserve(ftCfg.maxRebuildSteps);
		unsigned cnt = 15;
		EXPECT_LT(ftCfg.maxStepSize, cnt);
		for (int i = 0; i < ftCfg.maxRebuildSteps; ++i) {
			steps.emplace_back(FTIncrementalBuildApi::StepInfo{.wordsCnt = cnt, .wordsInDoc = 3});
			cnt += 5;
		}
		return steps;
	}
	std::vector<FTIncrementalBuildApi::StepInfo> InitIncrementalIndexDecreasingSteps() {
		const auto ftCfg = CreateConfig();
		FTIncrementalBuildApi::Init(ftCfg);

		// Create steps config
		std::vector<FTIncrementalBuildApi::StepInfo> steps;
		steps.reserve(ftCfg.maxRebuildSteps);
		unsigned cnt = 15 + 5 * ftCfg.maxRebuildSteps;
		EXPECT_LT(ftCfg.maxStepSize, cnt);
		for (int i = 0; i < ftCfg.maxRebuildSteps; ++i) {
			steps.emplace_back(FTIncrementalBuildApi::StepInfo{.wordsCnt = cnt, .wordsInDoc = 3});
			cnt -= 5;
		}
		return steps;
	}

	static unsigned GetStepsCount() { return 30 + rand() % 21; }

protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_inc_build_default_namespace"; }
};

TEST_F(FTIncrementalBuildApi, IncreasingStepsSize) {
	// Test with random words in each step and increasing step sizes
	const auto steps = InitIncrementalIndexIncreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, RandWordGenerator());
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::No>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, DecreasingStepsSize) {
	// Test with random words in each step and decreasing step sizes
	const auto steps = InitIncrementalIndexDecreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, RandWordGenerator());
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::No>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, IncreasingStepsSizeWordsPool) {
	// Test with low diversity words pool and increasing step sizes
	const auto steps = InitIncrementalIndexIncreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, PoolWordGenerator(CreateWordsPool(200)));
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::No>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, DecreasingStepsSizeWordsPool) {
	// Test with low diversity words pool and decreasing step sizes
	const auto steps = InitIncrementalIndexDecreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, PoolWordGenerator(CreateWordsPool(200)));
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::No>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, IncreasingStepsSizeUniqueWords) {
	// Test with unique words in each step and increasing step sizes
	const auto steps = InitIncrementalIndexIncreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, UniqueWordGenerator());
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::Yes>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, DecreasingStepsSizeWordsUniqueWords) {
	// Test with unique words in each step and decreasing step sizes
	const auto steps = InitIncrementalIndexDecreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	const auto wordsData = FillWithSteps(steps, UniqueWordGenerator());
	DataDumpGuard g(wordsData);
	CheckStepsSelection<StrictSuffixValidation::Yes>(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, IncreasingStepsSizePhrasesWordsPool) {
	// Test with low diversity words pool phrases and increasing step sizes
	const auto steps = InitIncrementalIndexIncreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	PoolWordGenerator pGen(CreateWordsPool(20));
	const std::string separator = "11";
	const auto wordsData = FillWithSteps(steps, PhraseGenerator(pGen), separator);
	DataDumpGuard g(wordsData);
	CheckPhrasesSelection(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, DecreasingStepsSizePhrasesWordsPool) {
	// Test with low diversity words pool phrases and decreasing step sizes
	const auto steps = InitIncrementalIndexDecreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	PoolWordGenerator pGen(CreateWordsPool(20));
	const std::string separator = "11";
	const auto wordsData = FillWithSteps(steps, PhraseGenerator(pGen), separator);
	DataDumpGuard g(wordsData);
	CheckPhrasesSelection(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, IncreasingStepsSizePhrasesRandomWords) {
	// Test with random words phrases and increasing step sizes
	const auto steps = InitIncrementalIndexIncreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	RandWordGenerator pGen;
	const std::string separator = "11";
	const auto wordsData = FillWithSteps(steps, PhraseGenerator(pGen), separator);
	DataDumpGuard g(wordsData);
	CheckPhrasesSelection(wordsData, steps);
}

TEST_F(FTIncrementalBuildApi, DecreasingStepsSizePhrasesRandomWords) {
	// Test with random words phrases and decreasing step sizes
	const auto steps = InitIncrementalIndexDecreasingSteps();
	SCOPED_TRACE(fmt::format("Steps count: {}", steps.size()));
	RandWordGenerator pGen;
	const std::string separator = "11";
	const auto wordsData = FillWithSteps(steps, PhraseGenerator(pGen), separator);
	DataDumpGuard g(wordsData);
	CheckPhrasesSelection(wordsData, steps);
}
