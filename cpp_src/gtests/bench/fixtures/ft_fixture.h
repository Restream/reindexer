#pragma once

#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "base_fixture.h"
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/usingcontainer.h"
#include "helpers.h"
#include "tools/fsops.h"

// #define ENABLE_TIME_TRACKER

class FullText : private BaseFixture {
public:
	virtual ~FullText() {}
	FullText(Reindexer* db, const std::string& name, size_t maxItems);

	virtual reindexer::Error Initialize() override;
	void RegisterAllCases(size_t iterationCount = -1);

private:
	virtual reindexer::Item MakeItem(benchmark::State&) override;

	template <reindexer::FtFastConfig::Optimization>
	void UpdateIndex(State&);
	void Insert(State& state);
	void BuildInsertSteps(State& state);

	void BuildAndInsertLowWordsDiversityNs(State& state);

	void Fast3PhraseLowDiversity(State& state);
	void Fast3WordsLowDiversity(State& state);

	void Fast3PhraseWithAreasLowDiversity(State& state);
	void Fast3WordsWithAreasLowDiversity(State& state);

	void Fast1WordWithAreaHighDiversity(State& state);

	void Fast2PhraseLowDiversity(State& state);
	void Fast2AndWordLowDiversity(State& state);

	void BuildCommonIndexes(State& state);
	void BuildFastTextIndex(State& state);
	void BuildFuzzyTextIndex(State& state);

	void Fast1WordMatch(State& state);
	void Fast2WordsMatch(State& state);
	void Fuzzy1WordMatch(State& state);
	void Fuzzy2WordsMatch(State& state);

	void Fast1PrefixMatch(State& state);
	void Fast2PrefixMatch(State& state);
	void Fuzzy1PrefixMatch(State& state);
	void Fuzzy2PrefixMatch(State& state);

	void Fast1SuffixMatch(State& state);
	void Fast2SuffixMatch(State& state);
	void Fuzzy1SuffixMatch(State& state);
	void Fuzzy2SuffixMatch(State& state);

	void Fast1TypoWordMatch(State& state);
	void Fast2TypoWordMatch(State& state);
	void Fuzzy1TypoWordMatch(State& state);
	void Fuzzy2TypoWordMatch(State& state);

	void BuildStepFastIndex(State& state);
	void Last(State& state);

	template <reindexer::FtFastConfig::Optimization>
	void InitForAlternatingUpdatesAndSelects(State&);
	void AlternatingUpdatesAndSelects(benchmark::State&);
	void AlternatingUpdatesAndSelectsByComposite(benchmark::State&);
	void AlternatingUpdatesAndSelectsByCompositeByNotIndexFields(benchmark::State&);

	std::string CreatePhrase();

	std::string MakePrefixWord();
	std::string MakeSuffixWord();
	std::string MakeTypoWord();

	std::wstring GetRandomUTF16WordByLength(size_t minLen = 4);

	std::vector<std::string> GetRandomCountries(size_t cnt = 5);
	reindexer::Item MakeSpecialItem();

	std::vector<std::string> words_;
	std::vector<std::string> words2_;
	std::vector<std::string> countries_;
	struct Values {
		Values(std::string s1, std::string s2, std::string f1, std::string f2) noexcept
			: search1{std::move(s1)}, search2{std::move(s2)}, field1{std::move(f1)}, field2{std::move(f2)} {}
		std::string search1;
		std::string search2;
		std::string field1;
		std::string field2;
	};
	std::vector<Values> values_;

	class RegisterWrapper {
	public:
		//-1 test iteration limit - time
		RegisterWrapper(size_t iterationCoun = -1) : iterationCoun_(iterationCoun) {}
		Benchmark* SetOptions(Benchmark* b) {
			b = b->Unit(benchmark::kMicrosecond);
			if (iterationCoun_ != size_t(-1)) {
				b = b->Iterations(iterationCoun_);
			}
			return b;
		}

	private:
		size_t iterationCoun_;
	};
#ifdef ENABLE_TIME_TRACKER
#define TIMETRACKER(fileName) TimeTracker timeTracker(fileName);
#define TIMEMEASURE() TimeTracker::TimeMeasure t(timeTracker);
#else
#define TIMETRACKER(fileName)
#define TIMEMEASURE()
#endif

	class TimeTracker {
	public:
		TimeTracker(const std::string& fileName) : fileName_(fileName) { timeOfTest_.reserve(10000); }

		class TimeMeasure {
		public:
			TimeMeasure(TimeTracker& t) : timeTracker_(t), t1_(std::chrono::high_resolution_clock::now()) {}
			~TimeMeasure() {
				auto t2 = std::chrono::high_resolution_clock::now();
				int tUs = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1_).count();
				timeTracker_.timeOfTest_.push_back(tUs);
			}

		private:
			TimeTracker& timeTracker_;
			std::chrono::high_resolution_clock::time_point t1_;
		};
		friend class TimeMeasure;
		~TimeTracker() {
			int tMin = INT32_MAX;
			int tMax = 0;
			for (auto v : timeOfTest_) {
				if (v < tMin) tMin = v;
				if (v > tMax) tMax = v;
			}
			std::vector<int> gist;
			const int columnCount = 1000;
			gist.resize(columnCount, 0);
			double dt = double(tMax - tMin) / columnCount;
			int counter = 1;
			std::string baseFileName = fileName_;
			while (reindexer::fs::Stat(fileName_) == reindexer::fs::StatFile || reindexer::fs::Stat(fileName_) == reindexer::fs::StatDir) {
				fileName_ = baseFileName + std::to_string(counter);
				counter++;
			}
			std::ofstream fileOut(fileName_);
			if (fabs(dt) < 0.00000001 || timeOfTest_.size() < 2) {
				fileOut << "dt=0" << std::endl;
				return;
			}
			double averageTime = 0;
			for (auto v : timeOfTest_) {
				averageTime += v;
				int indx = double(v - tMin) / dt;
				if (indx >= columnCount) indx = columnCount - 1;
				gist[indx]++;
			}
			averageTime /= timeOfTest_.size();

			fileOut << "{" << std::endl;
			fileOut << "\"tMax\":" << tMax << ",\n\"tMin\":" << tMin << ",\n\"dt\":" << dt << ",\n\"averageTime\":" << averageTime << ","
					<< std::endl;
			fileOut << "\"data\":[" << std::endl;
			bool isFirst = true;
			for (auto v : gist) {
				if (!isFirst) fileOut << "," << std::endl;
				fileOut << v;
				isFirst = false;
			}
			fileOut << "],\n \"raw_data\":[" << std::endl;
			isFirst = true;
			for (auto v : timeOfTest_) {
				if (!isFirst) fileOut << "," << std::endl;
				fileOut << v;
				isFirst = false;
			}

			fileOut << "]" << std::endl;
			fileOut << "}" << std::endl;
		}

	private:
		std::vector<int> timeOfTest_;  // us
		std::string fileName_;
	};

	void updateAlternatingNs(reindexer::WrSerializer&, benchmark::State&);
	reindexer::Error readDictFile(const std::string& fileName, std::vector<std::string>& words);
	const char* alternatingNs_ = "FtAlternatingUpdatesAndSelects";

	size_t raw_data_sz_ = 0;
	std::mt19937 randomEngine_{1};
	std::uniform_int_distribution<int> randomGenerator_{};

	NamespaceDef lowWordsDiversityNsDef_;
};
