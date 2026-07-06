#pragma once

#include <memory>
#include <string>
#include "core/ft/config/ftconfig.h"
#include "estl/h_vector.h"
#include "tools/rhashset.h"

namespace reindexer {

constexpr static uint64_t kMaxSynonymSize = 64;

class [[nodiscard]] GroupOfSynonyms {
public:
	std::vector<std::vector<std::wstring>> queryWords;
	std::vector<std::vector<std::wstring>> alternatives;
	size_t maxWordParts = 0;

	void Parse(const FTConfig::Synonym& synonym, const SplitOptions& splitOptions) {
		std::wstring buf;
		std::vector<std::wstring> parts;

		for (const auto& tok : synonym.tokens) {
			split(tok, buf, parts, splitOptions);
			assertrx(parts.size() <= kMaxSynonymSize);
			maxWordParts = std::max(maxWordParts, parts.size());
			queryWords.emplace_back(std::move(parts));
		}

		for (const auto& alt : synonym.alternatives) {
			split(alt, buf, parts, splitOptions);
			alternatives.emplace_back(std::move(parts));
		}
	}
};

class [[nodiscard]] Synonyms {
public:
	using Ptr = std::unique_ptr<Synonyms>;

	Synonyms() = default;

	void FindOne2OneSubstitutions(const std::wstring& word, h_vector<std::wstring, 5>& wordSubstitutions) {
		wordSubstitutions.resize(0);
		size_t groupsProcessed = 0;
		if (auto it = index_.find(word); it != index_.end()) {
			for (WordInfo& info : it->second) {
				if (info.groupIdx < groupsProcessed) {
					continue;
				}

				const auto& group = groups_[info.groupIdx];
				if (group.queryWords[info.groupQueryWordIdx].size() != 1) {
					continue;
				}

				for (const auto& alt : group.alternatives) {
					if (alt.size() == 1 && alt[0] != word) {
						wordSubstitutions.push_back(alt[0]);
					}
				}

				groupsProcessed = info.groupIdx + 1;
			}
		}
	}

	struct [[nodiscard]] Substitution {
		std::vector<std::wstring> substitutionWords;
		std::vector<size_t> positionsSubstituted;
		float proc = 0.0;

		Substitution() = default;
		Substitution(const std::vector<std::wstring>& words, const std::vector<size_t>& positions, float p)
			: substitutionWords(words), positionsSubstituted(positions), proc(p) {}

		void TransformPositions(const std::vector<size_t>& positionsMapping) {
			for (size_t& pos : positionsSubstituted) {
				pos = positionsMapping[pos];
			}
		}
	};

	static bool atLeastOneNewWord(const RSet<std::wstring>& uniqueWords, const std::vector<std::wstring>& words) {
		for (const std::wstring& w : words) {
			if (uniqueWords.find(w) == uniqueWords.end()) {
				return true;
			}
		}

		return false;
	}

	template <typename VariantsType>
	void FindComplexSubstitutions(const std::vector<VariantsType>& queryWords, std::vector<Substitution>& substitutionsFound) {
		std::vector<std::pair<size_t, size_t>> substData(groups_.size() * maxGroupSize);
		std::vector<bool> groupsFound(groups_.size(), false);

		RSet<std::wstring> uniqueQueryWords;
		for (size_t wIdx = 0; wIdx < queryWords.size(); ++wIdx) {
			for (size_t vIdx = 0; vIdx < queryWords[wIdx].size(); ++vIdx) {
				uniqueQueryWords.insert(queryWords[wIdx][vIdx].pattern);
			}
		}

		for (size_t wIdx = 0; wIdx < queryWords.size(); ++wIdx) {
			for (size_t vIdx = 0; vIdx < queryWords[wIdx].size(); ++vIdx) {
				const std::wstring& variantPattern = queryWords[wIdx][vIdx].pattern;
				const float variantProc = queryWords[wIdx][vIdx].proc;

				if (!queryWords[wIdx][vIdx].synonyms) {
					continue;
				}

				if (auto it = index_.find(variantPattern); it != index_.end()) {
					for (const WordInfo& wi : it->second) {
						const GroupOfSynonyms& group = groups_[wi.groupIdx];
						groupsFound[wi.groupIdx] = true;
						auto& data =
							substData[wi.groupIdx * maxGroupSize + wi.groupQueryWordIdx * group.maxWordParts + wi.groupQueryWordPos];
						if (data.first == 0 || variantProc >= queryWords[data.first - 1][data.second - 1].proc) {
							data = {wIdx + 1, vIdx + 1};
						}
					}
				}
			}
		}

		for (size_t gIdx = 0; gIdx < groups_.size(); ++gIdx) {
			if (!groupsFound[gIdx]) {
				continue;
			}

			const GroupOfSynonyms& group = groups_[gIdx];
			float groupQueryProc = 0.0f;
			std::vector<size_t> groupQueryPositions;
			std::vector<size_t> positionsTmp;

			for (size_t wIdx = 0; wIdx < group.queryWords.size(); ++wIdx) {
				// NOLINTNEXTLINE(bugprone-use-after-move)
				positionsTmp.resize(0);
				float sumProc = 0.0f;

				for (size_t pos = 0; pos < group.queryWords[wIdx].size(); ++pos) {
					auto& data = substData[gIdx * maxGroupSize + wIdx * group.maxWordParts + pos];
					if (data.first > 0) {
						positionsTmp.emplace_back(data.first - 1);
						sumProc += queryWords[data.first - 1][data.second - 1].proc;
					} else {
						break;
					}
				}

				if (positionsTmp.size() == group.queryWords[wIdx].size() && sumProc > groupQueryProc) {
					groupQueryPositions = std::move(positionsTmp);
					groupQueryProc = sumProc;
				}
			}

			if (!groupQueryPositions.empty()) {
				for (const auto& alt : group.alternatives) {
					if ((groupQueryPositions.size() > 1 || alt.size() > 1) && atLeastOneNewWord(uniqueQueryWords, alt)) {
						substitutionsFound.emplace_back(Substitution{alt, groupQueryPositions, groupQueryProc});
					}
				}
			}
		}
	}

	void SetConfig(FTConfig* cfg) {
		groups_.resize(0);
		index_.clear();

		groups_.reserve(cfg->synonyms.size());
		for (const auto& synonym : cfg->synonyms) {
			groups_.emplace_back();
			groups_.back().Parse(synonym, cfg->splitOptions);
		}

		indexateSubstitutions();
	}

private:
	struct [[nodiscard]] WordInfo {
		size_t groupIdx = 0;
		size_t groupQueryWordIdx = 0;
		size_t groupQueryWordPos = 0;
	};

	std::vector<GroupOfSynonyms> groups_;
	RHashMap<std::wstring, std::vector<WordInfo>> index_;
	size_t maxGroupSize = 0;

	void indexateSubstitutions() {
		index_.clear();
		for (size_t gIdx = 0; gIdx < groups_.size(); ++gIdx) {
			const GroupOfSynonyms& group = groups_[gIdx];
			maxGroupSize = std::max(maxGroupSize, group.queryWords.size() * group.maxWordParts);
			for (size_t wIdx = 0; wIdx < group.queryWords.size(); ++wIdx) {
				const std::vector<std::wstring>& queryWords = group.queryWords[wIdx];
				for (size_t pos = 0; pos < queryWords.size(); ++pos) {
					index_[queryWords[pos]].emplace_back(WordInfo{.groupIdx = gIdx, .groupQueryWordIdx = wIdx, .groupQueryWordPos = pos});
				}
			}
		}
	}
};

}  // namespace reindexer
