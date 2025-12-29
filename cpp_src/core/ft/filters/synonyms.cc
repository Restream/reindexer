#include "synonyms.h"
#include "core/ft/config/baseftconfig.h"
#include "core/ft/ftdsl.h"
#include "tools/stringstools.h"

namespace reindexer {

void Synonyms::GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
						   fast_hash_map<std::wstring, size_t>& patternsUsed) {
	if (one2one_.empty()) {
		return;
	}

	auto it = one2one_.find(data);
	if (it == one2one_.end()) {
		return;
	}
	for (const auto& ait : *it->second) {
		AddOrUpdateVariant(result, patternsUsed, {ait, proc, PrefAndStemmersForbidden_False});
	}
}

static FtDslOpts makeOptsForAlternatives(const FtDslOpts& termOpts, int proc) {
	FtDslOpts result;
	result.pref = false;
	result.suff = false;
	result.op = OpAnd;
	result.boost = termOpts.boost * proc / 100.0f;
	result.termLenBoost = termOpts.termLenBoost;
	result.fieldsOpts = termOpts.fieldsOpts;
	result.qpos = termOpts.qpos;
	return result;
}

static void addOptsForAlternatives(FtDslOpts& opts, const FtDslOpts& termOpts, int proc) {
	opts.boost += termOpts.boost * proc / 100.0f;
	opts.termLenBoost += termOpts.termLenBoost;
	assertrx(opts.fieldsOpts.size() == termOpts.fieldsOpts.size());
	for (size_t i = 0, end = opts.fieldsOpts.size(); i != end; ++i) {
		assertrx(opts.fieldsOpts[i].needSumRank == termOpts.fieldsOpts[i].needSumRank);
		opts.fieldsOpts[i].boost += termOpts.fieldsOpts[i].boost;
	}
	opts.qpos += termOpts.qpos;
}

static void divOptsForAlternatives(FtDslOpts& opts, size_t size) {
	assertrx(size != 0);
	opts.boost /= size;
	opts.termLenBoost /= size;
	for (auto& fOpts : opts.fieldsOpts) {
		fOpts.boost /= size;
	}
	opts.qpos /= size;
}

void Synonyms::AddOneToManySynonyms(const std::wstring& pattern, const FtDslOpts& opts, size_t termIdx, int proc,
									std::vector<MultiWord>& synonyms) const {
	if (opts.groupNum != -1) {
		// Skip multiword synonyms for phrase search
		return;
	}

	const h_vector<size_t, 2> termsIdxs{termIdx};
	const FtDslOpts alternativeOpts = makeOptsForAlternatives(opts, proc);

	if (auto it = one2many_.find(pattern); it != one2many_.end()) {
		assertrx_throw(it->second);
		for (const auto& phrase : *it->second) {
			assertrx_throw(!phrase.empty());
			synonyms.emplace_back(phrase, alternativeOpts, termsIdxs);
		}
	}
}

void Synonyms::addPhraseAlternatives(const FtDSLQuery& query, const h_vector<std::wstring, 2>& phrase,
									 const MultipleAlternativesCont& phraseAlternatives, int proc, std::vector<MultiWord>& synonyms) {
	FtDslOpts opts;
	h_vector<size_t, 2> termsIdx;

	for (auto pIt = phrase.begin(); pIt != phrase.end(); ++pIt) {
		const bool firstPhraseWord = (pIt == phrase.cbegin());

		const auto isAppropriate = [&pIt](const FtDSLEntry& e) {
			return e.Opts().op != OpNot && e.Opts().groupNum == -1 && e.Pattern() == *pIt;
		};
		const auto termIt = std::find_if(query.begin(), query.end(), isAppropriate);

		if (termIt == query.end()) {
			return;	 // phrase not found
		}

		if (firstPhraseWord) {
			opts = makeOptsForAlternatives(termIt->Opts(), proc);
		} else {
			addOptsForAlternatives(opts, termIt->Opts(), proc);
		}

		// Probably we don't need this?
		size_t idx = termIt - query.begin();
		termsIdx.push_back(idx);
		for (++idx; idx < query.NumTerms(); ++idx) {
			if (isAppropriate(query.GetTerm(idx))) {
				termsIdx.push_back(idx);
			}
		}
	}

	divOptsForAlternatives(opts, phrase.size());
	for (const auto& phrase : phraseAlternatives) {
		assertrx_throw(!phrase.empty());
		synonyms.emplace_back(phrase, opts, termsIdx);
	}
}

void Synonyms::AddManyToManySynonyms(const FtDSLQuery& query, int proc, std::vector<MultiWord>& synonyms) const {
	for (const auto& [multiWord, multiWordSynonyms] : many2any_) {
		assertrx_throw(!multiWord.empty());
		assertrx_throw(multiWordSynonyms);
		addPhraseAlternatives(query, multiWord, *multiWordSynonyms, proc, synonyms);
	}
}

void Synonyms::SetConfig(BaseFTConfig* cfg) {
	one2one_.clear();
	one2many_.clear();
	many2any_.clear();

	std::wstring buf;
	std::vector<std::wstring> resultOfSplit;
	for (const auto& synonym : cfg->synonyms) {
		auto singleAlternatives = std::make_shared<SingleAlternativeCont>();
		auto multipleAlternatives = std::make_shared<MultipleAlternativesCont>();
		for (const auto& alt : synonym.alternatives) {
			split(alt, buf, resultOfSplit, cfg->splitOptions);
			if (resultOfSplit.size() == 1) {
				singleAlternatives->emplace_back(std::move(resultOfSplit[0]));
			} else if (resultOfSplit.size() > 1) {
				multipleAlternatives->emplace_back(resultOfSplit.size());
				assertrx(multipleAlternatives->back().size() == resultOfSplit.size());
				for (size_t i = 0; i < resultOfSplit.size(); ++i) {
					multipleAlternatives->back()[i] = std::move(resultOfSplit[i]);
				}
			}
		}
		for (const auto& token : synonym.tokens) {
			split(token, buf, resultOfSplit, cfg->splitOptions);
			if (resultOfSplit.size() == 1) {
				if (!singleAlternatives->empty()) {
					const auto res = one2one_.emplace(resultOfSplit[0], singleAlternatives);
					if (!res.second) {
						if (res.first->second.use_count() != 1) {
							res.first->second = std::make_shared<SingleAlternativeCont>(*res.first->second);
						}
						res.first->second->insert(res.first->second->end(), singleAlternatives->begin(), singleAlternatives->end());
					}
				}
				if (!multipleAlternatives->empty()) {
					const auto res = one2many_.emplace(resultOfSplit[0], multipleAlternatives);
					if (!res.second) {
						if (res.first->second.use_count() != 1) {
							res.first->second = std::make_shared<MultipleAlternativesCont>(*res.first->second);
						}
						res.first->second->insert(res.first->second->end(), multipleAlternatives->begin(), multipleAlternatives->end());
					}
				}
			} else if (resultOfSplit.size() > 1) {
				if (!singleAlternatives->empty()) {
					auto multAlt = std::make_shared<MultipleAlternativesCont>();
					for (const std::wstring& singleAlt : *singleAlternatives) {
						multAlt->push_back({singleAlt});
					}
					many2any_.emplace_back(h_vector<std::wstring, 2>{resultOfSplit.begin(), resultOfSplit.end()}, std::move(multAlt));
				}
				if (!multipleAlternatives->empty()) {
					if (singleAlternatives->empty()) {
						many2any_.push_back({{resultOfSplit.begin(), resultOfSplit.end()}, multipleAlternatives});
					} else {
						many2any_.back().second->insert(many2any_.back().second->end(), multipleAlternatives->cbegin(),
														multipleAlternatives->cend());
					}
				}
			}
		}
	}
}

}  // namespace reindexer
