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

void Synonyms::addDslEntries(std::vector<SynonymsDsl>& synonymsDsl, const MultipleAlternativesCont& multiAlternatives,
							 const FtDslOpts& opts, const std::vector<size_t>& termsIdx, const FtDSLQuery& dsl) {
	for (const auto& alternatives : multiAlternatives) {
		assertrx(!alternatives.empty());
		synonymsDsl.emplace_back(dsl.CopyCtx(), termsIdx);

		for (const auto& alt : alternatives) {
			std::ignore = synonymsDsl.back().dsl.AddTerm(alt, opts);
		}
	}
}

static FtDslOpts makeOptsForAlternatives(const FtDslOpts& termOpts, int proc) {
	FtDslOpts result;
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

void Synonyms::AddOneToManySynonyms(const std::wstring& pattern, const FtDslOpts& opts, const FtDSLQuery& dsl, size_t termIdx,
									std::vector<SynonymsDsl>& synonymsDsl, int proc) const {
	if (opts.groupNum != -1) {
		// Skip multiword synonyms for phrase search
		return;
	}

	if (auto it = one2many_.find(pattern); it != one2many_.end()) {
		assertrx_throw(it->second);
		addDslEntries(synonymsDsl, *it->second, makeOptsForAlternatives(opts, proc), {termIdx}, dsl);
	}
}

void Synonyms::addPhraseAlternatives(const FtDSLQuery& dsl, const h_vector<std::wstring, 2>& phrase,
									 const MultipleAlternativesCont& phraseAlternatives, std::vector<SynonymsDsl>& synonymsDsl, int proc) {
	FtDslOpts opts;
	std::vector<size_t> termsIdx;

	for (auto pIt = phrase.begin(); pIt != phrase.end(); ++pIt) {
		const bool firstPhraseWord = (pIt == phrase.cbegin());

		const auto isAppropriate = [&pIt](const FtDSLEntry& e) { return e.opts.op != OpNot && e.opts.groupNum == -1 && e.pattern == *pIt; };
		const auto termIt = std::find_if(dsl.begin(), dsl.end(), isAppropriate);

		if (termIt == dsl.end()) {
			return;	 // phrase not found
		}

		if (firstPhraseWord) {
			opts = makeOptsForAlternatives(termIt->opts, proc);
		} else {
			addOptsForAlternatives(opts, termIt->opts, proc);
		}

		// Probably we don't need this?
		size_t idx = termIt - dsl.begin();
		termsIdx.push_back(idx);
		for (++idx; idx < dsl.NumTerms(); ++idx) {
			if (isAppropriate(dsl.GetTerm(idx))) {
				termsIdx.push_back(idx);
			}
		}
	}

	divOptsForAlternatives(opts, phrase.size());
	addDslEntries(synonymsDsl, phraseAlternatives, opts, termsIdx, dsl);
}

void Synonyms::AddManyToManySynonyms(const FtDSLQuery& dsl, std::vector<SynonymsDsl>& synonymsDsl, int proc) const {
	for (const auto& multiSynonyms : many2any_) {
		assertrx_throw(!multiSynonyms.first.empty());
		assertrx_throw(multiSynonyms.second);
		addPhraseAlternatives(dsl, multiSynonyms.first, *multiSynonyms.second.get(), synonymsDsl, proc);
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
