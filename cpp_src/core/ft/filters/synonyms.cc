#include "synonyms.h"
#include "core/ft/config/baseftconfig.h"
#include "core/ft/ftdsl.h"
#include "tools/stringstools.h"

namespace reindexer {

void Synonyms::GetVariants(const std::wstring& data, std::vector<FtDSLVariant>& result, int proc) {
	if (one2one_.empty()) {
		return;
	}

	auto it = one2one_.find(data);
	if (it == one2one_.end()) {
		return;
	}
	for (const auto& ait : *it->second) {
		result.emplace_back(ait, proc);
	}
}

void Synonyms::addDslEntries(std::vector<SynonymsDsl>& synonymsDsl, const MultipleAlternativesCont& multiAlternatives,
							 const FtDslOpts& opts, const std::vector<size_t>& termsIdx, const FtDSLQuery& dsl) {
	for (const auto& alternatives : multiAlternatives) {
		assertrx(!alternatives.empty());
		synonymsDsl.emplace_back(dsl.CopyCtx(), termsIdx);

		for (const auto& alt : alternatives) {
			synonymsDsl.back().dsl.emplace_back(alt, opts);
		}
	}
}

static FtDslOpts makeOptsForAlternatives(const FtDslOpts& termOpts, int proc) {
	FtDslOpts result;
	result.op = OpAnd;
	result.boost = termOpts.boost * proc / 100.0;
	result.termLenBoost = termOpts.termLenBoost;
	result.fieldsOpts = termOpts.fieldsOpts;
	result.qpos = termOpts.qpos;
	return result;
}

static void addOptsForAlternatives(FtDslOpts& opts, const FtDslOpts& termOpts, int proc) {
	opts.boost += termOpts.boost * proc / 100.0;
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

void Synonyms::PostProcess(const FtDSLEntry& term, const FtDSLQuery& dsl, size_t termIdx, std::vector<SynonymsDsl>& synonymsDsl,
						   int proc) const {
	if (term.opts.groupNum != -1) {
		// Skip multiword synonyms for phrase search
		return;
	}
	auto it = one2many_.find(term.pattern);
	if (it == one2many_.end()) {
		return;
	}

	const auto opts = makeOptsForAlternatives(term.opts, proc);

	assertrx(it->second);
	addDslEntries(synonymsDsl, *it->second, opts, {termIdx}, dsl);
}

void Synonyms::PreProcess(const FtDSLQuery& dsl, std::vector<SynonymsDsl>& synonymsDsl, int proc) const {
	for (const auto& multiSynonyms : many2any_) {
		bool match = !multiSynonyms.first.empty();
		FtDslOpts opts;
		std::vector<size_t> termsIdx;
		for (auto termIt = multiSynonyms.first.cbegin(); termIt != multiSynonyms.first.cend(); ++termIt) {
			const auto isAppropriateEntry = [&termIt](const FtDSLEntry& dslEntry) {
				return dslEntry.opts.op != OpNot && dslEntry.opts.groupNum == -1 && dslEntry.pattern == *termIt;
			};
			const auto dslIt = std::find_if(dsl.cbegin(), dsl.cend(), isAppropriateEntry);
			if (dslIt == dsl.cend()) {
				match = false;
				break;
			} else {
				if (termIt == multiSynonyms.first.cbegin()) {
					opts = makeOptsForAlternatives(dslIt->opts, proc);
				} else {
					addOptsForAlternatives(opts, dslIt->opts, proc);
				}
				size_t idx = dslIt - dsl.cbegin();
				termsIdx.push_back(idx);
				for (++idx; idx < dsl.size(); ++idx) {
					if (isAppropriateEntry(dsl[idx])) {
						termsIdx.push_back(idx);
					}
				}
			}
		}
		if (match) {
			divOptsForAlternatives(opts, multiSynonyms.first.size());
			assertrx(multiSynonyms.second);
			addDslEntries(synonymsDsl, *multiSynonyms.second, opts, termsIdx, dsl);
		}
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
			split(alt, buf, resultOfSplit, cfg->extraWordSymbols);
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
			split(token, buf, resultOfSplit, cfg->extraWordSymbols);
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
					many2any_.push_back({{resultOfSplit.begin(), resultOfSplit.end()}, std::move(multAlt)});
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
