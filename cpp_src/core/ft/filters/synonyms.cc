#include "synonyms.h"
#include "core/ft/config/baseftconfig.h"
#include "core/ft/ftdsl.h"
#include "tools/stringstools.h"

namespace reindexer {

constexpr int kSynonymProc = 95;

Synonyms::Synonyms() {}
void Synonyms::GetVariants(const wstring& data, std::vector<std::pair<std::wstring, int>>& result) {
	if (one2one_.empty()) return;

	auto it = one2one_.find(data);
	if (it == one2one_.end()) {
		return;
	}
	for (auto ait : *it->second) {
		result.push_back({ait, kSynonymProc});
	}
}

void Synonyms::addDslEntries(std::vector<SynonymsDsl>& synonymsDsl, const MultipleAlternativesCont& multiAlternatives,
							 const FtDslOpts& opts, const std::vector<size_t>& termsIdx, const FtDSLQuery& dsl) {
	for (const auto& alternatives : multiAlternatives) {
		assert(!alternatives.empty());
		synonymsDsl.emplace_back(dsl.CopyCtx(), termsIdx);

		for (const auto& alt : alternatives) {
			synonymsDsl.back().dsl.emplace_back(alt, opts);
		}
	}
}

static FtDslOpts makeOptsForAlternatives(const FtDslOpts& termOpts) {
	FtDslOpts result;
	result.op = OpAnd;
	result.boost = termOpts.boost * kSynonymProc / 100.0;
	result.termLenBoost = termOpts.termLenBoost;
	result.fieldsBoost = termOpts.fieldsBoost;
	result.qpos = termOpts.qpos;
	return result;
}

static void addOptsForAlternatives(FtDslOpts& opts, const FtDslOpts& termOpts) {
	opts.boost += termOpts.boost * kSynonymProc / 100.0;
	opts.termLenBoost += termOpts.termLenBoost;
	for (size_t i = 0, end = std::min(opts.fieldsBoost.size(), termOpts.fieldsBoost.size()); i != end; ++i) {
		opts.fieldsBoost[i] += termOpts.fieldsBoost[i];
	}
	if (opts.fieldsBoost.size() < termOpts.fieldsBoost.size()) {
		opts.fieldsBoost.insert(opts.fieldsBoost.end(), termOpts.fieldsBoost.cbegin() + opts.fieldsBoost.size(),
								termOpts.fieldsBoost.cend());
	}
	opts.qpos += termOpts.qpos;
}

static void divOptsForAlternatives(FtDslOpts& opts, size_t size) {
	assert(size != 0);
	opts.boost /= size;
	opts.termLenBoost /= size;
	for (float& fBoost : opts.fieldsBoost) fBoost /= size;
	opts.qpos /= size;
}

void Synonyms::PostProcess(const FtDSLEntry& term, const FtDSLQuery& dsl, size_t termIdx, std::vector<SynonymsDsl>& synonymsDsl) const {
	auto it = one2many_.find(term.pattern);
	if (it == one2many_.end()) {
		return;
	}

	const auto opts = makeOptsForAlternatives(term.opts);

	assert(it->second);
	addDslEntries(synonymsDsl, *it->second, opts, {termIdx}, dsl);
}

void Synonyms::PreProcess(const FtDSLQuery& dsl, std::vector<SynonymsDsl>& synonymsDsl) const {
	for (const auto& multiSynonyms : many2any_) {
		bool match = !multiSynonyms.first.empty();
		FtDslOpts opts;
		std::vector<size_t> termsIdx;
		for (auto termIt = multiSynonyms.first.cbegin(); termIt != multiSynonyms.first.cend(); ++termIt) {
			const auto isAppropriateEntry = [&termIt](const FtDSLEntry& dslEntry) {
				return dslEntry.opts.op != OpNot && dslEntry.pattern == *termIt;
			};
			const auto dslIt = std::find_if(dsl.cbegin(), dsl.cend(), isAppropriateEntry);
			if (dslIt == dsl.cend()) {
				match = false;
				break;
			} else {
				if (termIt == multiSynonyms.first.cbegin()) {
					opts = makeOptsForAlternatives(dslIt->opts);
				} else {
					addOptsForAlternatives(opts, dslIt->opts);
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
			assert(multiSynonyms.second);
			addDslEntries(synonymsDsl, *multiSynonyms.second, opts, termsIdx, dsl);
		}
	}
}
void Synonyms::SetConfig(BaseFTConfig* cfg) {
	one2one_.clear();
	one2many_.clear();
	many2any_.clear();

	wstring buf;
	vector<wstring> resultOfSplit;
	for (const auto& synonym : cfg->synonyms) {
		auto singleAlternatives = std::make_shared<SingleAlternativeCont>();
		auto multipleAlternatives = std::make_shared<MultipleAlternativesCont>();
		for (const auto& alt : synonym.alternatives) {
			split(alt, buf, resultOfSplit, cfg->extraWordSymbols);
			if (resultOfSplit.size() == 1) {
				singleAlternatives->emplace_back(std::move(resultOfSplit[0]));
			} else if (resultOfSplit.size() > 1) {
				multipleAlternatives->emplace_back(resultOfSplit.size());
				assert(multipleAlternatives->back().size() == resultOfSplit.size());
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
					for (const wstring& singleAlt : *singleAlternatives) {
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
