#pragma once

#include "core/ft/usingcontainer.h"
#include "itokenfilter.h"

namespace reindexer {

struct FtDslOpts;

class Synonyms : public ITokenFilter {
public:
	Synonyms() = default;
	virtual void GetVariants(const std::wstring& data, std::vector<FtDSLVariant>& result, int proc) override final;
	void SetConfig(BaseFTConfig* cfg) override final;
	void PreProcess(const FtDSLQuery&, std::vector<SynonymsDsl>&, int proc) const override final;
	void PostProcess(const FtDSLEntry&, const FtDSLQuery&, size_t termIdx, std::vector<SynonymsDsl>&, int proc) const override final;

private:
	using SingleAlternativeCont = std::vector<std::wstring>;
	using MultipleAlternativesCont = std::vector<RVector<std::wstring, 2>>;

	static void addDslEntries(std::vector<SynonymsDsl>&, const MultipleAlternativesCont&, const FtDslOpts&,
							  const std::vector<size_t>& termsIdx, const FtDSLQuery&);

	// word - single-word synonyms
	RHashMap<std::wstring, std::shared_ptr<SingleAlternativeCont>> one2one_;
	// word - multi-word synonyms
	RHashMap<std::wstring, std::shared_ptr<MultipleAlternativesCont>> one2many_;
	// list of pairs {phrase ( few words) - list of synonyms, both single-word and multi-word}
	std::vector<std::pair<RVector<std::wstring, 2>, std::shared_ptr<MultipleAlternativesCont>>> many2any_;
};
}  // namespace reindexer
