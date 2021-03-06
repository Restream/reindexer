#pragma once

#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "itokenfilter.h"
#include "tools/stringstools.h"

namespace reindexer {

struct FtDslOpts;

class Synonyms : public ITokenFilter {
public:
	Synonyms();
	virtual void GetVariants(const wstring& data, std::vector<std::pair<std::wstring, int>>& result) override final;
	void SetConfig(BaseFTConfig* cfg) override final;
	void PreProcess(const FtDSLQuery&, std::vector<SynonymsDsl>&) const override final;
	void PostProcess(const FtDSLEntry&, const FtDSLQuery&, size_t termIdx, std::vector<SynonymsDsl>&) const override final;

private:
	using SingleAlternativeCont = vector<wstring>;
	using MultipleAlternativesCont = vector<h_vector<wstring, 2>>;

	static void addDslEntries(std::vector<SynonymsDsl>&, const MultipleAlternativesCont&, const FtDslOpts&,
							  const std::vector<size_t>& termsIdx, const FtDSLQuery&);

	fast_hash_map<wstring, std::shared_ptr<SingleAlternativeCont>> one2one_;
	fast_hash_map<wstring, std::shared_ptr<MultipleAlternativesCont>> one2many_;
	vector<std::pair<h_vector<wstring, 2>, std::shared_ptr<MultipleAlternativesCont>>> many2any_;
};
}  // namespace reindexer
