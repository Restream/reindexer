#pragma once

#include "itokenfilter.h"

namespace reindexer {

struct FtDslOpts;

struct [[nodiscard]] SynonymsDsl {
	SynonymsDsl(FtDSLQuery&& dsl_, const std::vector<size_t>& termsIdx_) : dsl{std::move(dsl_)}, termsIdx{termsIdx_} {}
	FtDSLQuery dsl;
	std::vector<size_t> termsIdx;
};

class [[nodiscard]] Synonyms final : public ITokenFilter {
public:
	using Ptr = std::unique_ptr<Synonyms>;

	Synonyms() = default;
	void GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
					 fast_hash_map<std::wstring, size_t>& patternsUsed) override final;
	void SetConfig(BaseFTConfig* cfg) override final;
	void AddManyToManySynonyms(const FtDSLQuery&, std::vector<SynonymsDsl>&, int proc) const;
	void AddOneToManySynonyms(const std::wstring&, const FtDslOpts&, const FtDSLQuery&, size_t termIdx, std::vector<SynonymsDsl>&,
							  int proc) const;

private:
	using SingleAlternativeCont = std::vector<std::wstring>;
	using MultipleAlternativesCont = std::vector<h_vector<std::wstring, 2>>;

	static void addDslEntries(std::vector<SynonymsDsl>&, const MultipleAlternativesCont&, const FtDslOpts&,
							  const std::vector<size_t>& termsIdx, const FtDSLQuery&);

	static void addPhraseAlternatives(const FtDSLQuery& dsl, const h_vector<std::wstring, 2>& phrase,
									  const MultipleAlternativesCont& phraseAlternatives, std::vector<SynonymsDsl>& synonymsDsl, int proc);

	// word - single-word synonyms
	RHashMap<std::wstring, std::shared_ptr<SingleAlternativeCont>> one2one_;
	// word - multi-word synonyms
	RHashMap<std::wstring, std::shared_ptr<MultipleAlternativesCont>> one2many_;
	// list of pairs {phrase (few words) - list of synonyms, both single-word and multi-word}
	std::vector<std::pair<h_vector<std::wstring, 2>, std::shared_ptr<MultipleAlternativesCont>>> many2any_;
};
}  // namespace reindexer
