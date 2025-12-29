#pragma once

#include "itokenfilter.h"

namespace reindexer {

struct FtDslOpts;

struct [[nodiscard]] MultiWord {
	h_vector<std::wstring, 2> words;
	FtDslOpts opts;
	h_vector<size_t, 2> termsIdx;
};

class [[nodiscard]] Synonyms final : public ITokenFilter {
public:
	using Ptr = std::unique_ptr<Synonyms>;

	Synonyms() = default;
	void GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
					 fast_hash_map<std::wstring, size_t>& patternsUsed) override final;
	void SetConfig(BaseFTConfig* cfg) override final;
	void AddManyToManySynonyms(const FtDSLQuery& query, int proc, std::vector<MultiWord>& synonyms) const;
	void AddOneToManySynonyms(const std::wstring&, const FtDslOpts&, size_t termIdx, int proc, std::vector<MultiWord>&) const;

private:
	using SingleAlternativeCont = std::vector<std::wstring>;
	using MultipleAlternativesCont = std::vector<h_vector<std::wstring, 2>>;

	static void addPhraseAlternatives(const FtDSLQuery& query, const h_vector<std::wstring, 2>& phrase,
									  const MultipleAlternativesCont& phraseAlternatives, int proc, std::vector<MultiWord>& synonyms);

	// word - single-word synonyms
	RHashMap<std::wstring, std::shared_ptr<SingleAlternativeCont>> one2one_;
	// word - multi-word synonyms
	RHashMap<std::wstring, std::shared_ptr<MultipleAlternativesCont>> one2many_;
	// list of pairs {phrase (few words) - list of synonyms, both single-word and multi-word}
	std::vector<std::pair<h_vector<std::wstring, 2>, std::shared_ptr<MultipleAlternativesCont>>> many2any_;
};
}  // namespace reindexer
