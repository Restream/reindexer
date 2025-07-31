#include "compositewordssplitter.h"

namespace reindexer {

void AddOrUpdateVariant(ITokenFilter::ResultsStorage& variants, fast_hash_map<std::wstring, size_t>& patternsUsed, FtDSLVariant variant) {
	auto [it, emplaced] = patternsUsed.try_emplace(variant.pattern, variants.size());
	if (emplaced) {
		variants.emplace_back(std::move(variant));
		return;
	}

	// ToDo change this logic after #2142
	if (variants[it->second].proc < variant.proc || (variants[it->second].prefAndStemmersForbidden && !variant.prefAndStemmersForbidden)) {
		variants[it->second] = std::move(variant);
	}
}

void CompositeWordsSplitter::GetVariants(const std::wstring& data, ITokenFilter::ResultsStorage& result, int proc,
										 fast_hash_map<std::wstring, size_t>& patternsUsed) {
	if (!opts_.HasDelims() || !opts_.ContainsDelims(data)) {
		return;
	}

	std::wstring dataWithoutDelims;
	size_t numPartsFound = 0;

	std::wstring nextPart;
	for (auto wc : data) {
		if (!opts_.IsWordPartDelimiter(wc)) {
			dataWithoutDelims.push_back(wc);
			nextPart.push_back(wc);
			continue;
		}

		if (!nextPart.empty()) {
			numPartsFound++;
			if (nextPart.size() >= opts_.GetMinPartSize()) {
				FtDSLVariant variant{std::move(nextPart), proc, PrefAndStemmersForbidden_True};
				AddOrUpdateVariant(result, patternsUsed, std::move(variant));
			}

			nextPart.clear();
		}
	}

	if (!nextPart.empty()) {
		numPartsFound++;
		if (nextPart.size() >= opts_.GetMinPartSize()) {
			FtDSLVariant variant{std::move(nextPart), proc, PrefAndStemmersForbidden_False};
			AddOrUpdateVariant(result, patternsUsed, std::move(variant));
		}
	}

	// for word with delimiters e.g. user-friendly we should also search for user friendly #1863
	if (numPartsFound > 1) {
		FtDSLVariant variant{std::move(dataWithoutDelims), proc, PrefAndStemmersForbidden_False};
		AddOrUpdateVariant(result, patternsUsed, std::move(variant));
	}
}

}  // namespace reindexer
