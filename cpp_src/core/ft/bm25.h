#pragma once

#include <math.h>
#include <algorithm>

namespace reindexer {
const static double kKeofBm25k1 = 2.0;
const static double kKeofBm25b = 0.75;

inline double IDF(double totalDocCount, double matchedDocCount) {
	double f = log((totalDocCount - matchedDocCount + 1) / matchedDocCount) / log(1 + totalDocCount);
	// saturate min to 0.2
	if (f < 0.2) f = 0.2;
	return f;
}

inline double TF(double termCountInDoc, double mostFreqWordCountInDoc, double wordsInDoc) {
	(void)mostFreqWordCountInDoc;
	(void)wordsInDoc;
	return termCountInDoc;
}

inline double bm25score(double termCountInDoc, double mostFreqWordCountInDoc, double wordsInDoc, double avgDocLen) {
	auto termFreq = TF(termCountInDoc, mostFreqWordCountInDoc, wordsInDoc);
	return termFreq * (kKeofBm25k1 + 1.0) / (termFreq + kKeofBm25k1 * (1.0 - kKeofBm25b + kKeofBm25b * wordsInDoc / avgDocLen));
}
}  // namespace reindexer
