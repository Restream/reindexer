#pragma once

#include <math.h>

namespace reindexer {

class [[nodiscard]] Bm25Rx {
public:
	Bm25Rx(double totalDocCount, double matchedDocCount, double k1, double b) noexcept
		: k1_(k1), b_(b), idf_(IDF(totalDocCount, matchedDocCount)) {}

	RX_ALWAYS_INLINE double Get(double termCountInDoc, double wordsInDoc, double avgDocLen) const noexcept {
		auto termFreq = TF(termCountInDoc, wordsInDoc);
		return idf_ * termFreq * (k1_ + 1.0) / (termFreq + k1_ * (1.0 - b_ + b_ * wordsInDoc / avgDocLen));
	}
	RX_ALWAYS_INLINE double GetIDF() const noexcept { return idf_; }

private:
	static RX_ALWAYS_INLINE double IDF(double totalDocCount, double matchedDocCount) noexcept {
		double f = log((totalDocCount - matchedDocCount + 1) / matchedDocCount) / log(1 + totalDocCount);
		// saturate min to 0.2
		if (f < 0.2) {
			f = 0.2;
		}
		return f;
	}
	static RX_ALWAYS_INLINE double TF(double termCountInDoc, double wordsInDoc) noexcept {
		(void)wordsInDoc;
		return termCountInDoc;
	}

	const double k1_;
	const double b_;
	const double idf_;
};

class [[nodiscard]] Bm25Classic {
public:
	Bm25Classic(double totalDocCount, double matchedDocCount, double k1, double b) noexcept
		: k1_(k1), b_(b), idf_(IDF(totalDocCount, matchedDocCount)) {}

	RX_ALWAYS_INLINE double Get(double termCountInDoc, double wordsInDoc, double avgDocLen) const {
		auto termFreq = TF(termCountInDoc, wordsInDoc);
		return idf_ * termFreq * (k1_ + 1.0) / (termFreq + k1_ * (1.0 - b_ + b_ * wordsInDoc / avgDocLen));
	}
	RX_ALWAYS_INLINE double GetIDF() const noexcept { return idf_; }

private:
	static RX_ALWAYS_INLINE double IDF(double totalDocCount, double matchedDocCount) noexcept {
		return log(totalDocCount / (matchedDocCount + 1)) + 1;
	}
	static RX_ALWAYS_INLINE double TF(double termCountInDoc, double wordsInDoc) noexcept { return termCountInDoc / wordsInDoc; }

	const double k1_;
	const double b_;
	const double idf_;
};

class [[nodiscard]] TermCount {
public:
	TermCount(double /*totalDocCount*/, double /*matchedDocCount*/, double /*k1*/, double /*b*/) noexcept {}

	RX_ALWAYS_INLINE double Get(double termCountInDoc, double /*wordsInDoc*/, double /*avgDocLen*/) const noexcept {
		return termCountInDoc;
	}
	RX_ALWAYS_INLINE double GetIDF() const noexcept { return 0.0; }
};

template <typename BM>
class [[nodiscard]] Bm25Calculator {
public:
	Bm25Calculator(double totalDocCount, double matchedDocCount, double k1, double b) : bm_(totalDocCount, matchedDocCount, k1, b) {}
	RX_ALWAYS_INLINE double Get(double termCountInDoc, double wordsInDoc, double avgDocLen) const {
		return bm_.Get(termCountInDoc, wordsInDoc, avgDocLen);
	}
	RX_ALWAYS_INLINE double GetIDF() const noexcept { return bm_.GetIDF(); }

private:
	const BM bm_;
};

}  // namespace reindexer
