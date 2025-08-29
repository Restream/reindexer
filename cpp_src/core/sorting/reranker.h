#pragma once

#include <variant>
#include "core/enums.h"
#include "core/rank_t.h"
#include "estl/concepts.h"

namespace reindexer {

// kKnn * rankKnn + kFt * rankFt + c
class [[nodiscard]] RerankerLinear final {
public:
	RerankerLinear(double kKnn, double knnDefault, double kFt, double ftDefault, double c) noexcept
		: kKnn_{kKnn}, knnDefault_{knnDefault}, kFt_{kFt}, ftDefault_{ftDefault}, c_{c} {}

	RankT Calculate(double rankKnn, RankT rankFt) const noexcept { return RankT(kKnn_ * rankKnn + kFt_ * rankFt.Value() + c_); }
	RankT CalculateJustKnn(double rankKnn) const noexcept { return RankT(kKnn_ * rankKnn + kFt_ * ftDefault_ + c_); }
	RankT CalculateJustFt(RankT rankFt) const noexcept { return RankT(kKnn_ * knnDefault_ + kFt_ * rankFt.Value() + c_); }

private:
	const double kKnn_;
	const double knnDefault_;
	const double kFt_;
	const double ftDefault_;
	const double c_;
};

class [[nodiscard]] RerankerRRF final {
public:
	explicit RerankerRRF(double rankConst) noexcept : rankConst_{rankConst} {}

	RankT Calculate(size_t posKnn, size_t posFt) const noexcept { return RankT(1.0 / (rankConst_ + posKnn) + 1.0 / (rankConst_ + posFt)); }
	RankT CalculateSingle(size_t pos) const noexcept { return RankT(1.0 / (rankConst_ + pos)); }

private:
	const double rankConst_;
};

class [[nodiscard]] Reranker : private std::variant<RerankerLinear, RerankerRRF> {
	using Base = std::variant<RerankerLinear, RerankerRRF>;

public:
	template <concepts::OneOf<Base, RerankerLinear, RerankerRRF> R>
	Reranker(R&& rr, reindexer::Desc desc) noexcept : Base{std::forward<R>(rr)}, desc_{desc} {}

	const Base& AsVariant() const& noexcept { return *this; }
	auto AsVariant() const&& = delete;
	reindexer::Desc Desc() const noexcept { return desc_; }

	static Reranker Default() noexcept;
	bool IsRRF() const noexcept { return std::holds_alternative<RerankerRRF>(AsVariant()); }

private:
	const reindexer::Desc desc_{Desc_True};
};

}  // namespace reindexer
