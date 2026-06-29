#pragma once

#include "splitter.h"
#include "vendor/sort/pdqsort.hpp"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries, size_t MinEntries>
class [[nodiscard]] RStarSplitter : private Splitter<Entry, Node, Traits, Iterator, MaxEntries> {
	using Base = Splitter<Entry, Node, Traits, Iterator, MaxEntries>;

public:
	RStarSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}
	std::pair<std::unique_ptr<Node>, std::unique_ptr<Node>> Split() {
		static constexpr size_t distrCount = MaxEntries - 2 * MinEntries + 2;
		// Choose axis
		size_t sortedByX[MaxEntries + 1];
		size_t sortedByY[MaxEntries + 1];
		for (size_t i = 0; i <= MaxEntries; ++i) {
			sortedByX[i] = i;
			sortedByY[i] = i;
		}
		boost::sort::pdqsort(std::begin(sortedByX), std::end(sortedByX), [this](size_t lhs, size_t rhs) {
			const auto lbr = getBoundRect(lhs);
			const auto rbr = getBoundRect(rhs);
			return (approxEqual(lbr.Left(), rbr.Left()) && lbr.Right() < rbr.Right()) || lbr.Left() < rbr.Left();
		});
		boost::sort::pdqsort(std::begin(sortedByY), std::end(sortedByY), [this](size_t lhs, size_t rhs) {
			const auto lbr = getBoundRect(lhs);
			const auto rbr = getBoundRect(rhs);
			return (approxEqual(lbr.Bottom(), rbr.Bottom()) && lbr.Top() < rbr.Top()) || lbr.Bottom() < rbr.Bottom();
		});
		double marginSumByX = 0.0;
		double marginSumByY = 0.0;
		Rectangle rectsX[2][distrCount];
		Rectangle rectsY[2][distrCount];
		for (size_t k = 0; k < distrCount; ++k) {
			Rectangle brx = getBoundRect(sortedByX[0]);
			Rectangle bry = getBoundRect(sortedByY[0]);
			for (size_t i = 1; i < k + MinEntries; ++i) {
				brx = boundRect(brx, getBoundRect(sortedByX[i]));
				bry = boundRect(bry, getBoundRect(sortedByY[i]));
			}
			marginSumByX += (brx.Right() + brx.Top() - brx.Left() - brx.Bottom());
			marginSumByY += (bry.Right() + bry.Top() - bry.Left() - bry.Bottom());
			rectsX[0][k] = brx;
			rectsY[0][k] = bry;

			brx = getBoundRect(sortedByX[k + MinEntries]);
			bry = getBoundRect(sortedByY[k + MinEntries]);
			for (size_t i = k + MinEntries + 1; i <= MaxEntries; ++i) {
				brx = boundRect(brx, getBoundRect(sortedByX[i]));
				bry = boundRect(bry, getBoundRect(sortedByY[i]));
			}
			marginSumByX += (brx.Right() + brx.Top() - brx.Left() - brx.Bottom());
			marginSumByY += (bry.Right() + bry.Top() - bry.Left() - bry.Bottom());
			rectsX[1][k] = brx;
			rectsY[1][k] = bry;
		}

		if (marginSumByX < marginSumByY) {
			// Choose index
			double minOverlapArea = overlap(rectsX[0][0], rectsX[1][0]);
			size_t result = 0;
			for (size_t i = 1; i < distrCount; ++i) {
				const double currOverlapArea = overlap(rectsX[0][i], rectsX[1][i]);
				if ((approxEqual(currOverlapArea, minOverlapArea) &&
					 (rectsX[0][i].Area() + rectsX[1][i].Area()) < (rectsX[0][result].Area() + rectsX[1][result].Area())) ||
					currOverlapArea < minOverlapArea) {
					minOverlapArea = currOverlapArea;
					result = i;
				}
			}
			result += MinEntries;
			// Move values
			std::unique_ptr<Node> firstNode{new Node};
			std::unique_ptr<Node> secondNode{new Node};
			for (size_t i = 0; i < result; ++i) {
				this->moveEntryTo(*firstNode, sortedByX[i]);
			}
			for (size_t i = result; i <= MaxEntries; ++i) {
				this->moveEntryTo(*secondNode, sortedByX[i]);
			}
			return {std::move(firstNode), std::move(secondNode)};
		} else {
			// Choose index
			double minOverlapArea = overlap(rectsY[0][0], rectsY[1][0]);
			size_t result = 0;
			for (size_t i = 1; i < distrCount; ++i) {
				const double currOverlapArea = overlap(rectsY[0][i], rectsY[1][i]);
				if ((approxEqual(currOverlapArea, minOverlapArea) &&
					 (rectsY[0][i].Area() + rectsY[1][i].Area()) < (rectsY[0][result].Area() + rectsY[1][result].Area())) ||
					currOverlapArea < minOverlapArea) {
					minOverlapArea = currOverlapArea;
					result = i;
				}
			}
			result += MinEntries;
			// Move values
			std::unique_ptr<Node> firstNode{new Node};
			std::unique_ptr<Node> secondNode{new Node};
			for (size_t i = 0; i < result; ++i) {
				this->moveEntryTo(*firstNode, sortedByY[i]);
			}
			for (size_t i = result; i <= MaxEntries; ++i) {
				this->moveEntryTo(*secondNode, sortedByY[i]);
			}
			return {std::move(firstNode), std::move(secondNode)};
		}
	}

	static size_t ChooseSubtree(const Rectangle& insertingRect, const decltype(std::declval<Node>().data_)& data,
								bool splitOfChildAvailable) {
		bool haveLeaf = false;
		for (const auto& n : data) {
			if (n->IsLeaf()) {
				haveLeaf = true;
				break;
			}
		}
		if (haveLeaf) {
			return Base::chooseSubtreeByMinAreaIncrease(insertingRect, data, splitOfChildAvailable);
		}
		size_t i = 0;
		if (!splitOfChildAvailable) {
			while (i < data.size() && data[i]->IsFull()) {
				++i;
			}
		}
		assertrx(i < data.size());
		auto minOverlapIncrease = overlap(boundRect(data[i]->BoundRect(), insertingRect), i, data) - overlap(data[i]->BoundRect(), i, data);
		size_t result = i;
		for (++i; i < data.size(); ++i) {
			const auto currOverlapIncrease =
				overlap(boundRect(data[i]->BoundRect(), insertingRect), i, data) - overlap(data[i]->BoundRect(), i, data);
			if (approxEqual(minOverlapIncrease, currOverlapIncrease)) {
				const auto lastAreaIncrease = data[result]->AreaIncrease(insertingRect);
				const auto currAreaIncrease = data[i]->AreaIncrease(insertingRect);
				if ((approxEqual(currAreaIncrease, lastAreaIncrease) && data[i]->BoundRect().Area() < data[result]->BoundRect().Area()) ||
					currAreaIncrease < lastAreaIncrease) {
					result = i;
					minOverlapIncrease = currOverlapIncrease;
				}
			} else if (currOverlapIncrease < minOverlapIncrease) {
				result = i;
				minOverlapIncrease = currOverlapIncrease;
			}
		}
		return result;
	}
	static size_t ChooseNode(const Node& dst, const decltype(std::declval<Node>().data_)& data, size_t except) {
		bool haveLeaf = false;
		for (const auto& n : data) {
			if (n->IsLeaf()) {
				haveLeaf = true;
				break;
			}
		}
		if (haveLeaf) {
			return Base::chooseNodeByMinAreaIncrease(dst, data, except);
		}
		size_t result = 0;
		if (except == 0) {
			result = 1;
		}
		assertrx(result < data.size());
		const Rectangle& dstBoundRect = dst.BoundRect();
		auto minOverlapIncrease =
			overlap(boundRect(data[result]->BoundRect(), dstBoundRect), result, except, data) - overlap(dstBoundRect, result, except, data);
		for (size_t i = result + 1; i < data.size(); ++i) {
			if (i == except) {
				continue;
			}
			const auto currOverlapIncrease =
				overlap(boundRect(data[i]->BoundRect(), dstBoundRect), i, except, data) - overlap(dstBoundRect, i, except, data);
			if (approxEqual(minOverlapIncrease, currOverlapIncrease)) {
				const auto lastAreaIncrease = dst.AreaIncrease(data[result]->BoundRect());
				const auto currAreaIncrease = dst.AreaIncrease(data[i]->BoundRect());
				if ((approxEqual(currAreaIncrease, lastAreaIncrease) && data[i]->BoundRect().Area() < data[result]->BoundRect().Area()) ||
					currAreaIncrease < lastAreaIncrease) {
					result = i;
					minOverlapIncrease = currOverlapIncrease;
				}
			} else if (currOverlapIncrease < minOverlapIncrease) {
				result = i;
				minOverlapIncrease = currOverlapIncrease;
			}
		}
		return result;
	}

	using Base::AreaIncrease;

private:
	inline const Rectangle getBoundRect(size_t idx) const {
		return Base::getBoundRect(idx < MaxEntries ? this->srcNode_.data_[idx] : this->appendingEntry_);
	}
	static double overlap(const Rectangle& rect, size_t index, const decltype(std::declval<Node>().data_)& data) noexcept {
		assertrx(index < data.size());
		double result = 0.0;
		size_t i = 0;
		for (; i < index; ++i) {
			result += overlap(rect, data[i]->BoundRect());
		}
		for (++i; i < data.size(); ++i) {
			result += overlap(rect, data[i]->BoundRect());
		}
		return result;
	}
	static double overlap(const Rectangle& rect, size_t index1, size_t index2, const decltype(std::declval<Node>().data_)& data) noexcept {
		if (index1 > index2) {
			std::swap(index1, index2);
		}
		assertrx(index2 < data.size());
		double result = 0.0;
		size_t i = 0;
		for (; i < index1; ++i) {
			result += overlap(rect, data[i]->BoundRect());
		}
		for (++i; i < index2; ++i) {
			result += overlap(rect, data[i]->BoundRect());
		}
		for (++i; i < data.size(); ++i) {
			result += overlap(rect, data[i]->BoundRect());
		}
		return result;
	}
	static double overlap(const Rectangle& r1, const Rectangle& r2) noexcept {
		const auto left = std::max(r1.Left(), r2.Left());
		const auto right = std::min(r1.Right(), r2.Right());
		if (left >= right) {
			return 0.0;
		}
		const auto bottom = std::max(r1.Bottom(), r2.Bottom());
		const auto top = std::min(r1.Top(), r2.Top());
		if (bottom >= top) {
			return 0.0;
		}
		return (right - left) * (top - bottom);
	}
};

}  // namespace reindexer
