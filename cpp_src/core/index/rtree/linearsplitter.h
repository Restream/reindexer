#pragma once

#include <cmath>
#include "guttmansplitter.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries, size_t MinEntries>
class LinearSplitter : public GuttmanSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries,
											  LinearSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries>> {
	using Base = GuttmanSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries,
								 LinearSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries>>;

	friend Base;

	void pickSeeds(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		auto& src = this->srcNode_.data_;
		const auto appendingRect = Base::getBoundRect(this->appendingEntry_);
		const auto wholeRect = reindexer::boundRect(this->srcNode_.BoundRect(), appendingRect);

		size_t minXIdx = MaxEntries, maxXIdx = MaxEntries, minYIdx = MaxEntries, maxYIdx = MaxEntries;
		double minX = appendingRect.Left(), maxX = appendingRect.Right(), minY = appendingRect.Bottom(), maxY = appendingRect.Top();
		for (size_t i = 0; i < MaxEntries; ++i) {
			const auto currRect = Base::getBoundRect(src[i]);
			if (currRect.Left() > minX) {
				minX = currRect.Left();
				minXIdx = i;
			}
			if (currRect.Right() < maxX) {
				maxX = currRect.Right();
				maxXIdx = i;
			}
			if (currRect.Bottom() > minY) {
				minY = currRect.Bottom();
				minYIdx = i;
			}
			if (currRect.Top() < maxY) {
				maxY = currRect.Top();
				maxYIdx = i;
			}
		}
		if (minXIdx == maxXIdx) {
			double newMinX, newMaxX;
			size_t newMinXIdx, newMaxXIdx;
			if (minXIdx == MaxEntries) {
				newMinXIdx = newMaxXIdx = 0;
				const auto rect = Base::getBoundRect(src[0]);
				newMinX = rect.Left();
				newMaxX = rect.Right();
			} else {
				newMinXIdx = newMaxXIdx = MaxEntries;
				newMinX = appendingRect.Left();
				newMaxX = appendingRect.Right();
			}
			for (size_t i = 0; i <= MaxEntries; ++i) {
				if (i == minXIdx) {
					continue;
				}
				const auto currRect = ((i == MaxEntries) ? appendingRect : Base::getBoundRect(src[i]));
				if (currRect.Left() > newMinX) {
					newMinX = currRect.Left();
					newMinXIdx = i;
				}
				if (currRect.Right() < newMaxX) {
					newMaxX = currRect.Right();
					newMaxXIdx = i;
				}
			}
			if (minX - newMinX < newMaxX - maxX) {
				minX = newMinX;
				minXIdx = newMinXIdx;
			} else {
				maxX = newMaxX;
				maxXIdx = newMaxXIdx;
			}
		}
		if (minYIdx == maxYIdx) {
			double newMinY, newMaxY;
			size_t newMinYIdx, newMaxYIdx;
			if (minYIdx == MaxEntries) {
				newMinYIdx = newMaxYIdx = 0;
				const auto rect = Base::getBoundRect(src[0]);
				newMinY = rect.Bottom();
				newMaxY = rect.Top();
			} else {
				newMinYIdx = newMaxYIdx = MaxEntries;
				newMinY = appendingRect.Bottom();
				newMaxY = appendingRect.Top();
			}
			for (size_t i = 0; i <= MaxEntries; ++i) {
				if (i == minYIdx) {
					continue;
				}
				const auto currRect = ((i == MaxEntries) ? appendingRect : Base::getBoundRect(src[i]));
				if (currRect.Bottom() > newMinY) {
					newMinY = currRect.Bottom();
					newMinYIdx = i;
				}
				if (currRect.Top() < newMaxY) {
					newMaxY = currRect.Top();
					newMaxYIdx = i;
				}
			}
			if (minY - newMinY < newMaxY - maxY) {
				minY = newMinY;
				minYIdx = newMinYIdx;
			} else {
				maxY = newMaxY;
				maxYIdx = newMaxYIdx;
			}
		}

		const auto xDiff = std::abs((maxX - minX) / (wholeRect.Right() - wholeRect.Left()));
		const auto yDiff = std::abs((maxY - minY) / (wholeRect.Top() - wholeRect.Bottom()));

		const size_t seed1 = (xDiff > yDiff) ? minXIdx : minYIdx;
		const size_t seed2 = (xDiff > yDiff) ? maxXIdx : maxYIdx;

		this->moveEntryTo(*firstNode, seed1);
		this->moved_[seed1] = true;
		this->moveEntryTo(*secondNode, seed2);
		this->moved_[seed2] = true;
	}

	void pickNext(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		while (next_ <= MaxEntries && this->moved_[next_]) {
			++next_;
		}
		assertrx(next_ <= MaxEntries);
		const auto rect = Base::getBoundRect((next_ == MaxEntries) ? this->appendingEntry_ : this->srcNode_.data_[next_]);
		const auto firstAreaIncrease = Base::AreaIncrease(firstNode->BoundRect(), rect);
		const auto secondAreaIncrease = Base::AreaIncrease(secondNode->BoundRect(), rect);
		if ((approxEqual(firstAreaIncrease, secondAreaIncrease) && firstNode->BoundRect().Area() < secondNode->BoundRect().Area()) ||
			firstAreaIncrease < secondAreaIncrease) {
			this->moveEntryTo(*firstNode, next_);
		} else {
			this->moveEntryTo(*secondNode, next_);
		}
		this->moved_[next_] = true;
		++next_;
	}

public:
	LinearSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}

private:
	size_t next_ = 0;
};

}  // namespace reindexer
