#pragma once

#include <cmath>
#include "guttmansplitter.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MinEntries>
class LinearSplitter
	: public GuttmanSplitter<Entry, Node, Traits, Iterator, MinEntries, LinearSplitter<Entry, Node, Traits, Iterator, MinEntries>> {
public:
	using Base = GuttmanSplitter<Entry, Node, Traits, Iterator, MinEntries, LinearSplitter<Entry, Node, Traits, Iterator, MinEntries>>;

private:
	friend Base;

	void pickSeeds(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		auto& src = this->srcNode_.data_;
		const auto appendingRect = Base::getBoundRect(this->appendingEntry_);
		const auto wholeRect = reindexer::boundRect(this->srcNode_.BoundRect(), appendingRect);

		size_t minXIdx = this->srcSize_, maxXIdx = this->srcSize_, minYIdx = this->srcSize_, maxYIdx = this->srcSize_;
		double minX = appendingRect.Left(), maxX = appendingRect.Right(), minY = appendingRect.Bottom(), maxY = appendingRect.Top();
		for (size_t i = 0; i < this->srcSize_; ++i) {
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
			if (minXIdx == this->srcSize_) {
				newMinXIdx = newMaxXIdx = 0;
				const auto rect = Base::getBoundRect(src[0]);
				newMinX = rect.Left();
				newMaxX = rect.Right();
			} else {
				newMinXIdx = newMaxXIdx = this->srcSize_;
				newMinX = appendingRect.Left();
				newMaxX = appendingRect.Right();
			}
			for (size_t i = 0; i <= this->srcSize_; ++i) {
				if (i == minXIdx) continue;
				const auto currRect = ((i == this->srcSize_) ? appendingRect : Base::getBoundRect(src[i]));
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
			if (minYIdx == this->srcSize_) {
				newMinYIdx = newMaxYIdx = 0;
				const auto rect = Base::getBoundRect(src[0]);
				newMinY = rect.Bottom();
				newMaxY = rect.Top();
			} else {
				newMinYIdx = newMaxYIdx = this->srcSize_;
				newMinY = appendingRect.Bottom();
				newMaxY = appendingRect.Top();
			}
			for (size_t i = 0; i <= this->srcSize_; ++i) {
				if (i == minYIdx) continue;
				const auto currRect = ((i == this->srcSize_) ? appendingRect : Base::getBoundRect(src[i]));
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

		if (seed1 == this->srcSize_) {
			firstNode->data_.emplace_back(std::move(this->appendingEntry_));
			Base::setIterator(this->insertedIt_, *firstNode);
		} else {
			firstNode->data_.emplace_back(std::move(src[seed1]));
		}
		firstNode->SetBoundRect(Base::getBoundRect(firstNode->data_.back()));
		Base::setParent(firstNode->data_.back(), firstNode.get());
		this->moved_[seed1] = true;

		if (seed2 == this->srcSize_) {
			secondNode->data_.emplace_back(std::move(this->appendingEntry_));
			Base::setIterator(this->insertedIt_, *secondNode);
		} else {
			secondNode->data_.emplace_back(std::move(src[seed2]));
		}
		secondNode->SetBoundRect(Base::getBoundRect(secondNode->data_.back()));
		Base::setParent(secondNode->data_.back(), secondNode.get());
		this->moved_[seed2] = true;
	}

	void pickNext(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		auto& src = this->srcNode_.data_;

		while (next_ <= this->srcSize_ && this->moved_[next_]) {
			++next_;
		}
		assert(next_ <= this->srcSize_);
		const auto rect = Base::getBoundRect((next_ == this->srcSize_) ? this->appendingEntry_ : src[next_]);
		if (Base::AreaIncrease(firstNode->BoundRect(), rect) < Base::AreaIncrease(secondNode->BoundRect(), rect)) {
			if (next_ == this->srcSize_) {
				firstNode->data_.emplace_back(std::move(this->appendingEntry_));
				Base::setIterator(this->insertedIt_, *firstNode);
			} else {
				firstNode->data_.emplace_back(std::move(src[next_]));
			}
			firstNode->SetBoundRect(boundRect(firstNode->BoundRect(), Base::getBoundRect(firstNode->data_.back())));
			Base::setParent(firstNode->data_.back(), firstNode.get());
		} else {
			if (next_ == this->srcSize_) {
				secondNode->data_.emplace_back(std::move(this->appendingEntry_));
				Base::setIterator(this->insertedIt_, *secondNode);
			} else {
				secondNode->data_.emplace_back(std::move(src[next_]));
			}
			secondNode->SetBoundRect(reindexer::boundRect(secondNode->BoundRect(), Base::getBoundRect(secondNode->data_.back())));
			Base::setParent(secondNode->data_.back(), secondNode.get());
		}
		this->moved_[next_] = true;
		++next_;
	}

public:
	LinearSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}
	size_t next_ = 0;
};

}  // namespace reindexer
