#pragma once

#include <cmath>
#include "guttmansplitter.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MinEntries>
class QuadraticSplitter
	: public GuttmanSplitter<Entry, Node, Traits, Iterator, MinEntries, QuadraticSplitter<Entry, Node, Traits, Iterator, MinEntries>> {
public:
	using Base = GuttmanSplitter<Entry, Node, Traits, Iterator, MinEntries, QuadraticSplitter<Entry, Node, Traits, Iterator, MinEntries>>;

private:
	friend Base;

	void pickSeeds(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		auto& src = this->srcNode_.data_;

		auto worstAreaWaste = areaWaste(Base::getBoundRect(src[0]), Base::getBoundRect(this->appendingEntry_));
		size_t seed1 = 0, seed2 = this->srcSize_;
		for (size_t i = 1; i < this->srcSize_; ++i) {
			const auto currAreaWaste = areaWaste(Base::getBoundRect(src[i]), Base::getBoundRect(this->appendingEntry_));
			if (currAreaWaste > worstAreaWaste) {
				worstAreaWaste = currAreaWaste;
				seed1 = i;
			}
		}
		for (size_t i = 0; i < this->srcSize_ - 1; ++i) {
			for (size_t j = i + 1; j < this->srcSize_; ++j) {
				const auto currAreaWaste = areaWaste(Base::getBoundRect(src[i]), Base::getBoundRect(src[j]));
				if (currAreaWaste > worstAreaWaste) {
					worstAreaWaste = currAreaWaste;
					seed1 = i;
					seed2 = j;
				}
			}
		}

		firstNode->data_.emplace_back(std::move(src[seed1]));
		Base::setParent(firstNode->data_.back(), firstNode.get());
		firstNode->SetBoundRect(Base::getBoundRect(firstNode->data_.back()));
		this->moved_[seed1] = true;

		if (seed2 == this->srcSize_) {
			secondNode->data_.emplace_back(std::move(this->appendingEntry_));
			Base::setIterator(this->insertedIt_, *secondNode);
		} else {
			secondNode->data_.emplace_back(std::move(src[seed2]));
		}
		Base::setParent(secondNode->data_.back(), secondNode.get());
		secondNode->SetBoundRect(Base::getBoundRect(secondNode->data_.back()));
		this->moved_[seed2] = true;
	}

	void pickNext(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		auto& src = this->srcNode_.data_;

		size_t next = 0;
		while (next <= this->srcSize_ && this->moved_[next]) {
			++next;
		}
		assert(next <= this->srcSize_);
		double firstAreaIncrease{0.0}, secondAreaIncrease{0.0};
		for (size_t i = next; i <= this->srcSize_; ++i) {
			if (this->moved_[i]) continue;
			const auto rect = Base::getBoundRect((i == this->srcSize_) ? this->appendingEntry_ : src[i]);
			const auto currFirstAreaIncrease = Base::AreaIncrease(firstNode->BoundRect(), rect);
			const auto currSecondAreaIncrease = Base::AreaIncrease(secondNode->BoundRect(), rect);
			if (std::abs(firstAreaIncrease - secondAreaIncrease) < std::abs(currFirstAreaIncrease - currSecondAreaIncrease)) {
				next = i;
				firstAreaIncrease = currFirstAreaIncrease;
				secondAreaIncrease = currSecondAreaIncrease;
			}
		}

		if (firstAreaIncrease < secondAreaIncrease) {
			if (next == this->srcSize_) {
				firstNode->data_.emplace_back(std::move(this->appendingEntry_));
				Base::setIterator(this->insertedIt_, *firstNode);
			} else {
				firstNode->data_.emplace_back(std::move(src[next]));
			}
			firstNode->SetBoundRect(boundRect(firstNode->BoundRect(), Base::getBoundRect(firstNode->data_.back())));
			Base::setParent(firstNode->data_.back(), firstNode.get());
		} else {
			if (next == this->srcSize_) {
				secondNode->data_.emplace_back(std::move(this->appendingEntry_));
				Base::setIterator(this->insertedIt_, *secondNode);
			} else {
				secondNode->data_.emplace_back(std::move(src[next]));
			}
			secondNode->SetBoundRect(reindexer::boundRect(secondNode->BoundRect(), Base::getBoundRect(secondNode->data_.back())));
			Base::setParent(secondNode->data_.back(), secondNode.get());
		}
		this->moved_[next] = true;
	}

	static double areaWaste(const Rectangle& a, const Rectangle& b) noexcept {
		return reindexer::boundRect(a, b).Area() - a.Area() - b.Area();
	}

public:
	QuadraticSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}
};

}  // namespace reindexer
