#pragma once

#include <cmath>
#include "guttmansplitter.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries, size_t MinEntries>
class QuadraticSplitter : public GuttmanSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries,
												 QuadraticSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries>> {
	using Base = GuttmanSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries,
								 QuadraticSplitter<Entry, Node, Traits, Iterator, MaxEntries, MinEntries>>;
	friend Base;

	void pickSeeds(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		const auto seeds = this->quadraticChooseSeeds();
		this->moveEntryTo(*firstNode, seeds.first);
		this->moved_[seeds.first] = true;
		this->moveEntryTo(*secondNode, seeds.second);
		this->moved_[seeds.second] = true;
	}

	void pickNext(const std::unique_ptr<Node>& firstNode, const std::unique_ptr<Node>& secondNode) {
		size_t next = 0;
		while (next <= MaxEntries && this->moved_[next]) {
			++next;
		}
		assertrx(next <= MaxEntries);
		double firstAreaIncrease{0.0}, secondAreaIncrease{0.0};
		for (size_t i = next; i <= MaxEntries; ++i) {
			if (this->moved_[i]) {
				continue;
			}
			const auto rect = Base::getBoundRect((i == MaxEntries) ? this->appendingEntry_ : this->srcNode_.data_[i]);
			const auto currFirstAreaIncrease = Base::AreaIncrease(firstNode->BoundRect(), rect);
			const auto currSecondAreaIncrease = Base::AreaIncrease(secondNode->BoundRect(), rect);
			if (std::abs(firstAreaIncrease - secondAreaIncrease) < std::abs(currFirstAreaIncrease - currSecondAreaIncrease)) {
				next = i;
				firstAreaIncrease = currFirstAreaIncrease;
				secondAreaIncrease = currSecondAreaIncrease;
			}
		}

		if ((approxEqual(firstAreaIncrease, secondAreaIncrease) && firstNode->BoundRect().Area() < secondNode->BoundRect().Area()) ||
			firstAreaIncrease < secondAreaIncrease) {
			this->moveEntryTo(*firstNode, next);
		} else {
			this->moveEntryTo(*secondNode, next);
		}
		this->moved_[next] = true;
	}

public:
	QuadraticSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}
};

}  // namespace reindexer
