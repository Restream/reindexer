#pragma once

#include "splitter.h"
#include "vendor/sort/pdqsort.hpp"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries, size_t MinEntries>
class [[nodiscard]] GreeneSplitter : private Splitter<Entry, Node, Traits, Iterator, MaxEntries> {
	using Base = Splitter<Entry, Node, Traits, Iterator, MaxEntries>;

public:
	GreeneSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) : Base{std::move(appendingEntry), sourceNode, it} {}
	std::pair<std::unique_ptr<Node>, std::unique_ptr<Node>> Split() {
		const auto newBoundRect = reindexer::boundRect(this->srcNode_.BoundRect(), Base::getBoundRect(this->appendingEntry_));
		if (approxEqual(newBoundRect.Left(), newBoundRect.Right()) && approxEqual(newBoundRect.Bottom(), newBoundRect.Top())) {
			std::unique_ptr<Node> firstNode{new Node};
			std::unique_ptr<Node> secondNode{new Node};
			size_t i = 0;
			for (; i <= MaxEntries / 2; ++i) {
				this->moveEntryTo(*firstNode, i);
			}
			for (; i <= MaxEntries; ++i) {
				this->moveEntryTo(*secondNode, i);
			}
			return {std::move(firstNode), std::move(secondNode)};
		} else {
			auto& src = this->srcNode_.data_;
			auto seeds = this->quadraticChooseSeeds();
			assertrx(seeds.first < seeds.second);
			size_t indexes[MaxEntries - 1];
			size_t i = 0;
			for (; i < seeds.first; ++i) {
				indexes[i] = i;
			}
			for (++i; i < seeds.second; ++i) {
				indexes[i - 1] = i;
			}
			for (++i; i <= MaxEntries; ++i) {
				indexes[i - 2] = i;
			}
			// Choose axis
			const auto firstSeedBoundRect = Base::getBoundRect(src[seeds.first]);
			const auto secondSeedBoundRect = Base::getBoundRect(seeds.second == MaxEntries ? this->appendingEntry_ : src[seeds.second]);
			const auto separationX =
				approxEqual(newBoundRect.Left(), newBoundRect.Right())
					? 0.0
					: (((firstSeedBoundRect.Left() + firstSeedBoundRect.Right() < secondSeedBoundRect.Left() + secondSeedBoundRect.Right())
							? (secondSeedBoundRect.Left() - firstSeedBoundRect.Right())
							: (firstSeedBoundRect.Left() - secondSeedBoundRect.Right())) /
					   (newBoundRect.Right() - newBoundRect.Left()));
			const auto separationY =
				approxEqual(newBoundRect.Bottom(), newBoundRect.Top())
					? 0.0
					: (((firstSeedBoundRect.Bottom() + firstSeedBoundRect.Top() < secondSeedBoundRect.Bottom() + secondSeedBoundRect.Top())
							? (secondSeedBoundRect.Bottom() - firstSeedBoundRect.Top())
							: (firstSeedBoundRect.Bottom() - secondSeedBoundRect.Top())) /
					   (newBoundRect.Top() - newBoundRect.Bottom()));
			if (separationX > separationY) {
				if (firstSeedBoundRect.Left() + firstSeedBoundRect.Right() > secondSeedBoundRect.Left() + secondSeedBoundRect.Right()) {
					std::swap(seeds.first, seeds.second);
				}
				boost::sort::pdqsort(std::begin(indexes), std::end(indexes), [&src, this](size_t lhs, size_t rhs) {
					return Base::getBoundRect(lhs < MaxEntries ? src[lhs] : this->appendingEntry_).Left() <
						   Base::getBoundRect(rhs < MaxEntries ? src[rhs] : this->appendingEntry_).Left();
				});
			} else {
				if (firstSeedBoundRect.Bottom() + firstSeedBoundRect.Top() > secondSeedBoundRect.Bottom() + secondSeedBoundRect.Top()) {
					std::swap(seeds.first, seeds.second);
				}
				boost::sort::pdqsort(std::begin(indexes), std::end(indexes), [&src, this](size_t lhs, size_t rhs) {
					return Base::getBoundRect(lhs < MaxEntries ? src[lhs] : this->appendingEntry_).Bottom() <
						   Base::getBoundRect(rhs < MaxEntries ? src[rhs] : this->appendingEntry_).Bottom();
				});
			}
			// move values
			std::unique_ptr<Node> firstNode{new Node};
			std::unique_ptr<Node> secondNode{new Node};
			this->moveEntryTo(*firstNode, seeds.first);
			this->moveEntryTo(*secondNode, seeds.second);
			i = 0;
			for (; i < (MaxEntries - 1) / 2; ++i) {
				this->moveEntryTo(*firstNode, indexes[i]);
			}
			if (MaxEntries % 2 == 0) {
				++i;
			}
			for (; i < MaxEntries - 1; ++i) {
				this->moveEntryTo(*secondNode, indexes[i]);
			}
			if (MaxEntries % 2 == 0) {
				i = (MaxEntries - 1) / 2;
				const auto rect = Base::getBoundRect((indexes[i] == MaxEntries) ? this->appendingEntry_ : src[indexes[i]]);
				const auto firstAreaIncrease = Base::AreaIncrease(firstNode->BoundRect(), rect);
				const auto secondAreaIncrease = Base::AreaIncrease(secondNode->BoundRect(), rect);
				if ((approxEqual(firstAreaIncrease, secondAreaIncrease) &&
					 firstNode->BoundRect().Area() < secondNode->BoundRect().Area()) ||
					firstAreaIncrease < secondAreaIncrease) {
					this->moveEntryTo(*firstNode, indexes[i]);
				} else {
					this->moveEntryTo(*secondNode, indexes[i]);
				}
			}
			return {std::move(firstNode), std::move(secondNode)};
		}
	}

	static size_t ChooseSubtree(const Rectangle& insertingRect, const decltype(std::declval<Node>().data_)& data,
								bool splitOfChildAvailable) {
		return Base::chooseSubtreeByMinAreaIncrease(insertingRect, data, splitOfChildAvailable);
	}
	static size_t ChooseNode(const Node& dst, const decltype(std::declval<Node>().data_)& data, size_t except) {
		return Base::chooseNodeByMinAreaIncrease(dst, data, except);
	}

	using Base::AreaIncrease;
};

}  // namespace reindexer
