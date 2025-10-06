#pragma once

#include "splitter.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries, size_t MinEntries, typename Child>
class [[nodiscard]] GuttmanSplitter : protected Splitter<Entry, Node, Traits, Iterator, MaxEntries> {
	using Base = Splitter<Entry, Node, Traits, Iterator, MaxEntries>;

protected:
	GuttmanSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it)
		: Base{std::move(appendingEntry), sourceNode, it}, moved_(MaxEntries + 1, false) {}
	~GuttmanSplitter() = default;

public:
	using Base::AreaIncrease;

	std::pair<std::unique_ptr<Node>, std::unique_ptr<Node>> Split() {
		std::unique_ptr<Node> firstNode{new Node};
		std::unique_ptr<Node> secondNode{new Node};

		static_cast<Child*>(this)->pickSeeds(firstNode, secondNode);

		auto& first = firstNode->data_;
		auto& second = secondNode->data_;
		for (size_t leftEntries = MaxEntries - 1; leftEntries > 0;) {
			static_cast<Child*>(this)->pickNext(firstNode, secondNode);
			--leftEntries;

			// Just move entries if left few
			if (first.size() + leftEntries == MinEntries) {
				for (size_t i = 0; i <= MaxEntries; ++i) {
					if (!moved_[i]) {
						this->moveEntryTo(*firstNode, i);
					}
				}
				assertrx(first.size() == MinEntries);
				assertrx(second.size() > MinEntries);
				break;
			} else if (second.size() + leftEntries == MinEntries) {
				for (size_t i = 0; i <= MaxEntries; ++i) {
					if (!moved_[i]) {
						this->moveEntryTo(*secondNode, i);
					}
				}
				assertrx(second.size() == MinEntries);
				assertrx(first.size() > MinEntries);
				break;
			}
		}
		assertrx(first.size() + second.size() == MaxEntries + 1);
		return {std::move(firstNode), std::move(secondNode)};
	}

	static size_t ChooseSubtree(const Rectangle& insertingRect, const decltype(std::declval<Node>().data_)& data,
								bool splitOfChildAvailable) {
		return Base::chooseSubtreeByMinAreaIncrease(insertingRect, data, splitOfChildAvailable);
	}
	static size_t ChooseNode(const Node& dst, const decltype(std::declval<Node>().data_)& data, size_t except) {
		return Base::chooseNodeByMinAreaIncrease(dst, data, except);
	}

protected:
	std::vector<bool> moved_;
};

}  // namespace reindexer
