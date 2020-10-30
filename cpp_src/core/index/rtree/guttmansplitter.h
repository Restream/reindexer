#pragma once

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MinEntries, typename Child>
class GuttmanSplitter {
public:
	GuttmanSplitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it)
		: appendingEntry_{std::move(appendingEntry)},
		  srcNode_{sourceNode},
		  srcSize_{srcNode_.data_.size()},
		  moved_(srcSize_ + 1, false),
		  insertedIt_{it} {}
	std::pair<std::unique_ptr<Node>, std::unique_ptr<Node>> Split() {
		auto& src = srcNode_.data_;

		std::unique_ptr<Node> firstNode{new Node};
		std::unique_ptr<Node> secondNode{new Node};

		static_cast<Child*>(this)->pickSeeds(firstNode, secondNode);

		auto& first = firstNode->data_;
		auto& second = secondNode->data_;
		size_t leftEntries = srcSize_ - 1;
		while (leftEntries) {
			static_cast<Child*>(this)->pickNext(firstNode, secondNode);
			--leftEntries;

			// Just move entries if left few
			if (first.size() + leftEntries == MinEntries) {
				for (size_t i = 0; i < srcSize_; ++i) {
					if (moved_[i]) continue;
					first.emplace_back(std::move(src[i]));
					firstNode->SetBoundRect(reindexer::boundRect(firstNode->BoundRect(), getBoundRect(first.back())));
					setParent(first.back(), firstNode.get());
				}
				if (!moved_[srcSize_]) {
					first.emplace_back(std::move(appendingEntry_));
					setIterator(insertedIt_, *firstNode);
					firstNode->SetBoundRect(reindexer::boundRect(firstNode->BoundRect(), getBoundRect(first.back())));
					setParent(first.back(), firstNode.get());
				}
				assert(first.size() == MinEntries);
				assert(second.size() > MinEntries);
				break;
			} else if (second.size() + leftEntries == MinEntries) {
				for (size_t i = 0; i < srcSize_; ++i) {
					if (moved_[i]) continue;
					second.emplace_back(std::move(src[i]));
					secondNode->SetBoundRect(reindexer::boundRect(secondNode->BoundRect(), getBoundRect(second.back())));
					setParent(second.back(), secondNode.get());
				}
				if (!moved_[srcSize_]) {
					second.emplace_back(std::move(appendingEntry_));
					setIterator(insertedIt_, *secondNode);
					secondNode->SetBoundRect(reindexer::boundRect(secondNode->BoundRect(), getBoundRect(second.back())));
					setParent(second.back(), secondNode.get());
				}
				assert(second.size() == MinEntries);
				assert(first.size() > MinEntries);
				break;
			}
		}
		assert(first.size() + second.size() == srcSize_ + 1);
		std::pair<std::unique_ptr<Node>, std::unique_ptr<Node>> result;
		result.first = std::move(firstNode);
		result.second = std::move(secondNode);
		return result;
	}

	static double AreaIncrease(const Rectangle& lhs, const Rectangle& rhs) noexcept {
		return reindexer::boundRect(lhs, rhs).Area() - lhs.Area();
	}

protected:
	template <typename N>
	static Rectangle getBoundRect(const N& n) noexcept {
		return boundRect(Traits::GetPoint(n));
	}
	template <typename N>
	static Rectangle getBoundRect(const std::unique_ptr<N>& n) noexcept {
		return n->BoundRect();
	}
	template <typename ChildNode, typename Parent>
	static void setParent(const ChildNode&, Parent*) noexcept {};
	template <typename ChildNode, typename Parent>
	static void setParent(const std::unique_ptr<ChildNode>& n, Parent* parent) noexcept(noexcept(n->SetParent(parent))) {
		n->SetParent(parent);
	}
	template <typename It>
	static void setIterator(It* it, Node& node) noexcept {
		*it = It{&node, node.data_.begin() + (node.data_.size() - 1)};
	}
	static void setIterator(void*, Node&) noexcept {}

	Entry&& appendingEntry_;
	Node& srcNode_;
	const size_t srcSize_;
	std::vector<bool> moved_;
	Iterator* insertedIt_;
};

}  // namespace reindexer
