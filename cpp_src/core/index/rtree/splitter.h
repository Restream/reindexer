#pragma once

#include <utility>
#include "core/keyvalue/geometry.h"

namespace reindexer {

template <typename Entry, typename Node, typename Traits, typename Iterator, size_t MaxEntries>
class [[nodiscard]] Splitter {
protected:
	Splitter(Entry&& appendingEntry, Node& sourceNode, Iterator* it) noexcept
		: appendingEntry_{std::move(appendingEntry)}, srcNode_{sourceNode}, insertedIt_{it} {
		assertrx(MaxEntries == srcNode_.data_.size());
	}
	~Splitter() = default;

	static double AreaIncrease(const Rectangle& lhs, const Rectangle& rhs) noexcept {
		if (lhs.Contain(rhs)) {
			return 0.0;
		}
		return reindexer::boundRect(lhs, rhs).Area() - lhs.Area();
	}

	std::pair<size_t, size_t> quadraticChooseSeeds() const {
		auto& src = srcNode_.data_;

		auto worstAreaWaste = areaWaste(getBoundRect(src[0]), getBoundRect(appendingEntry_));
		size_t seed1 = 0, seed2 = MaxEntries;
		for (size_t i = 1; i < MaxEntries; ++i) {
			const auto currAreaWaste = areaWaste(getBoundRect(src[i]), getBoundRect(appendingEntry_));
			if (currAreaWaste > worstAreaWaste) {
				worstAreaWaste = currAreaWaste;
				seed1 = i;
			}
		}
		for (size_t i = 0; i < MaxEntries - 1; ++i) {
			for (size_t j = i + 1; j < MaxEntries; ++j) {
				const auto currAreaWaste = areaWaste(getBoundRect(src[i]), getBoundRect(src[j]));
				if (currAreaWaste > worstAreaWaste) {
					worstAreaWaste = currAreaWaste;
					seed1 = i;
					seed2 = j;
				}
			}
		}
		return {seed1, seed2};
	}

	static size_t chooseSubtreeByMinAreaIncrease(const Rectangle& insertingRect, const decltype(std::declval<Node>().data_)& data,
												 bool splitOfChildAvailable) {
		size_t i = 0;
		if (!splitOfChildAvailable) {
			while (i < data.size() && data[i]->IsFull()) {
				++i;
			}
		}
		assertrx(i < data.size());
		auto minAreaIncrease = data[i]->AreaIncrease(insertingRect);
		size_t result = i;
		for (++i; i < data.size(); ++i) {
			if (!splitOfChildAvailable && data[i]->IsFull()) {
				continue;
			}
			const auto currAreaIncrease = data[i]->AreaIncrease(insertingRect);
			if ((approxEqual(currAreaIncrease, minAreaIncrease) && data[i]->BoundRect().Area() < data[result]->BoundRect().Area()) ||
				currAreaIncrease < minAreaIncrease) {
				minAreaIncrease = currAreaIncrease;
				result = i;
			}
		}
		return result;
	}

	static size_t chooseNodeByMinAreaIncrease(const Node& dst, const decltype(std::declval<Node>().data_)& data, size_t except) {
		size_t result = 0;
		if (except == 0) {
			result = 1;
			assertrx(data.size() > 1);
		}
		auto minAreaIncrease = dst.AreaIncrease(data[result]->BoundRect());
		for (size_t i = result + 1; i < data.size(); ++i) {
			if (i == except) {
				continue;
			}
			const auto currAreaIncrease = dst.AreaIncrease(data[i]->BoundRect());
			if ((approxEqual(currAreaIncrease, minAreaIncrease) && data[i]->BoundRect().Area() < data[result]->BoundRect().Area()) ||
				currAreaIncrease < minAreaIncrease) {
				minAreaIncrease = currAreaIncrease;
				result = i;
			}
		}
		return result;
	}

	void moveEntryTo(Node& node, size_t idx) {
		if (idx == MaxEntries) {
			node.data_.emplace_back(std::move(appendingEntry_));
			setIterator(insertedIt_, node);
		} else {
			node.data_.emplace_back(std::move(srcNode_.data_[idx]));
		}
		setParent(node.data_.back(), &node);
		if (node.data_.size() == 1) {
			node.SetBoundRect(getBoundRect(node.data_[0]));
		} else {
			node.SetBoundRect(boundRect(node.BoundRect(), getBoundRect(node.data_.back())));
		}
	}
	template <typename N>
	static Rectangle getBoundRect(const N& n) noexcept {
		return boundRect(Traits::GetPoint(n));
	}
	template <typename N>
	static Rectangle getBoundRect(const std::unique_ptr<N>& n) noexcept {
		return n->BoundRect();
	}

private:
	template <typename N, typename Parent>
	static void setParent(const N&, Parent*) noexcept {};
	template <typename N, typename Parent>
	static void setParent(const std::unique_ptr<N>& n, Parent* parent) noexcept(noexcept(n->SetParent(parent))) {
		n->SetParent(parent);
	}

	static double areaWaste(const Rectangle& a, const Rectangle& b) noexcept {
		return reindexer::boundRect(a, b).Area() - a.Area() - b.Area();
	}

	template <typename It>
	static void setIterator(It* it, Node& node) noexcept {
		*it = It{&node, (node.data_.size() - 1)};
	}
	static void setIterator(void*, Node&) noexcept {}

protected:
	Entry&& appendingEntry_;
	Node& srcNode_;

private:
	Iterator* insertedIt_;
};

}  // namespace reindexer
