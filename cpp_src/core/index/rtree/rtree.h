#pragma once

#include <memory>
#include "core/keyvalue/geometry.h"
#include "estl/h_vector.h"

struct CollateOpts;

namespace reindexer {

struct [[nodiscard]] DefaultRTreeTraits {
	static Point GetPoint(Point v) noexcept { return v; }
};

template <typename T, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries, typename Traits = DefaultRTreeTraits>
class [[nodiscard]] RectangleTree {
	static_assert(1 < MinEntries && MinEntries <= MaxEntries / 2);

public:
	struct [[nodiscard]] Visitor {
		virtual bool operator()(const T&) = 0;
		virtual ~Visitor() noexcept = default;
	};

private:
	class Leaf;

	template <typename NodeBaseT>
	class [[nodiscard]] Iterator {
		template <typename U>
		struct [[nodiscard]] LeafTraits {
			using value_type = T;
			using leaf_type = Leaf;
			using node_base_type = U;
		};
		template <typename U>
		struct [[nodiscard]] LeafTraits<const U> {
			using value_type = const T;
			using leaf_type = const Leaf;
			using node_base_type = U;
		};
		using value_type = typename LeafTraits<NodeBaseT>::value_type;
		using leaf_type = typename LeafTraits<NodeBaseT>::leaf_type;
		using node_base_type = typename LeafTraits<NodeBaseT>::node_base_type;
		friend Iterator<const NodeBaseT>;
		friend RectangleTree;

	public:
		Iterator(leaf_type* l, size_t pos) noexcept : pos_{pos}, leaf_{l} {}
		Iterator(const Iterator&) noexcept = default;
		Iterator& operator=(const Iterator&) noexcept = default;
		template <typename It, typename std::enable_if<std::is_same<It, Iterator<node_base_type>>::value && std::is_const<NodeBaseT>::value,
													   void*>::type = nullptr>
		Iterator(const It& other) : pos_{other.pos_}, leaf_{other.leaf_} {}
		template <typename It>
		auto operator=(const It& other) noexcept ->
			typename std::enable_if<std::is_same<It, Iterator<node_base_type>>::value && std::is_const<NodeBaseT>::value, Iterator>::type& {
			pos_ = other.pos_;
			leaf_ = other.leaf_;
			return *this;
		}
		value_type& operator*() const noexcept { return leaf_->data_[pos_]; }
		value_type* operator->() const noexcept { return &operator*(); }
		bool operator==(const Iterator<node_base_type>& other) const noexcept { return pos_ == other.pos_ && leaf_ == other.leaf_; }
		bool operator==(const Iterator<const node_base_type>& other) const noexcept { return pos_ == other.pos_ && leaf_ == other.leaf_; }
		Iterator& operator++() noexcept {
			assertrx(pos_ < leaf_->data_.size());
			++pos_;
			if (pos_ == leaf_->data_.size()) {
				NodeBaseT* n = leaf_;
				while (n->Parent() && n == n->Parent()->data_.back().get()) {
					n = n->Parent();
				}
				if (n->Parent()) {
					auto& childrenOfParent = n->Parent()->data_;
					auto i = std::find_if(childrenOfParent.begin(), childrenOfParent.end(),
										  [n](const std::unique_ptr<node_base_type>& v) { return n == v.get(); });
					assertrx(i != childrenOfParent.end());
					++i;
					assertrx(i != childrenOfParent.end());
					*this = (*i)->begin();
				}
			}
			return *this;
		}

	private:
		size_t pos_;
		leaf_type* leaf_;
	};

	class NodeBase;

public:
	using iterator = Iterator<NodeBase>;
	using const_iterator = Iterator<const NodeBase>;

private:
	class Node;

	class [[nodiscard]] NodeBase : private Rectangle {
	public:
		explicit NodeBase(const Rectangle& r = {}) noexcept : Rectangle{r} {}
		virtual ~NodeBase() noexcept = default;
		const Rectangle& BoundRect() const noexcept { return *this; }
		void SetBoundRect(const Rectangle& r) noexcept { this->Rectangle::operator=(r); }
		virtual bool IsLeaf() const noexcept = 0;
		const Node* Parent() const noexcept { return parent_; }
		Node* Parent() noexcept { return parent_; }
		void SetParent(Node* p) noexcept { parent_ = p; }
		virtual double AreaIncrease(const Rectangle&) const noexcept = 0;
		virtual bool IsFull() const noexcept = 0;
		virtual size_t Size() const noexcept = 0;
		virtual bool Empty() const noexcept = 0;
		virtual std::unique_ptr<NodeBase> Clone() const = 0;

		virtual std::pair<std::unique_ptr<NodeBase>, std::unique_ptr<NodeBase>> insert(T&&, iterator& insertedIt, bool splitAvailable) = 0;
		virtual std::pair<bool, bool> DeleteOneIf(Visitor&) = 0;
		virtual bool DWithin(Point, double distance, RectangleTree::Visitor&) const noexcept = 0;
		virtual bool ForEach(RectangleTree::Visitor&) const noexcept = 0;

		virtual const_iterator cbegin() const noexcept = 0;
		virtual const_iterator begin() const noexcept = 0;
		virtual iterator begin() noexcept = 0;
		virtual const_iterator cend() const noexcept = 0;
		virtual const_iterator end() const noexcept = 0;
		virtual iterator end() noexcept = 0;

		virtual std::pair<iterator, bool> find(Point) noexcept = 0;
		virtual std::pair<const_iterator, bool> find(Point) const noexcept = 0;

		virtual bool Check(const Node* parent) const noexcept = 0;

	private:
		Node* parent_ = nullptr;
	};

	class [[nodiscard]] Leaf : public NodeBase {
		using SplitterT = Splitter<T, Leaf, Traits, iterator, MaxEntries, MinEntries>;
		friend Node;

	public:
		Leaf() noexcept = default;
		Leaf(const Leaf& other) : NodeBase{other.BoundRect()}, data_{other.data_} {}
		Leaf(Leaf&& other) noexcept : NodeBase{other.BoundRect()}, data_{std::move(other.data_)} {}
		using Container = h_vector<T, MaxEntries>;
		bool IsLeaf() const noexcept override { return true; }
		bool IsFull() const noexcept override { return data_.size() == MaxEntries; }
		size_t Size() const noexcept override { return data_.size(); }
		bool Empty() const noexcept override { return data_.empty(); }
		std::unique_ptr<NodeBase> Clone() const override { return std::unique_ptr<NodeBase>{new Leaf{*this}}; }

		const_iterator cbegin() const noexcept override { return {this, 0}; }
		const_iterator begin() const noexcept override { return cbegin(); }
		iterator begin() noexcept override { return {this, 0}; }
		const_iterator cend() const noexcept override { return {this, data_.size()}; }
		const_iterator end() const noexcept override { return cend(); }
		iterator end() noexcept override { return {this, data_.size()}; }

		std::pair<iterator, bool> find(Point p) noexcept override {
			const auto it = std::find_if(data_.begin(), data_.end(), [p](const T& v) { return p == Traits::GetPoint(v); });
			return {{this, size_t(std::distance(data_.begin(), it))}, it != data_.end()};
		}
		std::pair<const_iterator, bool> find(Point p) const noexcept override {
			const auto it = std::find_if(data_.cbegin(), data_.cend(), [p](const T& v) { return p == Traits::GetPoint(v); });
			return {{this, size_t(std::distance(data_.begin(), it))}, it != data_.cend()};
		}

		std::pair<std::unique_ptr<NodeBase>, std::unique_ptr<NodeBase>> insert(T&& v, iterator& insertedIt, bool splitAvailable) override {
			if (data_.size() < MaxEntries) {
				if (data_.empty()) {
					this->SetBoundRect(boundRect(Traits::GetPoint(v)));
				} else {
					this->SetBoundRect(boundRect(this->BoundRect(), Traits::GetPoint(v)));
				}
				data_.emplace_back(std::move(v));
				insertedIt = iterator{this, (data_.size() - 1)};
				return {};
			} else {
				assertrx(splitAvailable);
				(void)splitAvailable;
				SplitterT splitter{std::move(v), *this, &insertedIt};
				return splitter.Split();
			}
		}

		bool DWithin(Point p, double distance, RectangleTree::Visitor& visitor) const noexcept override {
			for (const auto& v : data_) {
				if (reindexer::DWithin(Traits::GetPoint(v), p, distance)) {
					if (visitor(v)) {
						return true;
					}
				}
			}
			return false;
		}
		bool ForEach(RectangleTree::Visitor& visitor) const noexcept override {
			for (const auto& v : data_) {
				if (visitor(v)) {
					return true;
				}
			}
			return false;
		}

		std::pair<bool, bool> DeleteOneIf(Visitor& visitor) override {
			for (auto it = data_.begin(), end = data_.end(); it != end; ++it) {
				if (visitor(*it)) {
					data_.erase(it);
					if (data_.size() < MinEntries) {
						if (data_.empty()) {
							this->SetBoundRect({});
						}
						return {true, true};
					} else {
						adjustBoundRect();
						return {true, false};
					}
				}
			}
			return {false, false};
		}
		void erase(typename Container::iterator it) {
			it = data_.erase(it);
			if (data_.size() < MinEntries) {
				this->Parent()->condenseTree(this);
			} else {
				adjustBoundRect();
				for (Node* parent{this->Parent()}; parent; parent = parent->Parent()) {
					parent->adjustBoundRect();
				}
			}
		}

		double AreaIncrease(const reindexer::Rectangle& r) const noexcept override {
			if (data_.empty()) {
				return r.Area();
			}
			return SplitterT::AreaIncrease(this->BoundRect(), r);
		}

		bool Check(const Node* parent) const noexcept override {
			if (parent != this->Parent()) {
				return false;
			}
			if (data_.size() > MaxEntries) {
				return false;
			}
			if (data_.empty()) {
				if (this->BoundRect() != reindexer::Rectangle{}) {
					return false;
				}
			} else {
				const reindexer::Rectangle thisBoundRect{this->BoundRect()};
				reindexer::Rectangle boundRectOfAllChildren = boundRect(Traits::GetPoint(data_[0]));
				for (const auto& v : data_) {
					boundRectOfAllChildren = boundRect(boundRectOfAllChildren, Traits::GetPoint(v));
					if (!thisBoundRect.Contain(Traits::GetPoint(v))) {
						return false;
					}
				}
				if (thisBoundRect != boundRectOfAllChildren) {
					return false;
				}
			}
			return true;
		}

	private:
		void adjustBoundRect() {
			if (data_.empty()) {
				this->SetBoundRect({});
				return;
			}
			reindexer::Rectangle newBoundRect{boundRect(Traits::GetPoint(data_[0]))};
			for (size_t i = 1; i < data_.size(); ++i) {
				newBoundRect = boundRect(newBoundRect, Traits::GetPoint(data_[i]));
			}
			this->SetBoundRect(newBoundRect);
		}

	public:
		Container data_;
	};

	class [[nodiscard]] Node : public NodeBase {
		using SplitterT = Splitter<std::unique_ptr<NodeBase>, Node, Traits, void, MaxEntries, MinEntries>;
		friend Leaf;
		friend RectangleTree;

	public:
		using Container = h_vector<std::unique_ptr<NodeBase>, MaxEntries>;

		Node() noexcept = default;
		Node(const reindexer::Rectangle& r, Container&& c) noexcept : NodeBase{r}, data_{std::move(c)} {}
		Node(Node&& other) noexcept : NodeBase{other.BoundRect()}, data_{std::move(other.data_)} {
			for (auto& n : data_) {
				n->SetParent(this);
			}
		}
		Node(const Node& other) : NodeBase{other.BoundRect()} {
			for (const auto& n : other.data_) {
				data_.emplace_back(n->Clone());
				data_.back()->SetParent(this);
			}
		}

		bool IsLeaf() const noexcept override { return false; }
		bool IsFull() const noexcept override {
			if (data_.size() < MaxEntries) {
				return false;
			}
			for (const auto& n : data_) {
				if (!n->IsFull()) {
					return false;
				}
			}
			return true;
		}
		size_t Size() const noexcept override {
			size_t result = 0;
			for (const auto& n : data_) {
				result += n->Size();
			}
			return result;
		}
		bool Empty() const noexcept override {
			for (const auto& n : data_) {
				if (!n->Empty()) {
					return false;
				}
			}
			return true;
		}
		std::unique_ptr<NodeBase> Clone() const override { return std::unique_ptr<NodeBase>{new Node{*this}}; }

		const_iterator cbegin() const noexcept override {
			assertrx(!data_.empty());
			return data_[0]->cbegin();
		}
		const_iterator begin() const noexcept override { return cbegin(); }
		iterator begin() noexcept override {
			assertrx(!data_.empty());
			return data_[0]->begin();
		}
		const_iterator cend() const noexcept override {
			assertrx(!data_.empty());
			return data_.back()->cend();
		}
		const_iterator end() const noexcept override { return cend(); }
		iterator end() noexcept override {
			assertrx(!data_.empty());
			return data_.back()->end();
		}

		std::pair<iterator, bool> find(Point p) noexcept override {
			for (auto& n : data_) {
				if (n->BoundRect().Contain(p)) {
					const auto res = n->find(p);
					if (res.second) {
						return res;
					}
				}
			}
			return {{nullptr, {}}, false};
		}
		std::pair<const_iterator, bool> find(Point p) const noexcept override {
			for (auto& n : data_) {
				if (n->BoundRect().Contain(p)) {
					const auto res = n->find(p);
					if (res.second) {
						return res;
					}
				}
			}
			return {{nullptr, {}}, false};
		}

		std::pair<std::unique_ptr<NodeBase>, std::unique_ptr<NodeBase>> insert(T&& v, iterator& insertedIt, bool splitAvailable) override {
			const reindexer::Rectangle boundRectOfObject = boundRect(Traits::GetPoint(v));
			const bool splitOfChildAvailable = splitAvailable || data_.size() < MaxEntries;
			const size_t nodeToInsert = SplitterT::ChooseSubtree(boundRectOfObject, data_, splitOfChildAvailable);

			auto splittedChildren = data_[nodeToInsert]->insert(std::move(v), insertedIt, splitOfChildAvailable);
			if (splittedChildren.first) {
				data_[nodeToInsert] = std::move(splittedChildren.first);
				auto splittedNodes = insert(std::move(splittedChildren.second));
				if (splittedNodes.first) {
					return splittedNodes;
				}
				data_[nodeToInsert]->SetParent(this);
			}
			if (data_.size() == 1) {
				this->SetBoundRect(data_[0]->BoundRect());
			} else {
				this->SetBoundRect(boundRect(this->BoundRect(), boundRectOfObject));
			}
			return {};
		}

		bool DWithin(Point p, double distance, RectangleTree::Visitor& visitor) const noexcept override {
			for (const auto& n : data_) {
				if (reindexer::DWithin(n->BoundRect(), p, distance)) {
					if (n->ForEach(visitor)) {
						return true;
					}
				} else if (intersect(n->BoundRect(), Circle{p, distance})) {
					if (n->DWithin(p, distance, visitor)) {
						return true;
					}
				}
			}
			return false;
		}
		bool ForEach(RectangleTree::Visitor& visitor) const noexcept override {
			for (const auto& n : data_) {
				if (n->ForEach(visitor)) {
					return true;
				}
			}
			return false;
		}

		std::pair<bool, bool> DeleteOneIf(Visitor& visitor) override {
			for (size_t i = 0; i < data_.size(); ++i) {
				const auto deletionResult{data_[i]->DeleteOneIf(visitor)};
				if (deletionResult.first) {
					if (deletionResult.second) {
						condenseTree(i);
						return {true, data_.size() < MinEntries};
					} else {
						adjustBoundRect();
						return {true, false};
					}
				}
			}
			return {false, false};
		}

		double AreaIncrease(const reindexer::Rectangle& r) const noexcept override {
			assertrx(!data_.empty());
			return SplitterT::AreaIncrease(this->BoundRect(), r);
		}

		bool Check(const Node* parent) const noexcept override {
			if (parent != this->Parent()) {
				return false;
			}
			if (data_.empty()) {
				return false;
			}
			if (data_.size() > MaxEntries) {
				return false;
			}
			const reindexer::Rectangle thisBoundRect{this->BoundRect()};
			reindexer::Rectangle boundRectOfAllChildren = data_[0]->BoundRect();
			for (const auto& n : data_) {
				if (!n->Check(this)) {
					return false;
				}
				boundRectOfAllChildren = boundRect(boundRectOfAllChildren, n->BoundRect());
				if (!thisBoundRect.Contain(n->BoundRect())) {
					return false;
				}
			}
			if (this->BoundRect() != boundRectOfAllChildren) {
				return false;
			}
			return true;
		}

	private:
		std::pair<std::unique_ptr<NodeBase>, std::unique_ptr<NodeBase>> insert(std::unique_ptr<NodeBase>&& ptr) {
			if (data_.size() < MaxEntries) {
				data_.emplace_back(std::move(ptr));
				this->SetBoundRect(boundRect(this->BoundRect(), data_.back()->BoundRect()));
				data_.back()->SetParent(this);
				return {};
			} else {
				SplitterT splitter{std::move(ptr), *this, nullptr};
				return splitter.Split();
			}
		}

		void condenseTree(NodeBase* node) {
			size_t i = 0;
			while (i < data_.size() && data_[i].get() != node) {
				++i;
			}
			assertrx(i < data_.size());
			condenseTree(i);
			if (data_.size() < MinEntries && this->Parent()) {
				this->Parent()->condenseTree(this);
			} else {
				for (Node* parent{this->Parent()}; parent; parent = parent->Parent()) {
					parent->adjustBoundRect();
				}
			}
		}

		void condenseTree(size_t deletingNode) {
			assertrx(deletingNode < data_.size());
			if (!this->Parent() && data_.size() == 1) {
				assertrx(data_[0]->IsLeaf());
				static_cast<Leaf*>(data_[0].get())->adjustBoundRect();
				this->SetBoundRect(data_[0]->BoundRect());
				return;
			}
			if (data_[deletingNode]->IsLeaf()) {
				const std::unique_ptr<Leaf> delLeaf{static_cast<Leaf*>(data_[deletingNode].release())};
				data_.erase(data_.begin() + deletingNode);
				adjustBoundRect();
				iterator tmpIt{begin()};
				for (auto& v : delLeaf->data_) {
					const auto splittedNodes{insert(std::move(v), tmpIt, false)};
					assertrx(!splittedNodes.first);
				}
			} else {
				Node* n{static_cast<Node*>(data_[deletingNode].get())};
				Container& d{n->data_};
				if (d.empty()) {
					data_.erase(data_.begin() + deletingNode);
				} else if (data_.size() - 1 + d.size() <= MaxEntries) {
					const std::unique_ptr<Node> delNode{static_cast<Node*>(data_[deletingNode].release())};
					data_[deletingNode] = std::move(d[0]);
					data_[deletingNode]->SetParent(this);
					for (size_t i = 1; i < d.size(); ++i) {
						data_.emplace_back(std::move(d[i]));
						data_.back()->SetParent(this);
					}
				} else {
					const size_t nodeToMove = SplitterT::ChooseNode(*n, data_, deletingNode);
					d.emplace_back(std::move(data_[nodeToMove]));
					d.back()->SetParent(n);
					n->adjustBoundRect();

					data_.erase(data_.begin() + nodeToMove);
				}
				adjustBoundRect();
			}
		}

		void adjustBoundRect() noexcept {
			assertrx(!data_.empty());
			auto newBoundRect{data_[0]->BoundRect()};
			for (size_t i = 1; i < data_.size(); ++i) {
				newBoundRect = boundRect(newBoundRect, data_[i]->BoundRect());
			}
			this->SetBoundRect(newBoundRect);
		}

	public:
		Container data_;
	};

public:
	using value_type = T;
	using traits = Traits;

	RectangleTree() { root_.insert(std::unique_ptr<NodeBase>{new Leaf}); }
	size_t size() const noexcept { return root_.Size(); }
	bool empty() const noexcept { return root_.Empty(); }
	std::pair<iterator, bool> insert(T&& v) {
		const auto findRes = root_.find(Traits::GetPoint(v));
		if (findRes.second) {
			return {findRes.first, false};
		} else {
			return {insert_without_test(std::move(v)), true};
		}
	}
	iterator insert_without_test(T&& v) {
		iterator inserted = begin();
		auto splittedNodes = root_.insert(std::move(v), inserted, true);
		if (splittedNodes.first) {
			root_.data_.clear();
			root_.data_.emplace_back(std::move(splittedNodes.first));
			root_.data_.back()->SetParent(&root_);
			root_.data_.emplace_back(std::move(splittedNodes.second));
			root_.data_.back()->SetParent(&root_);
			root_.SetBoundRect(boundRect(root_.data_[0]->BoundRect(), root_.data_[1]->BoundRect()));
		}
		return inserted;
	}
	bool DeleteOneIf(Visitor& visitor) { return root_.DeleteOneIf(visitor).first; }
	void erase(iterator it) { it.leaf_->erase(it.leaf_->data_.begin() + it.pos_); }

	const_iterator cbegin() const noexcept { return root_.cbegin(); }
	const_iterator begin() const noexcept { return cbegin(); }
	iterator begin() noexcept { return root_.begin(); }
	const_iterator cend() const noexcept { return root_.cend(); }
	const_iterator end() const noexcept { return cend(); }
	iterator end() noexcept { return root_.end(); }

	iterator find(Point p) noexcept {
		const auto res = root_.find(p);
		if (res.second) {
			return res.first;
		}
		return end();
	}
	const_iterator find(Point p) const noexcept {
		const auto res = root_.find(p);
		if (res.second) {
			return res.first;
		}
		return cend();
	}
	void DWithin(Point p, double distance, RectangleTree::Visitor& visitor) const { root_.DWithin(p, distance, visitor); }

	bool Check() const noexcept { return root_.Check(nullptr); }

private:
	Node root_;
};

template <typename Key, typename T>
class [[nodiscard]] RMapValue {
	Key first_;

public:
	const Key& first;
	T second;
	RMapValue() : first_{}, first{first_}, second{} {}
	RMapValue(Key&& k, T&& v) : first_{std::move(k)}, first{first_}, second{std::move(v)} {}
	RMapValue(Key&& k, const T& v) : first_{std::move(k)}, first{first_}, second{v} {}
	RMapValue(const Key& k, T&& v) : first_{k}, first{first_}, second{std::move(v)} {}
	RMapValue(const Key& k, const T& v) : first_{k}, first{first_}, second{v} {}
	RMapValue(RMapValue&& other) : first_{std::move(other.first_)}, first{first_}, second{std::move(other.second)} {}
	RMapValue(const RMapValue& other) : first_{other.first_}, first{first_}, second{other.second} {}
	RMapValue& operator=(RMapValue&& other) noexcept {
		first_ = std::move(other.first_);
		second = std::move(other.second);
		return *this;
	}
};

template <typename T>
struct [[nodiscard]] DefaultRMapTraits {
	static Point GetPoint(const RMapValue<Point, T>& p) noexcept { return p.first; }
};

template <typename T, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries, typename Traits = DefaultRMapTraits<T>>
class [[nodiscard]] RTreeMap : public RectangleTree<RMapValue<Point, T>, Splitter, MaxEntries, MinEntries, Traits> {
public:
	using key_type = Point;
	using mapped_type = T;
};

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
class [[nodiscard]] GeometryMap : public RTreeMap<KeyEntryT, Splitter, MaxEntries, MinEntries> {
	using Base = RTreeMap<KeyEntryT, Splitter, MaxEntries, MinEntries>;

public:
	GeometryMap() = default;
	template <typename>
	void erase(typename Base::iterator it) {
		Base::erase(it);
	}
};

}  // namespace reindexer
