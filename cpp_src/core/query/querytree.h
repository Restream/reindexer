#pragma once

#include <functional>
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace reindexer {

/// @class QueryTree
/// A tree contained in vector
template <typename T, int holdSize>
class QueryTree {
public:
	class iterator;
	class const_iterator;

private:
	/// @class Node
	/// leaf or beginning of subtree
	class Node {
	public:
		Node(OpType op) : Op(op) {}
		Node() = delete;
		Node(const Node&) = delete;
		Node& operator=(const Node&) = delete;
		virtual ~Node() {}
		virtual bool IsEqual(const Node& other) const {
			static const auto nodeTypeHash = typeid(Node).hash_code();
			return typeid(other).hash_code() == nodeTypeHash && Op == other.Op;
		}
		virtual void CopyTo(void* buffer) const { new (buffer) Node(Op); }
		virtual void MoveTo(void* buffer) && { new (buffer) Node(Op); }
		virtual size_t Size() const { return 1; }
		virtual bool IsLeaf() const { return false; }
		/// Use IsLeaf() before this
		virtual T& Value() { throw std::runtime_error("It is not leaf"); }
		/// Use IsLeaf() before this
		virtual const T& Value() const { throw std::runtime_error("It is not leaf"); }
		/// Increase space occupied by children
		virtual void Append() { throw std::runtime_error("It is not subtree"); }
		/// Decrease space occupied by children
		virtual void Erase(size_t /*length*/) { throw std::runtime_error("It is not subtree"); }

		/// @param it - points to this object
		/// @return iterator points to the first child
		virtual iterator begin(iterator it) {
			(void)it;
			throw std::runtime_error("It is not subtree");
		}
		/// @param it - points to this object
		/// @return iterator points to the first child
		virtual const_iterator begin(const_iterator it) const {
			(void)it;
			throw std::runtime_error("It is not subtree");
		}
		/// @param it - points to this object
		/// @return iterator points to the first child
		virtual const_iterator cbegin(const_iterator it) const {
			(void)it;
			throw std::runtime_error("It is not subtree");
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		virtual iterator end(iterator it) {  // -V524
			(void)it;
			throw std::runtime_error("It is not subtree");
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		virtual const_iterator end(const_iterator it) const {  // -V524
			(void)it;
			throw std::runtime_error("It is not subtree");
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		virtual const_iterator cend(const_iterator it) const {  // -V524
			(void)it;
			throw std::runtime_error("It is not subtree");
		}

		OpType Op = OpAnd;
	};

	/// @class Leaf
	/// contains payload
	class Leaf : public Node {
	public:
		Leaf(OpType op, const T& v) : Node(op), value_(v) {}
		Leaf(OpType op, T&& v) : Node(op), value_(std::move(v)) {}
		bool IsEqual(const Node& other) const override;
		void CopyTo(void* buffer) const override { new (buffer) Leaf(this->Op, value_); }
		void MoveTo(void* buffer) && override { new (buffer) Leaf(this->Op, std::move(value_)); }
		bool IsLeaf() const override { return true; }
		/// Use IsLeaf() before this
		T& Value() override { return value_; }
		/// Use IsLeaf() before this
		const T& Value() const override { return value_; }

	private:
		T value_;
	};

	/// @class SubTree
	/// A beginnig of subtree, all children are placed just behind it
	/// contains size of space occupied by all children + 1 for this node
	class SubTree : public Node {
	public:
		SubTree(OpType op, size_t s) : Node(op), size_(s) {}
		bool IsEqual(const Node& other) const override {
			const SubTree* otherPtr = dynamic_cast<const SubTree*>(&other);
			return otherPtr && other.Op == this->Op && otherPtr->size_ == size_;
		}
		void CopyTo(void* buffer) const override { new (buffer) SubTree(this->Op, size_); }
		void MoveTo(void* buffer) && override { new (buffer) SubTree(this->Op, size_); }
		size_t Size() const override { return size_; }
		/// Increase space occupied by children
		void Append() override { ++size_; }
		/// Decrease space occupied by children
		void Erase(size_t length) override {
			assert(size_ > length);
			size_ -= length;
		}

		/// @param it - points to this object
		/// @return iterator points to the first child
		iterator begin(iterator it) override {
			assert(&*it == this);
			++(it.it_);
			return it;
		}
		/// @param it - points to this object
		/// @return iterator points to the first child
		const_iterator begin(const_iterator it) const override {
			assert(&*it == this);
			++(it.it_);
			return it;
		}
		/// @param it - points to this object
		/// @return iterator points to the first child
		const_iterator cbegin(const_iterator it) const override {
			assert(&*it == this);
			++(it.it_);
			return it;
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		iterator end(iterator it) override {
			assert(&*it == this);
			it.it_ += size_;
			return it;
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		const_iterator end(const_iterator it) const override {
			assert(&*it == this);
			it.it_ += size_;
			return it;
		}
		/// @param it - points to this object
		/// @return iterator points to the node after the last child
		const_iterator cend(const_iterator it) const override {
			assert(&*it == this);
			it.it_ += size_;
			return it;
		}

	private:
		/// size of all children + 1
		size_t size_ = 1;
	};

	/// @class Buffer
	/// wrap for any node
	class Buffer {
	public:
		Buffer() { new (buffer_) Node(OpAnd); }
		Buffer(OpType op, size_t s) { new (buffer_) SubTree(op, s); }
		Buffer(OpType op, T&& v) { new (buffer_) Leaf(op, std::move(v)); }
		Buffer(OpType op, const T& v) { new (buffer_) Leaf(op, v); }
		Buffer(const Buffer& other) { other->CopyTo(buffer_); }
		Buffer(Buffer&& other) { (*std::move(other)).MoveTo(buffer_); }
		Buffer& operator=(const Buffer& other) {
			clean();
			other->CopyTo(buffer_);
			return *this;
		}
		Buffer& operator=(Buffer&& other) {
			clean();
			(*std::move(other)).MoveTo(buffer_);
			return *this;
		}
		~Buffer() { clean(); }

		Node& operator*() & { return ref(); }
		Node&& operator*() && { return std::move(ref()); }
		const Node& operator*() const& { return ref(); }
		Node* operator->() { return ptr(); }
		const Node* operator->() const { return ptr(); }

	private:
		void clean() { (*this)->~Node(); }
		Node* ptr() { return reinterpret_cast<Node*>(buffer_); }
		const Node* ptr() const { return reinterpret_cast<const Node*>(buffer_); }
		Node& ref() { return *ptr(); }
		const Node& ref() const { return *ptr(); }

		char buffer_[sizeof(Leaf) > sizeof(SubTree) ? sizeof(Leaf) : sizeof(SubTree)];
	};

protected:
	using Container = h_vector<Buffer, holdSize>;

public:
	QueryTree() = default;
	QueryTree(const QueryTree&) = default;
	QueryTree(QueryTree&&) = default;
	QueryTree& operator=(const QueryTree&) = delete;
	QueryTree& operator=(QueryTree&&) = delete;
	bool operator==(const QueryTree& other) const {
		if (container_.size() != other.container_.size()) return false;
		for (size_t i = 0; i < container_.size(); ++i) {
			if (!container_[i]->IsEqual(*other.container_[i])) return false;
		}
		return true;
	}
	bool operator!=(const QueryTree& other) const { return !operator==(other); }

	/// Appends value to the last openned subtree
	void Append(OpType op, T&& v) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i]->Append();
		}
		container_.emplace_back(op, std::move(v));
	}
	/// Appends value to the last openned subtree
	void Append(OpType op, const T& v) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i]->Append();
		}
		container_.emplace_back(op, v);
	}
	/// Appends all nodes from the interval to the last openned subtree
	void Append(const_iterator begin, const_iterator end) {
		for (; begin != end; ++begin) {
			if (begin->IsLeaf()) {
				Append(begin->Op, begin->Value());
			} else {
				OpenBracket(begin->Op);
				Append(begin->cbegin(begin), begin->cend(begin));
				CloseBracket();
			}
		}
	}
	/// Appends value as first child of the root
	void AppendFront(OpType op, T&& v) {
		for (unsigned& i : activeBrackets_) ++i;
		container_.emplace(container_.begin(), op, std::move(v));
	}
	/// Creates subtree
	void OpenBracket(OpType op) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i]->Append();
		}
		activeBrackets_.push_back(container_.size());
		container_.emplace_back(op, 1);
	}
	/// Closes last openned subtree for appendment
	void CloseBracket() {
		if (activeBrackets_.empty()) throw Error(errLogic, "Close bracket before open");
		activeBrackets_.pop_back();
	}
	/// Sets operation to last appended leaf or last closed subtree or last openned subtree if it is empty
	void SetLastOperation(OpType op) { container_[lastAppendedElement()]->Op = op; }
	bool Empty() const { return container_.empty(); }
	size_t Size() const { return container_.size(); }
	void Reserve(size_t s) { container_.reserve(s); }
	/// @return size of leaf of subtree beginning from i
	size_t Size(size_t i) const {
		assert(i < Size());
		return container_[i]->Size();
	}
	/// @return beginning of next children of the same parent
	size_t Next(size_t i) const {
		assert(i < Size());
		return i + Size(i);
	}
	OpType GetOperation(size_t i) const {
		assert(i < Size());
		return container_[i]->Op;
	}
	void SetOperation(OpType op, size_t i) {
		assert(i < Size());
		container_[i]->Op = op;
	}
	bool IsValue(size_t i) const {
		assert(i < container_.size());
		return container_[i]->IsLeaf();
	}
	/// Use IsValue() before this
	T& operator[](size_t i) {
		assert(i < container_.size());
		return container_[i]->Value();
	}
	/// Use IsValue() before this
	const T& operator[](size_t i) const {
		assert(i < container_.size());
		return container_[i]->Value();
	}
	void ForeachValue(const std::function<void(const T&, OpType)>& func) const {
		for (const Buffer& buf : container_) {
			if (buf->IsLeaf()) func(buf->Value(), buf->Op);
		}
	}
	void ForeachValue(const std::function<void(T&)>& func) {
		for (Buffer& buf : container_) {
			if (buf->IsLeaf()) func(buf->Value());
		}
	}
	void Erase(size_t from, size_t to) {
		size_t count = to - from;
		for (size_t i = 0; i < from; ++i) {
			if (!container_[i]->IsLeaf() && Next(i) >= to) container_[i]->Erase(count);
		}
		container_.erase(container_.begin() + from, container_.begin() + to);
	}

	/// @class const_iterator
	/// iterates between children of the same parent
	class const_iterator {
		friend SubTree;

	public:
		const_iterator(typename Container::const_iterator it) : it_(it) {}
		bool operator==(const const_iterator& other) const { return it_ == other.it_; }
		bool operator!=(const const_iterator& other) const { return !operator==(other); }
		const Node& operator*() const { return **it_; }
		const Node* operator->() const { return it_->operator->(); }
		const_iterator& operator++() {
			it_ += (*it_)->Size();
			return *this;
		}
		const_iterator& operator+=(size_t shift) {
			while (shift--) operator++();
			return *this;
		}
		const_iterator operator+(size_t shift) const {
			const_iterator result(it_);
			result += shift;
			return result;
		}
		typename Container::const_iterator PlainIterator() const { return it_; }

	private:
		typename Container::const_iterator it_;
	};

	/// @class iterator
	/// iterates between children of the same parent
	class iterator {
		friend SubTree;

	public:
		iterator(typename Container::iterator it) : it_(it) {}
		bool operator==(const iterator& other) const { return it_ == other.it_; }
		bool operator!=(const iterator& other) const { return !operator==(other); }
		Node& operator*() const { return **it_; }
		Node* operator->() const { return it_->operator->(); }
		iterator& operator++() {
			it_ += (*it_)->Size();
			return *this;
		}
		iterator& operator+=(size_t shift) {
			while (shift--) operator++();
			return *this;
		}
		iterator operator+(size_t shift) const {
			iterator result(it_);
			result += shift;
			return result;
		}
		size_t DistanceTo(iterator to) const {
			size_t result = 0;
			for (iterator tmp(it_); tmp != to; ++tmp) ++result;
			return result;
		}
		operator const_iterator() const { return const_iterator(it_); }
		typename Container::iterator PlainIterator() const { return it_; }

	private:
		typename Container::iterator it_;
	};

	/// @return iterator points to the first child of root
	iterator begin() { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	const_iterator begin() const { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	const_iterator cbegin() const { return {container_.begin()}; }
	/// @return iterator points to the node after the last child of root
	iterator end() { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	const_iterator end() const { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	const_iterator cend() const { return {container_.end()}; }

protected:
	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = container_.begin(), end = container_.end();
		if (!activeBrackets_.empty()) it += (activeBrackets_.back() + 1);
		if (it == end) return this->end();
		iterator i = it, e = end;
		while (i + 1 != e) ++i;
		return i;
	}

	Container container_;
	/// stack of openned brackets (beginnigs of subtrees)
	h_vector<unsigned, 2> activeBrackets_;

private:
	/// @return the last appended leaf or last closed subtree or last openned subtree if it is empty
	size_t lastAppendedElement() const {
		assert(!container_.empty());
		size_t start = 0;  // start of last openned subtree;
		if (!activeBrackets_.empty()) {
			start = activeBrackets_.back() + 1;
			if (start == container_.size()) return start - 1;  // last oppened subtree is empty
		}
		while (Next(start) != container_.size()) start = Next(start);
		return start;
	}
};

}  // namespace reindexer
