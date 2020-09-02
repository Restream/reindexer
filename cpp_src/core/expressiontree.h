#pragma once

#include "estl/h_vector.h"
#include "tools/errors.h"
#include "vendor/mpark/variant.h"

namespace reindexer {

/// @class Bracket
/// A beginnig of subtree, all children are placed just behind it
/// contains size of space occupied by all children + 1 for this node
class Bracket {
public:
	Bracket(size_t s) noexcept : size_(s) {}
	size_t Size() const noexcept { return size_; }
	/// Increase space occupied by children
	void Append() noexcept { ++size_; }
	/// Decrease space occupied by children
	void Erase(size_t length) noexcept {
		assert(size_ > length);
		size_ -= length;
	}
	void CopyPayloadFrom(const Bracket&) const noexcept {}
	bool operator==(const Bracket& other) const noexcept { return size_ == other.size_; }

private:
	/// size of all children + 1
	size_t size_ = 1;
};

/// @class ExpressionTree
/// A tree contained in vector
/// For detailed documentation see expressiontree.md
template <typename OperationType, typename SubTree, int holdSize, typename... Ts>
class ExpressionTree {
	template <typename T>
	class Ref {
	public:
		explicit Ref(T& v) noexcept : ptr_{&v} {}
		operator T&() noexcept { return *ptr_; }
		operator const T&() const noexcept { return *ptr_; }
		bool operator==(const Ref& other) const noexcept(noexcept(std::declval<T>() == std::declval<T>())) { return *ptr_ == *other.ptr_; }

	private:
		T* ptr_;
	};

	template <typename T>
	struct MakeTransparentRef {
		using type = Ref<T>;
	};
	template <typename T>
	using TransparentRef = typename MakeTransparentRef<T>::type;
	template <typename T>
	struct MakeTransparentRef<T const> {
		using type = TransparentRef<T> const;
	};
	template <typename T>
	struct MakeTransparentRef<T&> {
		using type = TransparentRef<T>&;
	};
	template <typename T>
	struct MakeTransparentRef<T&&> {
		using type = TransparentRef<T>&&;
	};

	template <typename R, typename Arg, typename... Args>
	class Visitor : public Visitor<R, Args...> {
	public:
		Visitor(const std::function<R(Arg)>& f, const std::function<R(Args)>&... funcs) : Visitor<R, Args...>{funcs...}, functor_(f) {}
		using Visitor<R, Args...>::operator();
		R operator()(Arg arg) const { return functor_(arg); }
		R operator()(TransparentRef<Arg> arg) const { return functor_(arg); }

	private:
		const std::function<R(Arg)>& functor_;
	};
	template <typename R, typename Arg>
	class Visitor<R, Arg> {
	public:
		Visitor(const std::function<R(Arg)>& f) : functor_(f) {}
		R operator()(Arg arg) const { return functor_(arg); }
		R operator()(TransparentRef<Arg> arg) const { return functor_(arg); }

	private:
		const std::function<R(Arg)>& functor_;
	};
	template <typename Arg>
	class Visitor<void, Arg> {
	public:
		Visitor(const std::function<void(Arg)>& f) : functor_(f) {}
		void operator()(Arg arg) const { functor_(arg); }
		void operator()(TransparentRef<Arg> arg) const { return functor_(arg); }
		template <typename T>
		void operator()(T) const {}

	private:
		const std::function<void(Arg)>& functor_;
	};

	template <typename T, typename...>
	struct Head_t {
		using type = T;
	};
	template <typename... Args>
	using Head = typename Head_t<Args...>::type;

	/// @class Node
	class Node {
		friend ExpressionTree;

		using Storage = mpark::variant<SubTree, Ts..., Ref<Ts>...>;

		struct SizeVisitor {
			template <typename T>
			size_t operator()(const T&) const noexcept {
				return 1;
			}
			size_t operator()(const SubTree& subTree) const noexcept { return subTree.Size(); }
		};

		template <typename T>
		struct GetVisitor {
			T& operator()(T& v) const noexcept { return v; }
			T& operator()(Ref<T>& r) const noexcept { return r; }
			template <typename U>
			T& operator()(U&) const noexcept {
				assert(0);
				abort();
			}
		};
		template <typename T>
		struct GetVisitor<T const> {
			const T& operator()(const T& v) const noexcept { return v; }
			const T& operator()(const Ref<T>& r) const noexcept { return r; }
			template <typename U>
			const T& operator()(const U&) const noexcept {
				assert(0);
				abort();
			}
		};
		struct EqVisitor {
			template <typename T>
			bool operator()(const T& lhs, const T& rhs) const noexcept(noexcept(lhs == rhs)) {
				return lhs == rhs;
			}
			template <typename T>
			bool operator()(const Ref<T>& lhs, const T& rhs) const noexcept(noexcept(rhs == rhs)) {
				return static_cast<const T&>(lhs) == rhs;
			}
			template <typename T>
			bool operator()(const T& lhs, const Ref<T>& rhs) const noexcept(noexcept(lhs == lhs)) {
				return lhs == static_cast<const T&>(rhs);
			}
			template <typename T, typename U>
			bool operator()(const T&, const U&) const noexcept {
				return false;
			}
		};
		struct LazyCopyVisitor {
			Storage operator()(SubTree& st) const noexcept { return st; }
			template <typename T>
			Storage operator()(Ref<T>& r) const {
				return r;
			}
			template <typename T>
			Storage operator()(T& v) const {
				return Ref<T>{v};
			}
		};
		struct DeepCopyVisitor {
			Storage operator()(const SubTree& st) const noexcept { return st; }
			template <typename T>
			Storage operator()(const Ref<T>& r) const {
				return static_cast<const T&>(r);
			}
			template <typename T>
			Storage operator()(const T& v) const {
				return v;
			}
		};
		struct DeepRValueCopyVisitor {
			Storage operator()(SubTree&& st) const noexcept { return std::move(st); }
			template <typename T>
			Storage operator()(Ref<T>&& r) const {
				return static_cast<const T&>(r);
			}
			template <typename T>
			Storage operator()(T&& v) const {
				return std::move(v);
			}
		};
		struct IsRefVisitor {
			template <typename T>
			bool operator()(const T&) const noexcept {
				return false;
			}
			template <typename T>
			bool operator()(const Ref<T>&) const noexcept {
				return true;
			}
		};

	public:
		Node() : storage_{SubTree{1}} {}
		template <typename... Args>
		Node(OperationType op, size_t s, Args&&... args) : storage_{SubTree{s, std::forward<Args>(args)...}}, operation{op} {}
		template <typename T>
		Node(OperationType op, T&& v) : storage_{std::forward<T>(v)}, operation{op} {}
		Node(const Node& other) : storage_{other.storage_}, operation{other.operation} {}
		Node(Node&& other) : storage_{std::move(other.storage_)}, operation{std::move(other.operation)} {}
		~Node() = default;
		Node& operator=(const Node& other) {
			storage_ = other.storage_;
			operation = other.operation;
			return *this;
		}
		Node& operator=(Node&& other) {
			storage_ = std::move(other.storage_);
			operation = std::move(other.operation);
			return *this;
		}
		bool operator==(const Node& other) const {
			static const EqVisitor visitor;
			return operation == other.operation && mpark::visit(visitor, storage_, other.storage_);
		}
		bool operator!=(const Node& other) const { return !operator==(other); }

		template <typename T = Head<Ts...>>
		T& Value() {
			const static GetVisitor<T> visitor;
			return mpark::visit(visitor, storage_);
		}
		template <typename T = Head<Ts...>>
		const T& Value() const {
			const static GetVisitor<const T> visitor;
			return mpark::visit(visitor, storage_);
		}
		size_t Size() const noexcept {
			static constexpr SizeVisitor sizeVisitor;
			return mpark::visit(sizeVisitor, storage_);
		}
		bool IsSubTree() const noexcept { return storage_.index() == 0; }
		bool IsLeaf() const noexcept { return !IsSubTree(); }
		template <typename T>
		bool Holds() const noexcept {
			return mpark::holds_alternative<T>(storage_);
		}
		void Append() { mpark::get<SubTree>(storage_).Append(); }
		void Erase(size_t length) { mpark::get<SubTree>(storage_).Erase(length); }
		/// Execute appropriate functor depending on content type, skip if no appropriate functor
		template <typename... Args>
		void ExecuteAppropriate(const std::function<void(Args&)>&... funcs) {
			mpark::visit(Visitor<void, Args&...>{funcs...}, storage_);
		}
		/// Execute appropriate functor depending on content type, skip if no appropriate functor
		template <typename... Args>
		void ExecuteAppropriate(const std::function<void(const Args&)>&... funcs) const {
			mpark::visit(Visitor<void, const Args&...>{funcs...}, storage_);
		}
		/// Execute appropriate functor depending on content type
		template <typename R>
		R CalculateAppropriate(const std::function<R(const SubTree&)>& f, const std::function<R(const Ts&)>&... funcs) const {
			return mpark::visit(Visitor<R, const SubTree&, const Ts&...>{f, funcs...}, storage_);
		}

		Node MakeLazyCopy() & {
			static const LazyCopyVisitor visitor;
			return {operation, mpark::visit(visitor, storage_)};
		}
		Node MakeDeepCopy() const & {
			static const DeepCopyVisitor visitor;
			return {operation, mpark::visit(visitor, storage_)};
		}
		Node MakeDeepCopy() && {
			static const DeepRValueCopyVisitor visitor;
			return {operation, mpark::visit(visitor, std::move(storage_))};
		}
		bool IsRef() const {
			static const IsRefVisitor visitor;
			return mpark::visit(visitor, storage_);
		}
		template <typename T>
		void SetValue(T&& v) {
			storage_ = std::forward<T>(v);
		}

	private:
		Storage storage_;

	public:
		OperationType operation;
	};

protected:
	using Container = h_vector<Node, holdSize>;

public:
	ExpressionTree() = default;
	ExpressionTree(ExpressionTree&&) = default;
	ExpressionTree& operator=(ExpressionTree&&) = default;
	ExpressionTree(const ExpressionTree& other) : activeBrackets_{other.activeBrackets_} {
		container_.reserve(other.container_.size());
		for (const Node& n : other.container_) container_.emplace_back(n.MakeDeepCopy());
	}
	ExpressionTree& operator=(const ExpressionTree& other) {
		if (this == &other) return *this;
		container_.clear();
		container_.reserve(other.container_.size());
		for (const Node& n : other.container_) container_.emplace_back(n.MakeDeepCopy());
		activeBrackets_ = other.activeBrackets_;
		return *this;
	}
	bool operator==(const ExpressionTree& other) const {
		if (container_.size() != other.container_.size()) return false;
		for (size_t i = 0; i < container_.size(); ++i) {
			if (container_[i] != other.container_[i]) return false;
		}
		return true;
	}
	bool operator!=(const ExpressionTree& other) const { return !operator==(other); }

	/// Appends value to the last openned subtree
	template <typename T>
	void Append(OperationType op, T&& v) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, std::move(v));
	}
	/// Appends value to the last openned subtree
	template <typename T>
	void Append(OperationType op, const T& v) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, v);
	}
	class const_iterator;
	/// Appends all nodes from the interval to the last openned subtree
	void Append(const_iterator begin, const_iterator end) {
		container_.reserve(container_.size() + (end.PlainIterator() - begin.PlainIterator()));
		append(begin, end);
	}
	class iterator;
	void LazyAppend(iterator begin, iterator end) {
		container_.reserve(container_.size() + (end.PlainIterator() - begin.PlainIterator()));
		lazyAppend(begin, end);
	}

	/// Appends value as first child of the root
	template <typename T>
	void AppendFront(OperationType op, T&& v) {
		for (unsigned& i : activeBrackets_) ++i;
		container_.emplace(container_.begin(), op, std::move(v));
	}
	/// Creates subtree
	template <typename... Args>
	void OpenBracket(OperationType op, Args&&... args) {
		for (unsigned i : activeBrackets_) {
			assert(i < container_.size());
			container_[i].Append();
		}
		activeBrackets_.push_back(container_.size());
		container_.emplace_back(op, size_t{1}, std::forward<Args>(args)...);
	}
	/// Closes last openned subtree for appendment
	void CloseBracket() {
		if (activeBrackets_.empty()) throw Error(errLogic, "Close bracket before open");
		activeBrackets_.pop_back();
	}
	/// Sets operation to last appended leaf or last closed subtree or last openned subtree if it is empty
	void SetLastOperation(OperationType op) { container_[lastAppendedElement()].operation = op; }
	bool Empty() const { return container_.empty(); }
	size_t Size() const { return container_.size(); }
	void Reserve(size_t s) { container_.reserve(s); }
	/// @return size of leaf of subtree beginning from i
	size_t Size(size_t i) const {
		assert(i < Size());
		return container_[i].Size();
	}
	/// @return beginning of next children of the same parent
	size_t Next(size_t i) const {
		assert(i < Size());
		return i + Size(i);
	}
	OperationType GetOperation(size_t i) const {
		assert(i < Size());
		return container_[i].operation;
	}
	void SetOperation(OperationType op, size_t i) {
		assert(i < Size());
		container_[i].operation = op;
	}
	bool IsValue(size_t i) const {
		assert(i < container_.size());
		return container_[i].IsLeaf();
	}
	void Erase(size_t from, size_t to) {
		size_t count = to - from;
		for (size_t i = 0; i < from; ++i) {
			if (container_[i].IsSubTree()) {
				if (Next(i) >= to) {
					container_[i].Erase(count);
				} else {
					assert(Next(i) <= from);
				}
			}
		}
		container_.erase(container_.begin() + from, container_.begin() + to);
	}
	/// Execute appropriate functor depending on content type for each node, skip if no appropriate functor
	template <typename... Args>
	void ExecuteAppropriateForEach(const std::function<void(const Args&)>&... funcs) const {
		const Visitor<void, const Args&...> visitor{funcs...};
		for (const Node& node : container_) mpark::visit(visitor, node.storage_);
	}
	/// Execute appropriate functor depending on content type for each node, skip if no appropriate functor
	template <typename... Args>
	void ExecuteAppropriateForEach(const std::function<void(Args&)>&... funcs) {
		const Visitor<void, Args&...> visitor{funcs...};
		for (Node& node : container_) mpark::visit(visitor, node.storage_);
	}

	/// @class const_iterator
	/// iterates between children of the same parent
	class const_iterator {
	public:
		const_iterator(typename Container::const_iterator it) : it_(it) {}
		bool operator==(const const_iterator& other) const { return it_ == other.it_; }
		bool operator!=(const const_iterator& other) const { return !operator==(other); }
		const Node& operator*() const { return *it_; }
		const Node* operator->() const { return &*it_; }
		const_iterator& operator++() {
			it_ += it_->Size();
			return *this;
		}
		const_iterator cbegin() const {
			assert(it_->IsSubTree());
			return it_ + 1;
		}
		const_iterator begin() const { return cbegin(); }
		const_iterator cend() const {
			assert(it_->IsSubTree());
			return it_ + it_->Size();
		}
		const_iterator end() const { return cend(); }
		typename Container::const_iterator PlainIterator() const { return it_; }

	private:
		typename Container::const_iterator it_;
	};

	/// @class iterator
	/// iterates between children of the same parent
	class iterator {
	public:
		iterator(typename Container::iterator it) : it_(it) {}
		bool operator==(const iterator& other) const { return it_ == other.it_; }
		bool operator!=(const iterator& other) const { return !operator==(other); }
		Node& operator*() const { return *it_; }
		Node* operator->() const { return &*it_; }
		iterator& operator++() {
			it_ += it_->Size();
			return *this;
		}
		operator const_iterator() const { return const_iterator(it_); }
		iterator begin() const {
			assert(it_->IsSubTree());
			return it_ + 1;
		}
		const_iterator cbegin() const { return begin(); }
		iterator end() const {
			assert(it_->IsSubTree());
			return it_ + it_->Size();
		}
		const_iterator cend() const { return end(); }
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
	/// @return iterator to first entry of current bracket
	const_iterator begin_of_current_bracket() const {
		if (activeBrackets_.empty()) return container_.cbegin();
		return container_.cbegin() + activeBrackets_.back() + 1;
	}

protected:
	Container container_;
	/// stack of openned brackets (beginnigs of subtrees)
	h_vector<unsigned, 2> activeBrackets_;
	void clear() {
		container_.clear();
		activeBrackets_.clear();
	}

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

	void append(const_iterator begin, const_iterator end) {
		for (; begin != end; ++begin) {
			if (begin->IsLeaf()) {
				Append(begin->operation, begin->Value());
			} else {
				OpenBracket(begin->operation);
				mpark::get<SubTree>(container_.back().storage_).CopyPayloadFrom(mpark::get<SubTree>(begin->storage_));
				append(begin.cbegin(), begin.cend());
				CloseBracket();
			}
		}
	}

	void lazyAppend(iterator begin, iterator end) {
		for (; begin != end; ++begin) {
			if (begin->IsLeaf()) {
				Append(begin->operation, Ref<Head<Ts...>>{begin->Value()});
			} else {
				OpenBracket(begin->operation);
				mpark::get<SubTree>(container_.back().storage_).CopyPayloadFrom(mpark::get<SubTree>(begin->storage_));
				lazyAppend(begin.begin(), begin.end());
				CloseBracket();
			}
		}
	}

	ExpressionTree makeLazyCopy() & {
		ExpressionTree result;
		result.container_.reserve(container_.size());
		for (Node& n : container_) result.container_.emplace_back(n.MakeLazyCopy());
		result.activeBrackets_ = activeBrackets_;
		return result;
	}
};

}  // namespace reindexer
