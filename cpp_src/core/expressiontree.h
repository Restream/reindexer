#pragma once

#include <functional>
#include <variant>
#include "estl/h_vector.h"
#include "tools/errors.h"

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
		assertrx(size_ > length);
		size_ -= length;
	}
	void CopyPayloadFrom(const Bracket&) const noexcept {}
	bool operator==(const Bracket& other) const noexcept { return size_ == other.size_; }

private:
	/// size of all children + 1
	size_t size_ = 1;
};

template <typename, typename...>
struct Skip {};

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

	template <typename R, typename T, typename... TT>
	struct OverloadResolutionHelper : private OverloadResolutionHelper<R, TT...> {
		constexpr static T resolve(std::function<R(T&)>) noexcept;
		using OverloadResolutionHelper<R, TT...>::resolve;
	};
	template <typename R, typename T>
	struct OverloadResolutionHelper<R, T> {
		constexpr static T resolve(std::function<R(T&)>) noexcept;
	};
	template <typename R>
	struct OverloadResolver : private OverloadResolutionHelper<R, SubTree, Ts...> {
		using OverloadResolutionHelper<R, SubTree, Ts...>::resolve;
	};

	struct TemplateTestStruct {};
	template <typename R, typename F, bool = std::is_invocable_v<F, TemplateTestStruct&>>
	class VisitorHelperImpl;
	template <typename R, typename F>
	class VisitorHelperImpl<R, F, false> {
		using Arg = decltype(OverloadResolver<R>::resolve(std::declval<F>()));

	public:
		VisitorHelperImpl(F&& f) noexcept(noexcept(F{std::forward<F>(f)})) : functor_{std::forward<F>(f)} {}
		R operator()(const Arg& arg) const noexcept(noexcept(std::declval<F>()(arg))) { return functor_(arg); }
		R operator()(Arg& arg) const noexcept(noexcept(std::declval<F>()(arg))) { return functor_(arg); }
		R operator()(Arg&& arg) const noexcept(noexcept(std::declval<F>()(std::forward<Arg>(arg)))) {
			return functor_(std::forward<Arg>(arg));
		}
		R operator()(const Ref<Arg>& arg) const noexcept(noexcept(std::declval<F>()(arg))) { return functor_(arg); }
		R operator()(Ref<Arg>& arg) const noexcept(noexcept(std::declval<F>()(arg))) { return functor_(arg); }
		R operator()(Ref<Arg>&& arg) const noexcept(noexcept(std::declval<F>()(std::forward<Ref<Arg>>(arg)))) {
			return functor_(std::forward<Ref<Arg>>(arg));
		}

	private:
		F functor_;
	};
	template <typename R, typename F>
	class VisitorHelperImpl<R, F, true> {
	public:
		VisitorHelperImpl(F&& f) noexcept(noexcept(F{std::forward<F>(f)})) : functor_{std::forward<F>(f)} {}
		template <typename Arg>
		R operator()(const Arg& arg) const noexcept(noexcept(std::declval<F>()(arg))) {
			return functor_(arg);
		}
		template <typename Arg>
		R operator()(Arg& arg) const noexcept(noexcept(std::declval<F>()(arg))) {
			return functor_(arg);
		}
		template <typename Arg>
		R operator()(Arg&& arg) const noexcept(noexcept(std::declval<F>()(std::forward<Arg>(arg)))) {
			return functor_(std::forward<Arg>(arg));
		}
		template <typename Arg>
		R operator()(const Ref<Arg>& arg) const noexcept(noexcept(std::declval<F>()(arg))) {
			return functor_(arg);
		}
		template <typename Arg>
		R operator()(Ref<Arg>& arg) const noexcept(noexcept(std::declval<F>()(arg))) {
			return functor_(arg);
		}
		template <typename Arg>
		R operator()(Ref<Arg>&& arg) const noexcept(noexcept(std::declval<F>()(std::forward<Ref<Arg>>(arg)))) {
			return functor_(std::forward<Ref<Arg>>(arg));
		}

	private:
		F functor_;
	};

	template <typename R, typename F, typename... Fs>
	class VisitorHelper : private VisitorHelperImpl<R, F>, private VisitorHelper<R, Fs...> {
	public:
		VisitorHelper(F&& f, Fs&&... fs) noexcept(noexcept(VisitorHelperImpl<R, F>{std::forward<F>(f)}) && noexcept(VisitorHelper<R, Fs...>{
			std::forward<Fs>(fs)...}))
			: VisitorHelperImpl<R, F>{std::forward<F>(f)}, VisitorHelper<R, Fs...>{std::forward<Fs>(fs)...} {}
		using VisitorHelperImpl<R, F>::operator();
		using VisitorHelper<R, Fs...>::operator();
	};
	template <typename R, typename F>
	class VisitorHelper<R, F> : private VisitorHelperImpl<R, F> {
	public:
		VisitorHelper(F&& f) noexcept(noexcept(VisitorHelperImpl<R, F>{std::forward<F>(f)}))
			: VisitorHelperImpl<R, F>{std::forward<F>(f)} {}
		using VisitorHelperImpl<R, F>::operator();
	};
	template <typename T, typename... TT>
	class SkipHelper : private SkipHelper<TT...> {
	public:
		using SkipHelper<TT...>::operator();
		void operator()(const T&) const noexcept {}
		void operator()(T&) const noexcept {}
		void operator()(T&&) const noexcept {}
		void operator()(const Ref<T>&) const noexcept {}
		void operator()(Ref<T>&) const noexcept {}
		void operator()(Ref<T>&&) const noexcept {}
	};
	template <typename T>
	class SkipHelper<T> {
	public:
		void operator()(const T&) const noexcept {}
		void operator()(T&) const noexcept {}
		void operator()(T&&) const noexcept {}
		void operator()(const Ref<T>&) const noexcept {}
		void operator()(Ref<T>&) const noexcept {}
		void operator()(Ref<T>&&) const noexcept {}
	};
	template <typename... TT, typename... Fs>
	class VisitorHelper<void, Skip<TT...>, Fs...> : private SkipHelper<TT...>, private VisitorHelper<void, Fs...> {
	public:
		VisitorHelper(Skip<TT...>, Fs&&... fs) noexcept(noexcept(VisitorHelper<void, Fs...>{std::forward<Fs>(fs)...}))
			: VisitorHelper<void, Fs...>{std::forward<Fs>(fs)...} {}
		using SkipHelper<TT...>::operator();
		using VisitorHelper<void, Fs...>::operator();
	};
	template <typename... TT>
	class VisitorHelper<void, Skip<TT...>> : private SkipHelper<TT...> {
	public:
		VisitorHelper(Skip<TT...>) noexcept {}
		using SkipHelper<TT...>::operator();
	};

	template <typename R, typename... Fs>
	class Visitor : private VisitorHelper<R, Fs...> {
	public:
		Visitor(Fs&&... fs) noexcept(noexcept(VisitorHelper<R, Fs...>{std::forward<Fs>(fs)...}))
			: VisitorHelper<R, Fs...>{std::forward<Fs>(fs)...} {}
		using VisitorHelper<R, Fs...>::operator();
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

		using Storage = std::variant<SubTree, Ts..., Ref<Ts>...>;

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
				assertrx(0);
				abort();
			}
		};
		template <typename T>
		struct GetVisitor<T const> {
			const T& operator()(const T& v) const noexcept { return v; }
			const T& operator()(const Ref<T>& r) const noexcept { return r; }
			template <typename U>
			const T& operator()(const U&) const noexcept {
				assertrx(0);
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
				return std::forward<T>(v);
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
			return operation == other.operation && std::visit(visitor, storage_, other.storage_);
		}
		bool operator!=(const Node& other) const { return !operator==(other); }

		template <typename T>
		T& Value() {
			const static GetVisitor<T> visitor;
			return std::visit(visitor, storage_);
		}
		template <typename T>
		const T& Value() const {
			const static GetVisitor<const T> visitor;
			return std::visit(visitor, storage_);
		}
		size_t Size() const noexcept {
			static constexpr SizeVisitor sizeVisitor;
			return std::visit(sizeVisitor, storage_);
		}
		bool IsSubTree() const noexcept { return storage_.index() == 0; }
		bool IsLeaf() const noexcept { return !IsSubTree(); }
		template <typename T>
		bool Holds() const noexcept {
			return std::holds_alternative<T>(storage_);
		}
		template <typename T>
		bool HoldsOrReferTo() const noexcept {
			return std::holds_alternative<T>(storage_) || std::holds_alternative<Ref<T>>(storage_);
		}
		void Append() { std::get<SubTree>(storage_).Append(); }
		void Erase(size_t length) { std::get<SubTree>(storage_).Erase(length); }
		/// Execute appropriate functor depending on content type
		template <typename R, typename... Fs>
		R InvokeAppropriate(Fs&&... funcs) {
			return std::visit(Visitor<R, Fs...>{std::forward<Fs>(funcs)...}, storage_);
		}
		template <typename R, typename... Fs>
		R InvokeAppropriate(Fs&&... funcs) const {
			return std::visit(Visitor<R, Fs...>{std::forward<Fs>(funcs)...}, storage_);
		}

		Node MakeLazyCopy() & {
			static const LazyCopyVisitor visitor;
			return {operation, std::visit(visitor, storage_)};
		}
		Node MakeDeepCopy() const& {
			static const DeepCopyVisitor visitor;
			return {operation, std::visit(visitor, storage_)};
		}
		Node MakeDeepCopy() && {
			static const DeepRValueCopyVisitor visitor;
			return {operation, std::visit(visitor, std::move(storage_))};
		}
		bool IsRef() const {
			static const IsRefVisitor visitor;
			return std::visit(visitor, storage_);
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
	bool operator==(const ExpressionTree& other) const noexcept {
		if (container_.size() != other.container_.size()) return false;
		for (size_t i = 0; i < container_.size(); ++i) {
			if (container_[i] != other.container_[i]) return false;
		}
		return true;
	}
	bool operator!=(const ExpressionTree& other) const noexcept { return !operator==(other); }

	/// Insert value at the position
	template <typename T>
	void Insert(size_t pos, OperationType op, T&& v) {
		assertrx(pos < container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx(b < container_.size());
			if (b >= pos) ++b;
		}
		for (size_t i = 0; i < pos; ++i) {
			if (container_[i].IsSubTree() && Next(i) > pos) container_[i].Append();
		}
		container_.emplace(container_.begin() + pos, op, std::forward<T>(v));
	}
	/// Insert value after the position
	template <typename T>
	void InsertAfter(size_t pos, OperationType op, T&& v) {
		assertrx(pos < container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx(b < container_.size());
			if (b > pos) ++b;
		}
		for (size_t i = 0; i < pos; ++i) {
			if (container_[i].IsSubTree() && Next(i) > pos) container_[i].Append();
		}
		container_.emplace(container_.begin() + pos + 1, op, std::forward<T>(v));
	}
	/// Appends value to the last openned subtree
	template <typename T>
	void Append(OperationType op, T&& v) {
		for (unsigned i : activeBrackets_) {
			assertrx(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, std::forward<T>(v));
	}
	/// Appends value to the last openned subtree
	template <typename T>
	void Append(OperationType op, const T& v) {
		for (unsigned i : activeBrackets_) {
			assertrx(i < container_.size());
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
		container_.emplace(container_.begin(), op, std::forward<T>(v));
	}
	/// Enclose area in brackets
	template <typename... Args>
	void EncloseInBracket(size_t from, size_t to, OperationType op, Args&&... args) {
		assertrx(to > from);
		assertrx(to <= container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx(b < container_.size());
			if (b >= from) ++b;
		}
		for (size_t i = 0; i < from; ++i) {
			if (container_[i].IsSubTree()) {
				const auto bracketEnd = Next(i);
				if (bracketEnd >= to) {
					container_[i].Append();
				} else {
					assertrx(bracketEnd <= from);
				}
			}
		}
#ifndef NDEBUG
		for (size_t i = from; i < to; ++i) {
			if (container_[i].IsSubTree()) {
				assertrx(Next(i) <= to);
			}
		}
#endif
		container_.emplace(container_.begin() + from, op, to - from + 1, std::forward<Args>(args)...);
	}
	/// Creates subtree
	template <typename... Args>
	void OpenBracket(OperationType op, Args&&... args) {
		for (unsigned i : activeBrackets_) {
			assertrx(i < container_.size());
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
	bool Empty() const noexcept { return container_.empty(); }
	size_t Size() const noexcept { return container_.size(); }
	void Reserve(size_t s) { container_.reserve(s); }
	/// @return size of leaf of subtree beginning from i
	size_t Size(size_t i) const noexcept {
		assertrx(i < Size());
		return container_[i].Size();
	}
	/// @return beginning of next children of the same parent
	size_t Next(size_t i) const noexcept {
		assertrx(i < Size());
		return i + Size(i);
	}
	template <typename T>
	bool HoldsOrReferTo(size_t i) const noexcept {
		assertrx(i < Size());
		return container_[i].template HoldsOrReferTo<T>();
	}
	bool IsSubTree(size_t i) const noexcept {
		assertrx(i < Size());
		return container_[i].IsSubTree();
	}
	OperationType GetOperation(size_t i) const noexcept {
		assertrx(i < Size());
		return container_[i].operation;
	}
	void SetOperation(OperationType op, size_t i) noexcept {
		assertrx(i < Size());
		container_[i].operation = op;
	}
	template <typename T>
	T& Get(size_t i) {
		assertrx(i < Size());
		return container_[i].template Value<T>();
	}
	template <typename T>
	const T& Get(size_t i) const {
		assertrx(i < Size());
		return container_[i].template Value<T>();
	}
	template <typename T>
	void SetValue(size_t i, T&& v) {
		assertrx(i < Size());
		return container_[i].template SetValue<T>(std::forward<T>(v));
	}
	void Erase(size_t from, size_t to) {
		assertrx(to >= from);
		const size_t count = to - from;
		for (size_t i = 0; i < from; ++i) {
			if (container_[i].IsSubTree()) {
				if (Next(i) >= to) {
					container_[i].Erase(count);
				} else {
					assertrx(Next(i) <= from);
				}
			}
		}
		container_.erase(container_.begin() + from, container_.begin() + to);
		activeBrackets_.erase(
			std::remove_if(activeBrackets_.begin(), activeBrackets_.end(), [from, to](size_t b) { return b >= from && b < to; }),
			activeBrackets_.end());
		for (auto& b : activeBrackets_) {
			if (b >= to) b -= count;
		}
	}
	/// Execute appropriate functor depending on content type
	template <typename R, typename... Fs>
	R InvokeAppropriate(size_t i, Fs&&... funcs) {
		assertrx(i < container_.size());
		return container_[i].template InvokeAppropriate<R, Fs...>(std::forward<Fs>(funcs)...);
	}
	template <typename R, typename... Fs>
	R InvokeAppropriate(size_t i, Fs&&... funcs) const {
		assertrx(i < container_.size());
		return container_[i].template InvokeAppropriate<R, Fs...>(std::forward<Fs>(funcs)...);
	}
	/// Execute appropriate functor depending on content type for each node, skip if no appropriate functor
	template <typename... Fs>
	void ExecuteAppropriateForEach(Fs&&... funcs) const {
		const Visitor<void, Fs...> visitor{std::forward<Fs>(funcs)...};
		for (const Node& node : container_) std::visit(visitor, node.storage_);
	}
	/// Execute appropriate functor depending on content type for each node, skip if no appropriate functor
	template <typename... Fs>
	void ExecuteAppropriateForEach(Fs&&... funcs) {
		const Visitor<void, Fs...> visitor{std::forward<Fs>(funcs)...};
		for (Node& node : container_) std::visit(visitor, node.storage_);
	}

	/// @class const_iterator
	/// iterates between children of the same parent
	class const_iterator {
	public:
		const_iterator(typename Container::const_iterator it) noexcept : it_(it) {}
		bool operator==(const const_iterator& other) const noexcept { return it_ == other.it_; }
		bool operator!=(const const_iterator& other) const noexcept { return !operator==(other); }
		const Node& operator*() const noexcept { return *it_; }
		const Node* operator->() const noexcept { return &*it_; }
		const_iterator& operator++() noexcept {
			it_ += it_->Size();
			return *this;
		}
		const_iterator cbegin() const noexcept {
			assertrx(it_->IsSubTree());
			return it_ + 1;
		}
		const_iterator begin() const noexcept { return cbegin(); }
		const_iterator cend() const noexcept {
			assertrx(it_->IsSubTree());
			return it_ + it_->Size();
		}
		const_iterator end() const noexcept { return cend(); }
		typename Container::const_iterator PlainIterator() const noexcept { return it_; }

	private:
		typename Container::const_iterator it_;
	};

	/// @class iterator
	/// iterates between children of the same parent
	class iterator {
	public:
		iterator(typename Container::iterator it) noexcept : it_(it) {}
		bool operator==(const iterator& other) const noexcept { return it_ == other.it_; }
		bool operator!=(const iterator& other) const noexcept { return !operator==(other); }
		Node& operator*() const noexcept { return *it_; }
		Node* operator->() const noexcept { return &*it_; }
		iterator& operator++() noexcept {
			it_ += it_->Size();
			return *this;
		}
		operator const_iterator() const noexcept { return const_iterator(it_); }
		iterator begin() const noexcept {
			assertrx(it_->IsSubTree());
			return it_ + 1;
		}
		const_iterator cbegin() const noexcept { return begin(); }
		iterator end() const noexcept {
			assertrx(it_->IsSubTree());
			return it_ + it_->Size();
		}
		const_iterator cend() const noexcept { return end(); }
		typename Container::iterator PlainIterator() const noexcept { return it_; }

	private:
		typename Container::iterator it_;
	};

	/// @return iterator points to the first child of root
	iterator begin() noexcept { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	const_iterator begin() const noexcept { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	const_iterator cbegin() const noexcept { return {container_.begin()}; }
	/// @return iterator points to the node after the last child of root
	iterator end() noexcept { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	const_iterator end() const noexcept { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	const_iterator cend() const noexcept { return {container_.end()}; }
	/// @return iterator to first entry of current bracket
	const_iterator begin_of_current_bracket() const noexcept {
		if (activeBrackets_.empty()) return container_.cbegin();
		return container_.cbegin() + activeBrackets_.back() + 1;
	}

	const SubTree* LastOpenBracket() const {
		if (activeBrackets_.empty()) return nullptr;
		return &container_[activeBrackets_.back()].template Value<SubTree>();
	}
	SubTree* LastOpenBracket() {
		if (activeBrackets_.empty()) return nullptr;
		return &container_[activeBrackets_.back()].template Value<SubTree>();
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
	size_t lastAppendedElement() const noexcept {
		assertrx(!container_.empty());
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
			const OpType op = begin->operation;
			begin->template InvokeAppropriate<void>(
				[this, &begin, op](const SubTree& b) {
					OpenBracket(op);
					std::get<SubTree>(container_.back().storage_).CopyPayloadFrom(b);
					append(begin.cbegin(), begin.cend());
					CloseBracket();
				},
				[this, op](const auto& v) -> void { this->Append(op, v); });
		}
	}

	void lazyAppend(iterator begin, iterator end) {
		for (; begin != end; ++begin) {
			const OpType op = begin->operation;
			begin->template InvokeAppropriate<void>(
				[this, &begin, op](const SubTree& b) {
					OpenBracket(op);
					std::get<SubTree>(container_.back().storage_).CopyPayloadFrom(b);
					lazyAppend(begin.begin(), begin.end());
					CloseBracket();
				},
				[this, op](auto& v) -> void { this->Append(op, Ref<std::decay_t<decltype(v)>>{v}); });
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
