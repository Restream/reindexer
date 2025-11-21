#pragma once

#include <variant>
#include "enums.h"
#include "estl/h_vector.h"
#include "estl/overloaded.h"
#include "estl/types_pack.h"
#include "tools/errors.h"

namespace reindexer {

/// @class Bracket
/// A beginning of subtree, all children are placed just behind it
/// contains size of space occupied by all children + 1 for this node
class [[nodiscard]] Bracket {
public:
	explicit Bracket(size_t s) noexcept : size_(s) {}
	RX_ALWAYS_INLINE size_t Size() const noexcept { return size_; }
	/// Increase space occupied by children
	RX_ALWAYS_INLINE void Append() noexcept { ++size_; }
	/// Decrease space occupied by children
	RX_ALWAYS_INLINE void Erase(size_t length) noexcept {
		assertrx_dbg(size_ > length);
		size_ -= length;
	}
	RX_ALWAYS_INLINE void CopyPayloadFrom(const Bracket&) const noexcept {}
	RX_ALWAYS_INLINE bool operator==(const Bracket& other) const noexcept { return size_ == other.size_; }

private:
	/// size of all children + 1
	size_t size_ = 1;
};

template <typename T, typename... Ts>
class [[nodiscard]] Skip : private Skip<Ts...> {
public:
	using Skip<Ts...>::operator();
	RX_ALWAYS_INLINE void operator()(const T&) const noexcept {}
};

template <typename T>
class [[nodiscard]] Skip<T> {
public:
	RX_ALWAYS_INLINE void operator()(const T&) const noexcept {}
};

template <class... Ts, class... Us>
class [[nodiscard]] Skip<TypesPack<Ts...>, Us...> : public Skip<Ts..., Us...> {};

template <class... Ts>
class [[nodiscard]] Skip<TypesPack<Ts...>> : public Skip<Ts...> {};

template <template <typename> typename Templ, typename... Ts>
using SkipTemplate = Skip<Templ<Ts>...>;

/// @class ExpressionTree
/// A tree contained in vector
/// For detailed documentation see expressiontree.md
template <typename OperationType, typename SubTree, int holdSize, typename... Ts>
class [[nodiscard]] ExpressionTree {
	template <typename T, typename...>
	struct [[nodiscard]] Head_t {
		using type = T;
	};
	template <typename... Args>
	using Head = typename Head_t<Args...>::type;

	/// @class Node
	class [[nodiscard]] Node {
		friend ExpressionTree;

		using Storage = std::variant<SubTree, Ts...>;

		struct [[nodiscard]] SizeVisitor {
			template <typename T>
			RX_ALWAYS_INLINE size_t operator()(const T&) const noexcept {
				return 1;
			}
			RX_ALWAYS_INLINE size_t operator()(const SubTree& subTree) const noexcept { return subTree.Size(); }
		};

		template <typename T>
		struct [[nodiscard]] GetVisitor {
			RX_ALWAYS_INLINE T& operator()(T& v) const noexcept { return v; }
			template <typename U>
			RX_ALWAYS_INLINE T& operator()(U&) const noexcept {
				assertrx(0);
				abort();
			}
		};
		template <typename T>
		struct [[nodiscard]] GetVisitor<const T> {
			RX_ALWAYS_INLINE const T& operator()(const T& v) const noexcept { return v; }
			template <typename U>
			RX_ALWAYS_INLINE const T& operator()(const U&) const noexcept {
				assertrx_dbg(0);
				abort();
			}
		};
		struct [[nodiscard]] EqVisitor {
			template <typename T>
			RX_ALWAYS_INLINE bool operator()(const T& lhs, const T& rhs) const noexcept(noexcept(lhs == rhs)) {
				return lhs == rhs;
			}
			template <typename T, typename U>
			RX_ALWAYS_INLINE bool operator()(const T&, const U&) const noexcept {
				return false;
			}
		};
		struct [[nodiscard]] CopyVisitor {
			RX_ALWAYS_INLINE Storage operator()(const SubTree& st) const noexcept { return st; }
			template <typename T>
			RX_ALWAYS_INLINE Storage operator()(const T& v) const {
				return v;
			}
		};
		struct [[nodiscard]] MoveVisitor {
			RX_ALWAYS_INLINE Storage operator()(SubTree&& st) const noexcept { return std::move(st); }
			template <typename T>
			RX_ALWAYS_INLINE Storage operator()(T&& v) const {
				return std::forward<T>(v);
			}
		};

	public:
		Node() : storage_{std::in_place_type<SubTree>, 1} {}
		template <typename... Args>
		Node(OperationType op, size_t s, Args&&... args)
			: storage_{std::in_place_type<SubTree>, s, std::forward<Args>(args)...}, operation{op} {}
		template <typename T>
		Node(OperationType op, T&& v) : storage_{std::forward<T>(v)}, operation{op} {}
		Node(const Node& other) : storage_{other.storage_}, operation{other.operation} {}
		Node(Node&& other) noexcept : storage_{std::move(other.storage_)}, operation{std::move(other.operation)} {}
		~Node() = default;
		RX_ALWAYS_INLINE Node& operator=(const Node& other) {
			if (this != &other) {
				storage_ = other.storage_;
				operation = other.operation;
			}
			return *this;
		}
		RX_ALWAYS_INLINE Node& operator=(Node&& other) noexcept {
			if (this != &other) {
				storage_ = std::move(other.storage_);
				operation = std::move(other.operation);
			}
			return *this;
		}
		RX_ALWAYS_INLINE bool operator==(const Node& other) const {
			static const EqVisitor visitor;
			return operation == other.operation && std::visit(visitor, storage_, other.storage_);
		}
		RX_ALWAYS_INLINE bool operator!=(const Node& other) const { return !operator==(other); }

		template <typename T>
		RX_ALWAYS_INLINE T& Value() {
			const static GetVisitor<T> visitor;
			return visit(visitor);
		}
		template <typename T>
		RX_ALWAYS_INLINE const T& Value() const {
			const static GetVisitor<const T> visitor;
			return visit(visitor);
		}
		RX_ALWAYS_INLINE size_t Size() const noexcept {
			static constexpr SizeVisitor sizeVisitor;
			return visit(sizeVisitor);
		}
		RX_ALWAYS_INLINE bool IsSubTree() const noexcept { return storage_.index() == 0; }
		template <typename T>
		RX_ALWAYS_INLINE bool Is() const noexcept {
			return std::holds_alternative<T>(storage_);
		}
		RX_ALWAYS_INLINE void Append() { std::get<SubTree>(storage_).Append(); }
		RX_ALWAYS_INLINE void Erase(size_t length) { std::get<SubTree>(storage_).Erase(length); }
		/// Execute appropriate functor depending on content type
		template <typename Visitor>
		RX_ALWAYS_INLINE decltype(auto) Visit(Visitor&& visitor) {
			return visit(std::forward<Visitor>(visitor));
		}
		template <typename Visitor>
		RX_ALWAYS_INLINE decltype(auto) Visit(Visitor&& visitor) const {
			return visit(std::forward<Visitor>(visitor));
		}
		template <typename... Fs>
		RX_ALWAYS_INLINE decltype(auto) Visit(Fs&&... fs) {
			return visit(overloaded{std::forward<Fs>(fs)...});
		}
		template <typename... Fs>
		RX_ALWAYS_INLINE decltype(auto) Visit(Fs&&... fs) const {
			return visit(overloaded{std::forward<Fs>(fs)...});
		}

		RX_ALWAYS_INLINE Node Copy() const& {
			static const CopyVisitor visitor;
			return {operation, visit(visitor)};
		}
		RX_ALWAYS_INLINE Node Move() && {
			static const MoveVisitor visitor;
			return {operation, std::move(*this).visit(visitor)};
		}
		template <typename T>
		RX_ALWAYS_INLINE void SetValue(T&& v) {
			storage_ = std::forward<T>(v);
		}
		template <typename T, typename... Args>
		RX_ALWAYS_INLINE void Emplace(Args&&... args) {
			storage_.template emplace<T>(std::forward<Args>(args)...);
		}

	private:
		constexpr static size_t VarSize = std::variant_size_v<Storage>;
		template <typename Visitor>
		RX_ALWAYS_INLINE decltype(auto) visit(Visitor&& visitor) const& {
			static_assert(VarSize <= 30);
			switch (storage_.index()) {
				case 0:
					return std::forward<Visitor>(visitor)(*std::get_if<0>(&storage_));
				case 1:
					return std::forward<Visitor>(visitor)(*std::get_if<1>(&storage_));
				case 2:
					return std::forward<Visitor>(visitor)(*std::get_if<2>(&storage_));
				case 3:
					return std::forward<Visitor>(visitor)(*std::get_if<3>(&storage_));
				case 4:
					return std::forward<Visitor>(visitor)(*std::get_if<4>(&storage_));
				case 5:
					if constexpr (VarSize > 5) {
						return std::forward<Visitor>(visitor)(*std::get_if<5>(&storage_));
					}
				case 6:
					if constexpr (VarSize > 6) {
						return std::forward<Visitor>(visitor)(*std::get_if<6>(&storage_));
					}
				case 7:
					if constexpr (VarSize > 7) {
						return std::forward<Visitor>(visitor)(*std::get_if<7>(&storage_));
					}
				case 8:
					if constexpr (VarSize > 8) {
						return std::forward<Visitor>(visitor)(*std::get_if<8>(&storage_));
					}
				case 9:
					if constexpr (VarSize > 9) {
						return std::forward<Visitor>(visitor)(*std::get_if<9>(&storage_));
					}
				case 10:
					if constexpr (VarSize > 10) {
						return std::forward<Visitor>(visitor)(*std::get_if<10>(&storage_));
					}
				case 11:
					if constexpr (VarSize > 11) {
						return std::forward<Visitor>(visitor)(*std::get_if<11>(&storage_));
					}
				case 12:
					if constexpr (VarSize > 12) {
						return std::forward<Visitor>(visitor)(*std::get_if<12>(&storage_));
					}
				case 13:
					if constexpr (VarSize > 13) {
						return std::forward<Visitor>(visitor)(*std::get_if<13>(&storage_));
					}
				case 14:
					if constexpr (VarSize > 14) {
						return std::forward<Visitor>(visitor)(*std::get_if<14>(&storage_));
					}
				case 15:
					if constexpr (VarSize > 15) {
						return std::forward<Visitor>(visitor)(*std::get_if<15>(&storage_));
					}
				case 16:
					if constexpr (VarSize > 16) {
						return std::forward<Visitor>(visitor)(*std::get_if<16>(&storage_));
					}
				case 17:
					if constexpr (VarSize > 17) {
						return std::forward<Visitor>(visitor)(*std::get_if<17>(&storage_));
					}
				case 18:
					if constexpr (VarSize > 18) {
						return std::forward<Visitor>(visitor)(*std::get_if<18>(&storage_));
					}
				case 19:
					if constexpr (VarSize > 19) {
						return std::forward<Visitor>(visitor)(*std::get_if<19>(&storage_));
					}
				case 20:
					if constexpr (VarSize > 20) {
						return std::forward<Visitor>(visitor)(*std::get_if<20>(&storage_));
					}
				case 21:
					if constexpr (VarSize > 21) {
						return std::forward<Visitor>(visitor)(*std::get_if<21>(&storage_));
					}
				case 22:
					if constexpr (VarSize > 22) {
						return std::forward<Visitor>(visitor)(*std::get_if<22>(&storage_));
					}
				case 23:
					if constexpr (VarSize > 23) {
						return std::forward<Visitor>(visitor)(*std::get_if<23>(&storage_));
					}
				case 24:
					if constexpr (VarSize > 24) {
						return std::forward<Visitor>(visitor)(*std::get_if<24>(&storage_));
					}
				case 25:
					if constexpr (VarSize > 25) {
						return std::forward<Visitor>(visitor)(*std::get_if<25>(&storage_));
					}
				case 26:
					if constexpr (VarSize > 26) {
						return std::forward<Visitor>(visitor)(*std::get_if<26>(&storage_));
					}
				case 27:
					if constexpr (VarSize > 27) {
						return std::forward<Visitor>(visitor)(*std::get_if<27>(&storage_));
					}
				case 28:
					if constexpr (VarSize > 28) {
						return std::forward<Visitor>(visitor)(*std::get_if<28>(&storage_));
					}
				case 29:
					if constexpr (VarSize > 29) {
						return std::forward<Visitor>(visitor)(*std::get_if<29>(&storage_));
					}
				default:
					abort();
			}
		}
		template <typename Visitor>
		RX_ALWAYS_INLINE decltype(auto) visit(Visitor&& visitor) & {
			static_assert(VarSize <= 30);
			switch (storage_.index()) {
				case 0:
					return std::forward<Visitor>(visitor)(*std::get_if<0>(&storage_));
				case 1:
					return std::forward<Visitor>(visitor)(*std::get_if<1>(&storage_));
				case 2:
					return std::forward<Visitor>(visitor)(*std::get_if<2>(&storage_));
				case 3:
					return std::forward<Visitor>(visitor)(*std::get_if<3>(&storage_));
				case 4:
					return std::forward<Visitor>(visitor)(*std::get_if<4>(&storage_));
				case 5:
					if constexpr (VarSize > 5) {
						return std::forward<Visitor>(visitor)(*std::get_if<5>(&storage_));
					}
				case 6:
					if constexpr (VarSize > 6) {
						return std::forward<Visitor>(visitor)(*std::get_if<6>(&storage_));
					}
				case 7:
					if constexpr (VarSize > 7) {
						return std::forward<Visitor>(visitor)(*std::get_if<7>(&storage_));
					}
				case 8:
					if constexpr (VarSize > 8) {
						return std::forward<Visitor>(visitor)(*std::get_if<8>(&storage_));
					}
				case 9:
					if constexpr (VarSize > 9) {
						return std::forward<Visitor>(visitor)(*std::get_if<9>(&storage_));
					}
				case 10:
					if constexpr (VarSize > 10) {
						return std::forward<Visitor>(visitor)(*std::get_if<10>(&storage_));
					}
				case 11:
					if constexpr (VarSize > 11) {
						return std::forward<Visitor>(visitor)(*std::get_if<11>(&storage_));
					}
				case 12:
					if constexpr (VarSize > 12) {
						return std::forward<Visitor>(visitor)(*std::get_if<12>(&storage_));
					}
				case 13:
					if constexpr (VarSize > 13) {
						return std::forward<Visitor>(visitor)(*std::get_if<13>(&storage_));
					}
				case 14:
					if constexpr (VarSize > 14) {
						return std::forward<Visitor>(visitor)(*std::get_if<14>(&storage_));
					}
				case 15:
					if constexpr (VarSize > 15) {
						return std::forward<Visitor>(visitor)(*std::get_if<15>(&storage_));
					}
				case 16:
					if constexpr (VarSize > 16) {
						return std::forward<Visitor>(visitor)(*std::get_if<16>(&storage_));
					}
				case 17:
					if constexpr (VarSize > 17) {
						return std::forward<Visitor>(visitor)(*std::get_if<17>(&storage_));
					}
				case 18:
					if constexpr (VarSize > 18) {
						return std::forward<Visitor>(visitor)(*std::get_if<18>(&storage_));
					}
				case 19:
					if constexpr (VarSize > 19) {
						return std::forward<Visitor>(visitor)(*std::get_if<19>(&storage_));
					}
				case 20:
					if constexpr (VarSize > 20) {
						return std::forward<Visitor>(visitor)(*std::get_if<20>(&storage_));
					}
				case 21:
					if constexpr (VarSize > 21) {
						return std::forward<Visitor>(visitor)(*std::get_if<21>(&storage_));
					}
				case 22:
					if constexpr (VarSize > 22) {
						return std::forward<Visitor>(visitor)(*std::get_if<22>(&storage_));
					}
				case 23:
					if constexpr (VarSize > 23) {
						return std::forward<Visitor>(visitor)(*std::get_if<23>(&storage_));
					}
				case 24:
					if constexpr (VarSize > 24) {
						return std::forward<Visitor>(visitor)(*std::get_if<24>(&storage_));
					}
				case 25:
					if constexpr (VarSize > 25) {
						return std::forward<Visitor>(visitor)(*std::get_if<25>(&storage_));
					}
				case 26:
					if constexpr (VarSize > 26) {
						return std::forward<Visitor>(visitor)(*std::get_if<26>(&storage_));
					}
				case 27:
					if constexpr (VarSize > 27) {
						return std::forward<Visitor>(visitor)(*std::get_if<27>(&storage_));
					}
				case 28:
					if constexpr (VarSize > 28) {
						return std::forward<Visitor>(visitor)(*std::get_if<28>(&storage_));
					}
				case 29:
					if constexpr (VarSize > 29) {
						return std::forward<Visitor>(visitor)(*std::get_if<29>(&storage_));
					}
				default:
					abort();
			}
		}
		template <typename Visitor>
		RX_ALWAYS_INLINE decltype(auto) visit(Visitor&& visitor) && {
			static_assert(VarSize <= 30);
			switch (storage_.index()) {
				case 0:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<0>(&storage_)));
				case 1:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<1>(&storage_)));
				case 2:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<2>(&storage_)));
				case 3:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<3>(&storage_)));
				case 4:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<4>(&storage_)));
				case 5:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<5>(&storage_)));
				case 6:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<6>(&storage_)));
				case 7:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<7>(&storage_)));
				case 8:
					return std::forward<Visitor>(visitor)(std::move(*std::get_if<8>(&storage_)));
				case 9:
					if constexpr (VarSize > 9) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<9>(&storage_)));
					}
				case 10:
					if constexpr (VarSize > 10) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<10>(&storage_)));
					}
				case 11:
					if constexpr (VarSize > 11) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<11>(&storage_)));
					}
				case 12:
					if constexpr (VarSize > 12) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<12>(&storage_)));
					}
				case 13:
					if constexpr (VarSize > 13) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<13>(&storage_)));
					}
				case 14:
					if constexpr (VarSize > 14) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<14>(&storage_)));
					}
				case 15:
					if constexpr (VarSize > 15) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<15>(&storage_)));
					}
				case 16:
					if constexpr (VarSize > 16) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<16>(&storage_)));
					}
				case 17:
					if constexpr (VarSize > 17) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<17>(&storage_)));
					}
				case 18:
					if constexpr (VarSize > 18) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<18>(&storage_)));
					}
				case 19:
					if constexpr (VarSize > 19) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<19>(&storage_)));
					}
				case 20:
					if constexpr (VarSize > 20) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<20>(&storage_)));
					}
				case 21:
					if constexpr (VarSize > 21) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<21>(&storage_)));
					}
				case 22:
					if constexpr (VarSize > 22) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<22>(&storage_)));
					}
				case 23:
					if constexpr (VarSize > 23) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<23>(&storage_)));
					}
				case 24:
					if constexpr (VarSize > 24) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<24>(&storage_)));
					}
				case 25:
					if constexpr (VarSize > 25) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<25>(&storage_)));
					}
				case 26:
					if constexpr (VarSize > 26) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<26>(&storage_)));
					}
				case 27:
					if constexpr (VarSize > 27) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<27>(&storage_)));
					}
				case 28:
					if constexpr (VarSize > 28) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<28>(&storage_)));
					}
				case 29:
					if constexpr (VarSize > 29) {
						return std::forward<Visitor>(visitor)(std::move(*std::get_if<29>(&storage_)));
					}
				default:
					abort();
			}
		}
		template <typename T>
		RX_ALWAYS_INLINE T& get() & noexcept {
			return *std::get_if<T>(&storage_);
		}
		template <typename T>
		RX_ALWAYS_INLINE T&& get() && noexcept {
			return std::move(*std::get_if<T>(&storage_));
		}
		template <typename T>
		RX_ALWAYS_INLINE const T& get() const& noexcept {
			return *std::get_if<T>(&storage_);
		}
		template <typename T>
		RX_ALWAYS_INLINE const T&& get() const&& noexcept {
			return std::move(*std::get_if<T>(&storage_));
		}

		Storage storage_;

	public:
		OperationType operation;
	};

protected:
	using Container = h_vector<Node, holdSize>;
	enum class [[nodiscard]] MergeResult { NotMerged, Merged, Annihilated };

private:
	template <typename T>
	class [[nodiscard]] PostProcessor {
	public:
		static constexpr bool NoOp = true;
	};

public:
	RX_ALWAYS_INLINE bool operator==(const ExpressionTree& other) const noexcept {
		if (container_.size() != other.container_.size()) {
			return false;
		}
		for (size_t i = 0; i < container_.size(); ++i) {
			if (container_[i] != other.container_[i]) {
				return false;
			}
		}
		return true;
	}

	/// Insert value at the position
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	size_t Insert(size_t pos, OperationType op, T&& v) {
		if (pos == container_.size()) {
			return Append(op, std::forward<T>(v));
		}
		size_t insertedCount = 1;
		assertrx_dbg(pos < container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx_dbg(b < container_.size());
			if (b >= pos) {
				++b;
			}
		}
		for (size_t i = 0; i < pos; ++i) {
			if (container_[i].IsSubTree() && Next(i) > pos) {
				container_[i].Append();
			}
		}
		container_.emplace(container_.begin() + pos, op, std::forward<T>(v));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, pos);
		}
		return insertedCount;
	}
	/// Emplace value at the position
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T, typename... Args>
	size_t Emplace(size_t pos, OperationType op, Args&&... args) {
		if (pos == container_.size()) {
			return Append<T>(op, std::forward<Args>(args)...);
		}
		size_t insertedCount = 1;
		assertrx_throw(pos < container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx_throw(b < container_.size());
			if (b >= pos) {
				++b;
			}
		}
		for (size_t i = 0; i < pos; ++i) {
			if (container_[i].IsSubTree() && Next(i) > pos) {
				container_[i].Append();
			}
		}
		container_.emplace(container_.begin() + pos, op, T(std::forward<Args>(args)...));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, pos);
		}
		return insertedCount;
	}
	/// Insert value after the position
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	size_t InsertAfter(size_t pos, OperationType op, T&& v) {
		size_t insertedCount = 1;
		assertrx_dbg(pos < container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx_dbg(b < container_.size());
			if (b > pos) {
				++b;
			}
		}
		for (size_t i = 0; i < pos; ++i) {
			if (container_[i].IsSubTree() && Next(i) > pos) {
				container_[i].Append();
			}
		}
		++pos;
		container_.emplace(container_.begin() + pos, op, std::forward<T>(v));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, pos);
		}
		return insertedCount;
	}
	/// Appends value to the last opened subtree
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	size_t Append(OperationType op, T&& v) {
		size_t insertedCount = 1;
		for (unsigned i : activeBrackets_) {
			assertrx_dbg(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, std::forward<T>(v));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, Size() - 1);
		}
		return insertedCount;
	}
	/// Appends value to the last opened subtree
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	size_t Append(OperationType op, const T& v) {
		size_t insertedCount = 1;
		for (unsigned i : activeBrackets_) {
			assertrx_dbg(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, v);
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, Size() - 1);
		}
		return insertedCount;
	}
	/// Appends value to the last opened subtree
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T, typename... Args>
	size_t Append(OperationType op, Args&&... args) {
		size_t insertedCount = 1;
		for (unsigned i : activeBrackets_) {
			assertrx_dbg(i < container_.size());
			container_[i].Append();
		}
		container_.emplace_back(op, T{std::forward<Args>(args)...});
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, Size() - 1);
		}
		return insertedCount;
	}
	class const_iterator;
	class iterator;
	/// Appends all nodes from the interval to the last opened subtree
	/// Always appends 'as is', i.e. without postprocessing and without implicit nodes creation
	RX_ALWAYS_INLINE void Append(const_iterator begin, const_iterator end) {
		container_.reserve(container_.size() + (end.PlainIterator() - begin.PlainIterator()));
		append(begin, end);
	}

	/// Appends value as first child of the root
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	RX_ALWAYS_INLINE size_t AppendFront(OperationType op, T&& v) {
		size_t insertedCount = 1;
		for (unsigned& i : activeBrackets_) {
			++i;
		}

		container_.emplace(container_.begin(), op, std::forward<T>(v));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, 0);
		}
		return insertedCount;
	}
	/// Appends value as first child of the root
	/// @return actual inserted nodes count - this method may add new nodes during postprocessing phase
	template <typename T, typename... Args>
	RX_ALWAYS_INLINE size_t AppendFront(OperationType op, Args&&... args) {
		size_t insertedCount = 1;
		for (unsigned& i : activeBrackets_) {
			++i;
		}
		container_.emplace(container_.begin(), op, T{std::forward<Args>(args)...});
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, 0);
		}
		return insertedCount;
	}
	/// Pop last node
	void PopBack() {
		assertrx_dbg(!container_.empty());
		for (unsigned i : activeBrackets_) {
			assertrx_dbg(i < container_.size());
			container_[i].Erase(1);
		}
		if (container_.back().IsSubTree() && !activeBrackets_.empty() && activeBrackets_.back() == container_.size() - 1) {
			activeBrackets_.pop_back();
		}
		container_.pop_back();
	}
	/// Enclose area in brackets
	template <typename... Args>
	void EncloseInBracket(size_t from, size_t to, OperationType op, Args&&... args) {
		assertrx_dbg(to > from);
		assertrx_dbg(to <= container_.size());
		for (unsigned& b : activeBrackets_) {
			assertrx_dbg(b < container_.size());
			if (b >= from) {
				++b;
			}
		}
		for (size_t i = 0; i < from; ++i) {
			if (container_[i].IsSubTree()) {
				const auto bracketEnd = Next(i);
				if (bracketEnd >= to) {
					container_[i].Append();
				} else {
					assertrx_dbg(bracketEnd <= from);
				}
			}
		}
#ifdef RX_WITH_STDLIB_DEBUG
		for (size_t i = from; i < to; ++i) {
			if (container_[i].IsSubTree()) {
				assertrx_dbg(Next(i) <= to);
			}
		}
#endif
		container_.emplace(container_.begin() + from, op, to - from + 1, std::forward<Args>(args)...);
	}
	/// Creates subtree
	template <typename... Args>
	void OpenBracket(OperationType op, Args&&... args) {
		for (unsigned i : activeBrackets_) {
			assertrx_dbg(i < container_.size());
			container_[i].Append();
		}
		activeBrackets_.push_back(container_.size());
		container_.emplace_back(op, size_t{1}, std::forward<Args>(args)...);
	}
	/// Closes last open subtree for appending
	void CloseBracket() {
		if (activeBrackets_.empty()) [[unlikely]] {
			throw Error(errLogic, "Close bracket before open");
		}
		activeBrackets_.pop_back();
	}
	/// Sets operation to last appended leaf or last closed subtree or last opened subtree if it is empty
	RX_ALWAYS_INLINE void SetLastOperation(OperationType op) { container_[LastAppendedElement()].operation = op; }
	RX_ALWAYS_INLINE bool Empty() const noexcept { return container_.empty(); }
	RX_ALWAYS_INLINE size_t Size() const noexcept { return container_.size(); }
	RX_ALWAYS_INLINE void Reserve(size_t s) { container_.reserve(s); }
	/// @return size of leaf of subtree beginning from i
	RX_ALWAYS_INLINE size_t Size(size_t i) const noexcept {
		assertrx_dbg(i < Size());
		return container_[i].Size();
	}
	/// @return beginning of next children of the same parent
	RX_ALWAYS_INLINE size_t Next(size_t i) const noexcept {
		assertrx_dbg(i < Size());
		return i + Size(i);
	}
	/// @return 'true' if type of the specified node is T
	template <typename T>
	RX_ALWAYS_INLINE bool Is(size_t i) const noexcept {
		assertrx_dbg(i < Size());
		return container_[i].template Is<T>();
	}
	/// @return 'true' if specified node is subtree (bracket)
	RX_ALWAYS_INLINE bool IsSubTree(size_t i) const noexcept {
		assertrx_dbg(i < Size());
		return container_[i].IsSubTree();
	}
	RX_ALWAYS_INLINE OperationType GetOperation(size_t i) const noexcept {
		assertrx_dbg(i < Size());
		return container_[i].operation;
	}
	RX_ALWAYS_INLINE void SetOperation(OperationType op, size_t i) noexcept {
		assertrx_dbg(i < Size());
		container_[i].operation = op;
	}
	/// @return node, casted to the specified type (T). Does not performs type check
	template <typename T>
	RX_ALWAYS_INLINE T& Get(size_t i) {
		assertrx_dbg(i < Size());
		return container_[i].template Value<T>();
	}
	/// @return node, casted to the specified type (T). Does not performs type check
	template <typename T>
	RX_ALWAYS_INLINE const T& Get(size_t i) const {
		assertrx_dbg(i < Size());
		return container_[i].template Value<T>();
	}
	/// Set new value to the target node and perform postprocessing if required
	/// @return actual inserted/changed nodes count - this method may add new nodes during postprocessing phase
	template <typename T>
	RX_ALWAYS_INLINE size_t SetValue(size_t i, T&& v) {
		size_t insertedCount = 1;
		assertrx_dbg(i < Size());
		container_[i].template SetValue<T>(std::forward<T>(v));
		if constexpr (!PostProcessor<T>::NoOp) {
			insertedCount += PostProcessor<T>::Process(*this, i);
		}
		return insertedCount;
	}
	/// Try to set new values to the target node in-place (if target type has TryUpdateInplace method).
	/// This method tries to update node content without postprocessing phase (i.e. without extra nodes creation).
	/// @return 'true' - in case of success
	template <typename T, typename U>
	bool TryUpdateInplace(size_t i, U& values) noexcept {
		if (Is<T>(i)) {
			return Get<T>(i).TryUpdateInplace(values);
		}
		return false;
	}
	/// Erase nodes range
	void Erase(size_t from, size_t to) {
		assertrx_dbg(to >= from);
		const size_t count = to - from;
		for (size_t i = 0; i < from; ++i) {
			if (container_[i].IsSubTree()) {
				if (Next(i) >= to) {
					container_[i].Erase(count);
				} else {
					assertrx_dbg(Next(i) <= from);
				}
			}
		}
		std::ignore = container_.erase(container_.begin() + from, container_.begin() + to);
		activeBrackets_.erase(
			std::remove_if(activeBrackets_.begin(), activeBrackets_.end(), [from, to](size_t b) { return b >= from && b < to; }),
			activeBrackets_.end());
		for (auto& b : activeBrackets_) {
			if (b >= to) {
				b -= count;
			}
		}
	}
	/// Visit target node with specified visitor
	template <typename Visitor>
	RX_ALWAYS_INLINE decltype(auto) Visit(size_t i, Visitor&& visitor) {
		assertrx_dbg(i < container_.size());
		return container_[i].visit(std::forward<Visitor>(visitor));
	}
	template <typename Visitor>
	RX_ALWAYS_INLINE decltype(auto) Visit(size_t i, Visitor&& visitor) const {
		assertrx_dbg(i < container_.size());
		return container_[i].visit(std::forward<Visitor>(visitor));
	}
	template <typename... Fs>
	RX_ALWAYS_INLINE decltype(auto) Visit(size_t i, Fs&&... fs) {
		assertrx_dbg(i < container_.size());
		return container_[i].visit(overloaded{std::forward<Fs>(fs)...});
	}
	template <typename... Fs>
	RX_ALWAYS_INLINE decltype(auto) Visit(size_t i, Fs&&... fs) const {
		assertrx_dbg(i < container_.size());
		return container_[i].visit(overloaded{std::forward<Fs>(fs)...});
	}
	/// Visit each node of the tree with specified visitor
	template <typename Visitor>
	RX_ALWAYS_INLINE void VisitForEach(const Visitor& visitor) const {
		for (const Node& node : container_) {
			node.visit(visitor);
		}
	}
	template <typename Visitor>
	RX_ALWAYS_INLINE void VisitForEach(const Visitor& visitor) {
		for (Node& node : container_) {
			node.visit(visitor);
		}
	}
	template <typename... Fs>
	RX_ALWAYS_INLINE void VisitForEach(Fs&&... fs) const {
		overloaded visitor{std::forward<Fs>(fs)...};
		for (const Node& node : container_) {
			node.visit(visitor);
		}
	}
	template <typename... Fs>
	RX_ALWAYS_INLINE void VisitForEach(Fs&&... fs) {
		overloaded visitor{std::forward<Fs>(fs)...};
		for (Node& node : container_) {
			node.visit(visitor);
		}
	}

	/// @class const_iterator
	/// iterates between children of the same parent
	class [[nodiscard]] const_iterator {
	public:
		using iterator_category = std::forward_iterator_tag;
		using value_type = Node;
		using difference_type = std::ptrdiff_t;
		using reference = const Node&;
		using pointer = const Node*;

		const_iterator(typename Container::const_iterator it) noexcept : it_(it) {}
		RX_ALWAYS_INLINE bool operator==(const const_iterator& other) const noexcept { return it_ == other.it_; }
		RX_ALWAYS_INLINE bool operator!=(const const_iterator& other) const noexcept { return !operator==(other); }
		RX_ALWAYS_INLINE reference operator*() const noexcept { return *it_; }
		RX_ALWAYS_INLINE pointer operator->() const noexcept { return &*it_; }
		RX_ALWAYS_INLINE const_iterator& operator++() noexcept {
			it_ += it_->Size();
			return *this;
		}
		RX_ALWAYS_INLINE const_iterator cbegin() const noexcept {
			assertrx_dbg(it_->IsSubTree());
			assertrx_dbg(it_->IsSubTree());
			return it_ + 1;
		}
		RX_ALWAYS_INLINE const_iterator begin() const noexcept { return cbegin(); }
		RX_ALWAYS_INLINE const_iterator cend() const noexcept {
			assertrx_dbg(it_->IsSubTree());
			return it_ + it_->Size();
		}
		RX_ALWAYS_INLINE const_iterator end() const noexcept { return cend(); }
		RX_ALWAYS_INLINE typename Container::const_iterator PlainIterator() const noexcept { return it_; }

	private:
		typename Container::const_iterator it_;
	};

	/// @class iterator
	/// iterates between children of the same parent
	class [[nodiscard]] iterator {
	public:
		using iterator_category = std::forward_iterator_tag;
		using value_type = Node;
		using difference_type = std::ptrdiff_t;
		using reference = Node&;
		using pointer = Node*;

		iterator(typename Container::iterator it) noexcept : it_(it) {}
		RX_ALWAYS_INLINE bool operator==(const iterator& other) const noexcept { return it_ == other.it_; }
		RX_ALWAYS_INLINE bool operator!=(const iterator& other) const noexcept { return !operator==(other); }
		RX_ALWAYS_INLINE reference operator*() const noexcept { return *it_; }
		RX_ALWAYS_INLINE pointer operator->() const noexcept { return &*it_; }
		RX_ALWAYS_INLINE iterator& operator++() noexcept {
			it_ += it_->Size();
			return *this;
		}
		RX_ALWAYS_INLINE operator const_iterator() const noexcept { return const_iterator(it_); }
		RX_ALWAYS_INLINE iterator begin() const noexcept {
			assertrx_dbg(it_->IsSubTree());
			return it_ + 1;
		}
		RX_ALWAYS_INLINE const_iterator cbegin() const noexcept { return begin(); }
		RX_ALWAYS_INLINE iterator end() const noexcept {
			assertrx_dbg(it_->IsSubTree());
			return it_ + it_->Size();
		}
		RX_ALWAYS_INLINE const_iterator cend() const noexcept { return end(); }
		RX_ALWAYS_INLINE typename Container::iterator PlainIterator() const noexcept { return it_; }

	private:
		typename Container::iterator it_;
	};

	/// @return iterator points to the first child of root
	RX_ALWAYS_INLINE iterator begin() noexcept { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	RX_ALWAYS_INLINE const_iterator begin() const noexcept { return {container_.begin()}; }
	/// @return iterator points to the first child of root
	RX_ALWAYS_INLINE const_iterator cbegin() const noexcept { return {container_.begin()}; }
	/// @return iterator points to the node after the last child of root
	RX_ALWAYS_INLINE iterator end() noexcept { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	RX_ALWAYS_INLINE const_iterator end() const noexcept { return {container_.end()}; }
	/// @return iterator points to the node after the last child of root
	RX_ALWAYS_INLINE const_iterator cend() const noexcept { return {container_.end()}; }
	/// @return iterator to first entry of current bracket
	RX_ALWAYS_INLINE const_iterator begin_of_current_bracket() const noexcept {
		if (activeBrackets_.empty()) {
			return container_.cbegin();
		}
		return container_.cbegin() + activeBrackets_.back() + 1;
	}
	/// @return pointer to the last active bracket (if exists)
	RX_ALWAYS_INLINE const SubTree* LastOpenBracket() const {
		if (activeBrackets_.empty()) {
			return nullptr;
		}
		return &container_[activeBrackets_.back()].template Value<SubTree>();
	}
	/// @return pointer to the last active bracket (if exists)
	RX_ALWAYS_INLINE SubTree* LastOpenBracket() {
		if (activeBrackets_.empty()) {
			return nullptr;
		}
		return &container_[activeBrackets_.back()].template Value<SubTree>();
	}
	/// @return the last appended leaf or last closed subtree or last opened subtree if it is empty
	size_t LastAppendedElement() const noexcept {
		assertrx_dbg(!container_.empty());
		size_t start = 0;  // start of last opened subtree;
		if (!activeBrackets_.empty()) {
			start = activeBrackets_.back() + 1;
			if (start == container_.size()) {
				return start - 1;  // last opened subtree is empty
			}
		}
		while (Next(start) != container_.size()) {
			start = Next(start);
		}
		return start;
	}
	/// Erase target node
	void Erase(iterator it) {
		assertrx_dbg(it != end());
		const auto pos = it.PlainIterator() - begin().PlainIterator();
		return Erase(pos, pos + 1);
	}

protected:
	ExpressionTree() = default;
	ExpressionTree(ExpressionTree&&) noexcept = default;
	ExpressionTree& operator=(ExpressionTree&&) noexcept = default;
	ExpressionTree(const ExpressionTree& other) : activeBrackets_{other.activeBrackets_} {
		container_.reserve(other.container_.size());
		for (const Node& n : other.container_) {
			container_.emplace_back(n.Copy());
		}
	}
	ExpressionTree& operator=(const ExpressionTree& other) {
		if (this == &other) [[unlikely]] {
			return *this;
		}
		container_.clear();
		container_.reserve(other.container_.size());
		for (const Node& n : other.container_) {
			container_.emplace_back(n.Copy());
		}
		activeBrackets_ = other.activeBrackets_;
		return *this;
	}
	Container container_;
	/// stack of opened brackets (beginnings of subtrees)
	h_vector<unsigned, 2> activeBrackets_;
	void clear() {
		container_.template clear<false>();
		activeBrackets_.template clear<false>();
	}

	void append(const_iterator begin, const_iterator end) {
		for (; begin != end; ++begin) {
			const OpType op = begin->operation;
			begin->Visit(
				[this, &begin, op](const SubTree& b) {
					OpenBracket(op);
					std::get<SubTree>(container_.back().storage_).CopyPayloadFrom(b);
					append(begin.cbegin(), begin.cend());
					CloseBracket();
				},
				[this, op](const auto& v) -> void { std::ignore = this->Append(op, v); });
		}
	}

	// MSVC 14.44/14.51 unable to build mergeEntriesImpl without this wrapper
	template <typename Merger>
	size_t mergeEntries(Merger& merger, uint16_t dst, uint16_t srcBegin, uint16_t srcEnd, Changed& changed) {
		return mergeEntriesImpl<Merger, typename Merger::SkippingEntries, typename Merger::InvalidEntries>(merger, dst, srcBegin, srcEnd,
																										   changed);
	}
	template <typename Merger, typename SkippingEntries, typename InvalidEntries>
	size_t mergeEntriesImpl(Merger&, uint16_t dst, uint16_t srcBegin, uint16_t srcEnd, Changed&);
};

}  // namespace reindexer
