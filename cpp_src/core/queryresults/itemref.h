#pragma once

#include <utility>
#include "core/enums.h"
#include "core/payload/payloadvalue.h"
#include "core/rank_t.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"

namespace reindexer {

class ItemRef {
public:
	ItemRef() noexcept : id_(0), raw_(0), valueInitialized_(false), nsid_(0) {}
	ItemRef(IdType id, const PayloadValue& value, uint16_t nsid = 0, bool raw = false) noexcept
		: id_(id), raw_(raw), valueInitialized_(true), nsid_(nsid), value_(value) {}
	ItemRef(IdType id, unsigned sortExprResultsIdx, uint16_t nsid = 0) noexcept
		: id_(id), raw_(0), valueInitialized_(false), nsid_(nsid), sortExprResultsIdx_(sortExprResultsIdx) {}
	ItemRef(ItemRef&& other) noexcept
		: id_(other.id_),
		  raw_(other.raw_),
		  valueInitialized_(other.valueInitialized_),
		  nsid_(other.nsid_),
		  sortExprResultsIdx_(other.sortExprResultsIdx_) {
		if (valueInitialized_) {
			new (&value_) PayloadValue(std::move(other.value_));
		}
	}
	ItemRef(const ItemRef& other) noexcept
		: id_(other.id_),
		  raw_(other.raw_),
		  valueInitialized_(other.valueInitialized_),
		  nsid_(other.nsid_),
		  sortExprResultsIdx_(other.sortExprResultsIdx_) {
		if (valueInitialized_) {
			new (&value_) PayloadValue(other.value_);
		}
	}
	ItemRef& operator=(ItemRef&& other) noexcept {
		if (&other == this) {
			return *this;
		}
		id_ = other.id_;
		raw_ = other.raw_;
		nsid_ = other.nsid_;
		if (valueInitialized_) {
			if (other.valueInitialized_) {
				value_ = std::move(other.value_);
			} else {
				value_.~PayloadValue();
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		} else {
			if (other.valueInitialized_) {
				new (&value_) PayloadValue(std::move(other.value_));
			} else {
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		}
		valueInitialized_ = other.valueInitialized_;
		return *this;
	}
	ItemRef& operator=(const ItemRef& other) noexcept {
		if (&other == this) {
			return *this;
		}
		id_ = other.id_;
		raw_ = other.raw_;
		nsid_ = other.nsid_;
		if (valueInitialized_) {
			if (other.valueInitialized_) {
				value_ = other.value_;
			} else {
				value_.~PayloadValue();
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		} else {
			if (other.valueInitialized_) {
				new (&value_) PayloadValue(other.value_);
			} else {
				sortExprResultsIdx_ = other.sortExprResultsIdx_;
			}
		}
		valueInitialized_ = other.valueInitialized_;
		return *this;
	}
	~ItemRef() {
		if (valueInitialized_) {
			value_.~PayloadValue();
		}
	}

	IdType Id() const noexcept { return id_; }
	uint16_t Nsid() const noexcept { return nsid_; }
	bool Raw() const noexcept { return raw_; }
	const PayloadValue& Value() const noexcept {
		assertrx(valueInitialized_);
		return value_;
	}
	PayloadValue& Value() noexcept {
		assertrx(valueInitialized_);
		return value_;
	}
	unsigned SortExprResultsIdx() const noexcept {
		assertrx(!valueInitialized_);
		return sortExprResultsIdx_;
	}
	void SetValue(PayloadValue&& value) noexcept {
		assertrx(!valueInitialized_);
		new (&value_) PayloadValue(std::move(value));
		valueInitialized_ = true;
	}
	void SetValue(const PayloadValue& value) noexcept {
		assertrx(!valueInitialized_);
		new (&value_) PayloadValue(value);
		valueInitialized_ = true;
	}
	bool ValueInitialized() const noexcept { return valueInitialized_; }

private:
	IdType id_ = 0;
	uint16_t raw_ : 1;
	uint16_t valueInitialized_ : 1;
	uint16_t nsid_ = 0;
	union {
		PayloadValue value_;
		unsigned sortExprResultsIdx_ = 0u;
	};
};

class ItemRefRanked : private ItemRef {
public:
	template <typename... Args>
	ItemRefRanked(RankT r, Args&&... args) : ItemRef{std::forward<Args>(args)...}, rank_{r} {}
	RankT Rank() const noexcept { return rank_; }
	const ItemRef& NotRanked() const& noexcept { return *this; }
	ItemRef& NotRanked() & noexcept { return *this; }
	auto NotRanked() const&& = delete;

private:
	RankT rank_;
};

class [[nodiscard]] ItemRefVariant : public std::variant<ItemRef, ItemRefRanked> {
	using Base = std::variant<ItemRef, ItemRefRanked>;

public:
	[[nodiscard]] const ItemRef& NotRanked() const& noexcept {
		return std::visit(overloaded{[](const ItemRef& v) noexcept -> const ItemRef& { return v; },
									 [](const ItemRefRanked& v) noexcept -> const ItemRef& { return v.NotRanked(); }},
						  AsVariant());
	}
	[[nodiscard]] const ItemRefRanked& Ranked() const& {
		return std::visit(
			overloaded{[](const ItemRef&) -> const ItemRefRanked& { throw Error{errLogic, "Get rank from result of not ranked query"}; },
					   [](const ItemRefRanked& r) noexcept -> const ItemRefRanked& { return r; }},
			AsVariant());
	}
	[[nodiscard]] RankT Rank() const {
		return std::visit(overloaded{[](const ItemRef&) -> RankT { throw Error{errLogic, "Get rank from result of not ranked query"}; },
									 [](const ItemRefRanked& r) noexcept { return r.Rank(); }},
						  AsVariant());
	}
	const Base& AsVariant() const& noexcept { return *this; }

	auto Ranked() const&& = delete;
	auto NotRanked() const&& = delete;
};

class [[nodiscard]] ItemRefVector {
	static constexpr size_t kDefaultQueryResultsSize = 32;

	using NotRankedVec = h_vector<ItemRef, kDefaultQueryResultsSize>;
	using RankedVec = h_vector<ItemRefRanked, kDefaultQueryResultsSize>;
	using Variant = std::variant<NotRankedVec, RankedVec>;

	template <typename It, typename RIt>
	class [[nodiscard]] IteratorImpl : private std::variant<It, RIt> {
		using Base = std::variant<It, RIt>;
		using ItemRefRef = std::remove_reference_t<typename std::iterator_traits<It>::reference>&;

		friend ItemRefVector;

	public:
		using iterator_category = std::random_access_iterator_tag;
		using RankedIt = RIt;
		using NotRankedIt = It;

		IteratorImpl(NotRankedIt it) noexcept : Base{std::in_place_index<0>, it} {}
		IteratorImpl(RankedIt it) noexcept : Base{std::in_place_index<1>, it} {}
		IteratorImpl(const IteratorImpl<NotRankedVec::iterator, RankedVec::iterator>& other) noexcept
			: Base{std::visit([](auto& it) { return Base{it}; }, other.asVariant())} {}
		IteratorImpl(IteratorImpl<NotRankedVec::iterator, RankedVec::iterator>&& other) noexcept
			: Base{std::visit([](auto& it) { return Base{std::move(it)}; }, other.asVariant())} {}
		IteratorImpl& operator=(const IteratorImpl<NotRankedVec::iterator, RankedVec::iterator>& other) noexcept {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(this->index() == other.index(), "%d %d", this->index(), other.index());
#endif	// REINDEX_CORE_BUILD
			Base::operator=(other.asVariant());
			return *this;
		}
		IteratorImpl& operator=(IteratorImpl<NotRankedVec::iterator, RankedVec::iterator>&& other) noexcept {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(this->index() == other.index(), "%d %d", this->index(), other.index());
#endif	// REINDEX_CORE_BUILD
			Base::operator=(std::move(other).asVariant());
			return *this;
		}

		IteratorImpl& operator++() & noexcept {
			std::visit([](auto& it) noexcept { ++it; }, asVariant());
			return *this;
		}
		IteratorImpl&& operator++() && noexcept { return std::move(operator++()); }
		IteratorImpl operator++(int) & noexcept {
			IteratorImpl res = *this;
			std::visit([](auto& it) noexcept { ++it; }, asVariant());
			return res;
		}

		IteratorImpl& operator--() & noexcept {
			std::visit([](auto& it) noexcept { --it; }, asVariant());
			return *this;
		}
		IteratorImpl&& operator--() && noexcept { return std::move(operator--()); }

		IteratorImpl operator--(int) & noexcept {
			IteratorImpl res = *this;
			std::visit([](auto& it) noexcept { --it; }, asVariant());
			return res;
		}

		template <typename I>
		IteratorImpl& operator+=(I i) & noexcept {
			std::visit([i](auto& it) noexcept { it += i; }, asVariant());
			return *this;
		}
		template <typename I>
		IteratorImpl&& operator+=(I i) && noexcept {
			return std::move(operator+=(i));
		}
		template <typename I>
		IteratorImpl& operator-=(I i) & noexcept {
			return operator+=(-i);
		}
		template <typename I>
		IteratorImpl&& operator-=(I i) && noexcept {
			return std::move(operator-=(i));
		}

		template <typename I>
		IteratorImpl operator+(I i) const noexcept {
			IteratorImpl res{*this};
			res += i;
			return res;
		}
		template <typename I>
		IteratorImpl operator-(I i) const noexcept {
			return operator+(-i);
		}

		std::ptrdiff_t operator-(IteratorImpl other) const noexcept {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(this->index() == other.index(), "%d %d", this->index(), other.index());
#endif	// REINDEX_CORE_BUILD
			return std::visit([other](auto lhs) noexcept { return lhs - std::get<decltype(lhs)>(other); }, asVariant());
		}

		template <typename I, typename IR>
		[[nodiscard]] bool operator==(IteratorImpl<I, IR> other) const noexcept {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(this->index() == other.index(), "%d %d", this->index(), other.index());
#endif	// REINDEX_CORE_BUILD
			return std::visit([other](auto lhs) noexcept { return lhs == std::get<decltype(lhs)>(other); }, asVariant());
		}
		template <typename I, typename IR>
		[[nodiscard]] bool operator!=(IteratorImpl<I, IR> other) const noexcept {
			return !operator==(other);
		}
		template <typename I, typename IR>
		[[nodiscard]] bool operator<(IteratorImpl<I, IR> other) const noexcept {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(this->index() == other.index(), "%d %d", this->index(), other.index());
#endif	// REINDEX_CORE_BUILD
			return std::visit([other](auto lhs) noexcept { return lhs < std::get<decltype(lhs)>(other); }, asVariant());
		}
		template <typename I, typename IR>
		[[nodiscard]] bool operator>=(IteratorImpl<I, IR> other) const noexcept {
			return !operator<(other);
		}
		template <typename I, typename IR>
		[[nodiscard]] bool operator<=(IteratorImpl<I, IR> other) const noexcept {
			return other >= *this;
		}

		[[nodiscard]] IteratorImpl operator*() const noexcept { return *this; }

		NotRankedIt NotRanked() const { return std::get<NotRankedIt>(*this); }
		RankedIt Ranked() const { return std::get<RankedIt>(*this); }
		[[nodiscard]] ItemRefRef GetItemRef() const noexcept {
			return std::visit(overloaded{[](NotRankedIt it) noexcept -> ItemRefRef { return *it; },
										 [](RankedIt it) noexcept -> ItemRefRef { return it->NotRanked(); }},
							  asVariant());
		}
		reindexer::IsRanked IsRanked() const noexcept { return reindexer::IsRanked(std::holds_alternative<RIt>(asVariant())); }

	private:
		Base& asVariant() & noexcept { return *this; }
		const Base& asVariant() const& noexcept { return *this; }
		Base&& asVariant() && noexcept { return std::move(*this); }
	};

public:
	using Iterator = IteratorImpl<NotRankedVec::iterator, RankedVec::iterator>;
	using ConstIterator = IteratorImpl<NotRankedVec::const_iterator, RankedVec::const_iterator>;
	using MoveIterator = IteratorImpl<std::move_iterator<NotRankedVec::iterator>, std::move_iterator<RankedVec::iterator>>;

	ItemRefVector() noexcept = default;
	ItemRefVector(std::initializer_list<ItemRef> l) : variant_{l} {}
	ItemRefVector(std::initializer_list<ItemRefRanked> l) : variant_{l} {}
	ItemRefVector(ConstIterator b, ConstIterator e) {
#ifdef REINDEX_CORE_BUILD
		/*TODO*/ assertf(b.index() == e.index(), "%d %d", b.index(), e.index());
#endif	// REINDEX_CORE_BUILD
		std::visit(overloaded{
					   [e, this](NotRankedVec::const_iterator b) { variant_.emplace<NotRankedVec>(b, e.NotRanked()); },
					   [e, this](RankedVec::const_iterator b) { variant_.emplace<RankedVec>(b, e.Ranked()); },
				   },
				   b.asVariant());
	}

	[[nodiscard]] const ItemRef& GetItemRef(size_t i) const& noexcept {
		return std::visit(overloaded{[i](const NotRankedVec& v) noexcept -> const ItemRef& { return v[i]; },
									 [i](const RankedVec& v) noexcept -> const ItemRef& { return v[i].NotRanked(); }},
						  variant_);
	}
	[[nodiscard]] ItemRef& GetItemRef(size_t i) & noexcept {
		return const_cast<ItemRef&>(const_cast<const ItemRefVector&>(*this).GetItemRef(i));
	}
	[[nodiscard]] const ItemRefRanked& GetItemRefRanked(size_t i) const& {
		return std::visit(overloaded{[](const NotRankedVec&) -> const ItemRefRanked& {
										 throw Error{errLogic, "Get rank from result of not ranked query"};
									 },
									 [i](const RankedVec& v) noexcept -> const ItemRefRanked& { return v[i]; }},
						  variant_);
	}
	[[nodiscard]] ItemRefRanked& GetItemRefRanked(size_t i) & {
		return const_cast<ItemRefRanked&>(const_cast<const ItemRefVector&>(*this).GetItemRefRanked(i));
	}
	ItemRefVariant GetItemRefVariant(size_t i) const {
		return std::visit([i](const auto& v) { return ItemRefVariant{v[i]}; }, variant_);
	}

	[[nodiscard]] const ItemRef& Back() const& noexcept {
		return std::visit(overloaded{[](const RankedVec& v) noexcept -> const ItemRef& { return v.back().NotRanked(); },
									 [](const NotRankedVec& v) noexcept -> const ItemRef& { return v.back(); }},
						  variant_);
	}
	[[nodiscard]] ItemRef& Back() & noexcept {
		return std::visit(overloaded{[](RankedVec& v) noexcept -> ItemRef& { return v.back().NotRanked(); },
									 [](NotRankedVec& v) noexcept -> ItemRef& { return v.back(); }},
						  variant_);
	}

	ConstIterator cbegin() const& noexcept {
		return std::visit([](const auto& v) noexcept { return ConstIterator{v.cbegin()}; }, variant_);
	}
	ConstIterator cend() const& noexcept {
		return std::visit([](const auto& v) noexcept { return ConstIterator{v.cend()}; }, variant_);
	}
	ConstIterator begin() const& noexcept { return cbegin(); }
	ConstIterator end() const& noexcept { return cend(); }
	Iterator begin() & noexcept {
		return std::visit([](auto& v) noexcept { return Iterator{v.begin()}; }, variant_);
	}
	Iterator end() & noexcept {
		return std::visit([](auto& v) noexcept { return Iterator{v.end()}; }, variant_);
	}
	MoveIterator mbegin() && noexcept {
		return std::visit([](auto& v) noexcept { return MoveIterator{std::make_move_iterator(v.begin())}; }, variant_);
	}
	MoveIterator mend() && noexcept {
		return std::visit([](auto& v) noexcept { return MoveIterator{std::make_move_iterator(v.end())}; }, variant_);
	}

	[[nodiscard]] size_t Size() const noexcept {
		return std::visit([](const auto& v) noexcept { return v.size(); }, variant_);
	}
	[[nodiscard]] size_t Capacity() const noexcept {
		return std::visit([](const auto& v) noexcept { return v.capacity(); }, variant_);
	}
	[[nodiscard]] bool Empty() const noexcept {
		return std::visit([](const auto& v) noexcept { return v.empty(); }, variant_);
	}
	void Reserve(size_t s) {
		std::visit([s](auto& v) { return v.reserve(s); }, variant_);
	}
	void Reserve(size_t s, IsRanked isRanked) {
		if (isRanked != this->IsRanked()) {
			assertrx_throw(Empty());
			if (isRanked) {
				variant_.emplace<RankedVec>();
			} else {
				variant_.emplace<NotRankedVec>();
			}
		}
		Reserve(s);
	}
	template <bool FreeHeapMemory = true>
	void Clear() noexcept {
		std::visit([](auto& v) { return v.template clear<FreeHeapMemory>(); }, variant_);
	}

	template <typename... Args>
	void EmplaceBack(Args&&... args) {
		if (Empty() && this->IsRanked()) {
			const auto cap = std::get<RankedVec>(variant_).capacity();
			auto& vec = variant_.emplace<NotRankedVec>();
			vec.reserve(cap);
			vec.emplace_back(std::forward<Args>(args)...);
		} else {
			std::get<NotRankedVec>(variant_).emplace_back(std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	void EmplaceBack(RankT r, Args&&... args) {
		if (Empty() && !this->IsRanked()) {
			const auto cap = std::get<NotRankedVec>(variant_).capacity();
			auto& vec = variant_.emplace<RankedVec>();
			vec.reserve(cap);
			vec.emplace_back(r, std::forward<Args>(args)...);
		} else {
			std::get<RankedVec>(variant_).emplace_back(r, std::forward<Args>(args)...);
		}
	}

	void Insert(ConstIterator it, MoveIterator b, MoveIterator e) {
#ifdef REINDEX_CORE_BUILD
		/*TODO*/ assertf(b.index() == e.index(), "%d %d", b.index(), e.index());
		/*TODO*/ assertf(variant_.index() == it.index(), "%d %d", variant_.index(), it.index());
#endif	// REINDEX_CORE_BUILD
		if (Empty() && this->IsRanked() != b.IsRanked()) {
			/*TODO*/ assertrx(it == cbegin());
			std::visit(overloaded{
						   [e, this](std::move_iterator<NotRankedVec::iterator> b) {
							   variant_.emplace<NotRankedVec>(std::make_move_iterator(b), std::make_move_iterator(e.NotRanked()));
						   },
						   [e, this](std::move_iterator<RankedVec::iterator> b) {
							   variant_.emplace<RankedVec>(std::make_move_iterator(b), std::make_move_iterator(e.Ranked()));
						   },
					   },
					   b.asVariant());
		} else {
#ifdef REINDEX_CORE_BUILD
			/*TODO*/ assertf(variant_.index() == b.index(), "%d %d", variant_.index(), b.index());
#endif	// REINDEX_CORE_BUILD
			std::visit(overloaded{[it, b, e](NotRankedVec& v) {
									  v.insert(it.NotRanked(), std::make_move_iterator(b.NotRanked()),
											   std::make_move_iterator(e.NotRanked()));
								  },
								  [it, b, e](RankedVec& v) {
									  v.insert(it.Ranked(), std::make_move_iterator(b.Ranked()), std::make_move_iterator(e.Ranked()));
								  }},
					   variant_);
		}
	}

	void Erase(ConstIterator b, ConstIterator e) {
#ifdef REINDEX_CORE_BUILD
		/*TODO*/ assertf(b.index() == e.index(), "%d %d", b.index(), e.index());
		/*TODO*/ assertf(variant_.index() == b.index(), "%d %d", variant_.index(), b.index());
#endif	// REINDEX_CORE_BUILD
		std::visit(overloaded{[b, e](NotRankedVec& v) { v.erase(b.NotRanked(), e.NotRanked()); },
							  [b, e](RankedVec& v) { v.erase(b.Ranked(), e.Ranked()); }},
				   variant_);
	}

	reindexer::IsRanked IsRanked() const noexcept { return reindexer::IsRanked{std::holds_alternative<RankedVec>(variant_)}; }

	auto cbegin() const&& = delete;
	auto cend() const&& = delete;
	auto begin() const&& = delete;
	auto end() const&& = delete;
	auto GetItemRef(size_t) const&& = delete;
	auto GetItemRefRanked(size_t) const&& = delete;
	auto Back() const&& = delete;

private:
	Variant variant_;
};

}  // namespace reindexer

namespace std {

template <typename T1, typename T2>
struct iterator_traits<reindexer::ItemRefVector::IteratorImpl<T1, T2>> {
	using iterator_category = random_access_iterator_tag;
	using difference_type = std::ptrdiff_t;
};

}  // namespace std
