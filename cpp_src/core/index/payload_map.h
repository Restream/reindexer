#pragma once

#include "core/namespace/stringsholder.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "cpp-btree/btree_map.h"
#include "sparse-map/sparse_map.h"
#include "sparse-map/sparse_set.h"
#include "vendor/hopscotch/hopscotch_set.h"

namespace reindexer {

class [[nodiscard]] PayloadValueWithHash : public PayloadValue {
public:
	PayloadValueWithHash() noexcept : PayloadValue() {}
	PayloadValueWithHash(PayloadValue&& pv, const PayloadType& pt, const FieldsSet& fields)
		: PayloadValue(std::move(pv)), hash_(ConstPayload(pt, *static_cast<PayloadValue*>(this)).GetHash(fields)) {}
	PayloadValueWithHash(const PayloadValueWithHash& o) noexcept : PayloadValue(o), hash_(o.hash_) {}
	PayloadValueWithHash(PayloadValueWithHash&& o) noexcept : PayloadValue(std::move(o)), hash_(o.hash_) {}
	PayloadValueWithHash& operator=(PayloadValueWithHash&& o) noexcept {
		hash_ = o.hash_;
		return static_cast<PayloadValueWithHash&>(PayloadValue::operator=(std::move(o)));
	}
	uint32_t GetHash() const noexcept { return hash_; }

private:
	uint32_t hash_ = 0;
};

class [[nodiscard]] equal_composite {
public:
	using is_transparent = void;

	template <typename PT, typename FS>
	equal_composite(PT&& type, FS&& fields) : type_(std::forward<PT>(type)), fields_(std::forward<FS>(fields)) {
		assertrx_dbg(type_);
	}
	bool operator()(const PayloadValue& lhs, const PayloadValue& rhs) const { return ConstPayload(type_, lhs).IsEQ(rhs, fields_); }
	bool operator()(const PayloadValueWithHash& lhs, const PayloadValueWithHash& rhs) const {
		return ConstPayload(type_, lhs).IsEQ(rhs, fields_);
	}
	bool operator()(const PayloadValueWithHash& lhs, const PayloadValue& rhs) const { return ConstPayload(type_, lhs).IsEQ(rhs, fields_); }
	bool operator()(const PayloadValue& lhs, const PayloadValueWithHash& rhs) const { return ConstPayload(type_, lhs).IsEQ(rhs, fields_); }

private:
	PayloadType type_;
	FieldsSet fields_;
};

class [[nodiscard]] equal_composite_ref {
public:
	equal_composite_ref(const PayloadType& type, const FieldsSet& fields) noexcept : type_(type), fields_(fields) {
		assertrx_dbg(type_.get());
	}
	bool operator()(const PayloadValue& lhs, const PayloadValue& rhs) const {
		assertrx_dbg(!lhs.IsFree());
		assertrx_dbg(!rhs.IsFree());
		return ConstPayload(type_, lhs).IsEQ(rhs, fields_);
	}

private:
	std::reference_wrapper<const PayloadType> type_;
	std::reference_wrapper<const FieldsSet> fields_;
};

class [[nodiscard]] hash_composite {
public:
	template <typename PT, typename FS>
	hash_composite(PT&& type, FS&& fields) : type_(std::forward<PT>(type)), fields_(std::forward<FS>(fields)) {
		assertrx_dbg(type_);
	}
	size_t operator()(const PayloadValueWithHash& s) const noexcept { return s.GetHash(); }
	size_t operator()(const PayloadValue& s) const { return ConstPayload(type_, s).GetHash(fields_); }

private:
	PayloadType type_;
	FieldsSet fields_;
};

class [[nodiscard]] hash_composite_ref {
public:
	hash_composite_ref(const PayloadType& type, const FieldsSet& fields) noexcept : type_(type), fields_(fields) {
		assertrx_dbg(type_.get());
	}
	size_t operator()(const PayloadValue& s) const { return ConstPayload(type_, s).GetHash(fields_); }

private:
	std::reference_wrapper<const PayloadType> type_;
	std::reference_wrapper<const FieldsSet> fields_;
};

class [[nodiscard]] less_composite {
public:
	less_composite(PayloadType&& type, FieldsSet&& fields) noexcept : type_(std::move(type)), fields_(std::move(fields)) {
		assertrx_dbg(type_);
	}
	bool operator()(const PayloadValue& lhs, const PayloadValue& rhs) const {
		assertrx_dbg(!lhs.IsFree());
		assertrx_dbg(!rhs.IsFree());
		return (ConstPayload(type_, lhs).Compare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs, fields_) ==
				ComparationResult::Lt);
	}

private:
	PayloadType type_;
	FieldsSet fields_;
};

class [[nodiscard]] less_composite_ref {
public:
	less_composite_ref(const PayloadType& type, const FieldsSet& fields) noexcept : type_(type), fields_(fields) {
		assertrx_dbg(type_.get());
	}
	bool operator()(const PayloadValue& lhs, const PayloadValue& rhs) const {
		assertrx_dbg(!lhs.IsFree());
		assertrx_dbg(!rhs.IsFree());
		return (ConstPayload(type_, lhs).Compare<WithString::No, NotComparable::Throw, kDefaultNullsHandling>(rhs, fields_) ==
				ComparationResult::Lt);
	}

private:
	std::reference_wrapper<const PayloadType> type_;
	std::reference_wrapper<const FieldsSet> fields_;
};

template <bool hold>
class payload_str_fields_helper;

template <>
class [[nodiscard]] payload_str_fields_helper<true> {
protected:
	payload_str_fields_helper(PayloadType&& payloadType, const FieldsSet& fields) : payload_type_(std::move(payloadType)) {
		if (fields.getTagsPathsLength() || fields.getJsonPathsLength()) {
			str_fields_.push_back(0);
		}
		for (int f : payload_type_.StrFields()) {
			if (fields.contains(f)) {
				str_fields_.push_back(f);
			}
		}
	}
	payload_str_fields_helper(const payload_str_fields_helper&) = default;
	payload_str_fields_helper(payload_str_fields_helper&&) = default;

	inline void add_ref(PayloadValue& pv) const {
		Payload pl(payload_type_, pv);
		for (int f : str_fields_) {
			pl.AddRefStrings(f);
		}
	}

	inline void release(PayloadValue& pv) const {
		Payload pl(payload_type_, pv);
		for (int f : str_fields_) {
			pl.ReleaseStrings(f);
		}
	}

	inline void move_strings_to_holder(PayloadValue& pv, StringsHolder& strHolder) const {
		Payload pl(payload_type_, pv);
		for (int f : str_fields_) {
			pl.MoveStrings(f, strHolder);
		}
	}

	inline bool have_str_fields() const noexcept { return !str_fields_.empty(); }

private:
	PayloadType payload_type_;
	h_vector<int, 4> str_fields_;
};

template <>
class [[nodiscard]] payload_str_fields_helper<false> {
protected:
	payload_str_fields_helper(const PayloadType&, const FieldsSet&) noexcept {}

	inline void add_ref(PayloadValue&) const noexcept {}
	inline void release(PayloadValue&) const noexcept {}
	inline void move_strings_to_holder(PayloadValue&, StringsHolder&) const noexcept {}
	inline bool have_str_fields() const noexcept { return false; }
};

struct [[nodiscard]] no_deep_clean {
	template <typename T>
	void operator()(const T&) const noexcept {}
};

template <typename T1, bool hold>
class unordered_payload_map
	: private tsl::sparse_map<PayloadValueWithHash, T1, hash_composite, equal_composite,
							  std::allocator<std::pair<PayloadValueWithHash, T1>>, tsl::sh::power_of_two_growth_policy<2>,
							  tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
	  private payload_str_fields_helper<hold> {
	using base_hash_map =
		tsl::sparse_map<PayloadValueWithHash, T1, hash_composite, equal_composite, std::allocator<std::pair<PayloadValueWithHash, T1>>,
						tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

public:
	using typename base_hash_map::value_type;
	using typename base_hash_map::key_type;
	using typename base_hash_map::mapped_type;
	using typename base_hash_map::iterator;
	using typename base_hash_map::const_iterator;

	using base_hash_map::size;
	using base_hash_map::empty;
	using base_hash_map::find;
	using base_hash_map::begin;
	using base_hash_map::end;
	using payload_str_fields_helper<hold>::have_str_fields;

	static_assert(std::is_nothrow_move_constructible<std::pair<PayloadValueWithHash, T1>>::value,
				  "Nothrow movebale key and value required");
	unordered_payload_map(size_t size, PayloadType&& pt, FieldsSet&& f)
		: base_hash_map(size, hash_composite(PayloadType{pt}, FieldsSet{f}), equal_composite(PayloadType{pt}, FieldsSet{f})),
		  payload_str_fields_helper<hold>(PayloadType{pt}, f),
		  payloadType_(std::move(pt)),
		  fields_(std::move(f)) {}

	unordered_payload_map(PayloadType&& pt, FieldsSet&& f) : unordered_payload_map(1000, std::move(pt), std::move(f)) {}

	unordered_payload_map(const unordered_payload_map& other)
		: base_hash_map(other), payload_str_fields_helper<hold>(other), payloadType_(other.payloadType_), fields_(other.fields_) {
		for (auto& item : *this) {
			this->add_ref(item.first);
		}
	}
	unordered_payload_map(unordered_payload_map&&) = default;
	unordered_payload_map& operator=(unordered_payload_map&& other) {
		for (auto& item : *this) {
			this->release(item.first);
		}
		base_hash_map::operator=(std::move(other));
		return *this;
	}
	unordered_payload_map& operator=(const unordered_payload_map&) = delete;

	~unordered_payload_map() {
		for (auto& item : *this) {
			this->release(item.first);
		}
	}

	std::pair<iterator, bool> insert(const std::pair<PayloadValue, T1>& v) {
		PayloadValueWithHash key(v.first, payloadType_, fields_);
		auto res = base_hash_map::emplate(std::move(key), v.second);
		if (res.second) {
			add_ref(res.first->first);
		}
		return res;
	}
	std::pair<iterator, bool> insert(std::pair<PayloadValue, T1>&& v) {
		PayloadValueWithHash key(std::move(v.first), payloadType_, fields_);
		auto res = base_hash_map::emplace(std::move(key), std::move(v.second));
		if (res.second) {
			this->add_ref(res.first->first);
		}
		return res;
	}
	template <typename V>
	std::pair<iterator, bool> emplace(const PayloadValue& pl, V&& v) {
		PayloadValueWithHash key(PayloadValue{pl}, payloadType_, fields_);
		auto res = base_hash_map::emplace(std::move(key), std::forward<V>(v));
		if (res.second) {
			this->add_ref(res.first->first);
		}
		return res;
	}
	template <typename V>
	std::pair<iterator, bool> emplace(PayloadValue&& pl, V&& v) {
		PayloadValueWithHash key(std::move(pl), payloadType_, fields_);
		auto res = base_hash_map::emplace(std::move(key), std::forward<V>(v));
		if (res.second) {
			this->add_ref(res.first->first);
		}
		return res;
	}

	template <typename deep_cleaner>
	iterator erase(iterator pos, StringsHolder& strHolder) {
		static const deep_cleaner deep_clean;
		if (pos != end()) {
			this->move_strings_to_holder(pos->first, strHolder);
		}
		deep_clean(*pos);
		return base_hash_map::erase(pos);
	}

	T1& operator[](const PayloadValue& k) {
		PayloadValueWithHash key(PayloadValue{k}, payloadType_, fields_);
		return base_hash_map::operator[](std::move(key));
	}
	T1& operator[](PayloadValue&& k) {
		PayloadValueWithHash key(std::move(k), payloadType_, fields_);
		return base_hash_map::operator[](std::move(key));
	}

private:
	PayloadType payloadType_;
	FieldsSet fields_;
};

template <bool hold>
class [[nodiscard]] unordered_payload_set
	: private tsl::sparse_set<PayloadValueWithHash, hash_composite, equal_composite, std::allocator<PayloadValueWithHash>,
							  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>,
	  private payload_str_fields_helper<hold> {
	using base_hash_set = tsl::sparse_set<PayloadValueWithHash, hash_composite, equal_composite, std::allocator<PayloadValueWithHash>,
										  tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

public:
	using typename base_hash_set::value_type;
	using typename base_hash_set::key_type;
	using typename base_hash_set::iterator;
	using typename base_hash_set::const_iterator;

	using base_hash_set::size;
	using base_hash_set::empty;
	using base_hash_set::find;
	using base_hash_set::begin;
	using base_hash_set::cbegin;
	using base_hash_set::end;
	using base_hash_set::cend;
	using payload_str_fields_helper<hold>::have_str_fields;
	using payload_str_fields_helper<hold>::add_ref;

	static_assert(std::is_nothrow_move_constructible<PayloadValueWithHash>::value, "Nothrow movebale value required");
	unordered_payload_set(size_t size, PayloadType&& pt, FieldsSet&& f)
		: base_hash_set(size, hash_composite(PayloadType{pt}, FieldsSet{f}), equal_composite(PayloadType{pt}, FieldsSet{f})),
		  payload_str_fields_helper<hold>(PayloadType{pt}, f),
		  payloadType_(std::move(pt)),
		  fields_(std::move(f)) {}

	unordered_payload_set(PayloadType&& pt, FieldsSet&& f) : unordered_payload_set(1000, std::move(pt), std::move(f)) {}

	unordered_payload_set(const unordered_payload_set& other)
		: base_hash_set(other), payload_str_fields_helper<hold>(other), payloadType_(other.payloadType_), fields_(other.fields_) {
		for (auto& item : *this) {
			this->add_ref(item);
		}
	}
	unordered_payload_set(unordered_payload_set&&) = default;
	unordered_payload_set& operator=(unordered_payload_set&& other) {
		for (auto& item : *this) {
			this->release(item);
		}
		base_hash_set::operator=(std::move(other));
		return *this;
	}
	unordered_payload_set& operator=(const unordered_payload_set&) = delete;

	~unordered_payload_set() {
		for (auto& item : *this) {
			this->release(item);
		}
	}

	std::pair<iterator, bool> insert(const PayloadValue& v) {
		auto res = base_hash_set::emplace(PayloadValue{v}, payloadType_, fields_);
		if (res.second) {
			this->add_ref(*res.first);
		}
		return res;
	}

	std::pair<iterator, bool> insert(PayloadValue&& v) {
		auto res = base_hash_set::emplace(std::move(v), payloadType_, fields_);
		if (res.second) {
			this->add_ref(res.first);
		}
		return res;
	}

	std::pair<iterator, bool> emplace(const PayloadValue& pl) {
		auto res = base_hash_set::emplace(PayloadValue{pl}, payloadType_, fields_);
		if (res.second) {
			this->add_ref(*res.first);
		}
		return res;
	}

	std::pair<iterator, bool> emplace(PayloadValue&& pl) {
		auto res = base_hash_set::emplace(std::move(pl), payloadType_, fields_);
		if (res.second) {
			this->add_ref(*res.first);
		}
		return res;
	}

	template <typename deep_cleaner>
	iterator erase(iterator pos, StringsHolder& strHolder) {
		static const deep_cleaner deep_clean;
		if (pos != end()) {
			this->move_strings_to_holder(pos, strHolder);
		}
		deep_clean(*pos);
		return base_hash_set::erase(pos);
	}

	template <typename deep_cleaner>
	iterator erase(const_iterator pos, StringsHolder& strHolder) {
		static const deep_cleaner deep_clean;
		if (pos != end()) {
			this->move_strings_to_holder(pos, strHolder);
		}
		deep_clean(*pos);
		return base_hash_set::erase(pos);
	}

private:
	PayloadType payloadType_;
	FieldsSet fields_;
};

template <typename T1, bool hold>
class [[nodiscard]] payload_map : private btree::btree_map<PayloadValue, T1, less_composite>, private payload_str_fields_helper<hold> {
	using base_tree_map = btree::btree_map<PayloadValue, T1, less_composite>;

	using base_tree_map::insert;
	using base_tree_map::erase;

public:
	using typename base_tree_map::value_type;
	using typename base_tree_map::key_type;
	using typename base_tree_map::mapped_type;
	using typename base_tree_map::iterator;
	using typename base_tree_map::const_iterator;
	using typename base_tree_map::reverse_iterator;
	using typename base_tree_map::const_reverse_iterator;

	using base_tree_map::size;
	using base_tree_map::empty;
	using base_tree_map::begin;
	using base_tree_map::end;
	using base_tree_map::key_comp;
	using base_tree_map::lower_bound;
	using base_tree_map::upper_bound;
	using base_tree_map::find;
	using payload_str_fields_helper<hold>::have_str_fields;

	payload_map(PayloadType payloadType, const FieldsSet& fields)
		: base_tree_map(less_composite(PayloadType{payloadType}, FieldsSet{fields})),
		  payload_str_fields_helper<hold>(std::move(payloadType), fields) {}
	payload_map(const payload_map& other) : base_tree_map(other), payload_str_fields_helper<hold>(other) {
		for (auto& item : *this) {
			this->add_ref(const_cast<PayloadValue&>(item.first));
		}
	}
	payload_map(payload_map&&) = default;

	~payload_map() {
		for (auto& item : *this) {
			this->release(const_cast<PayloadValue&>(item.first));
		}
	}

	std::pair<iterator, bool> insert(const value_type& v) {
		auto res = base_tree_map::insert(v);
		if (res.second) {
			this->add_ref(const_cast<PayloadValue&>(res.first->first));
		}
		return res;
	}
	iterator insert(iterator, const value_type& v) { return insert(v).first; }

	template <typename deep_cleaner>
	iterator erase(const iterator& pos, StringsHolder& strHolder) {
		static const deep_cleaner deep_clean;
		if (pos != end()) {
			this->move_strings_to_holder(const_cast<PayloadValue&>(pos->first), strHolder);
		}
		deep_clean(*pos);
		return base_tree_map::erase(pos);
	}
};

using unordered_payload_ref_set =
	tsl::hopscotch_set<PayloadValue, hash_composite_ref, equal_composite_ref, std::allocator<PayloadValue>, 30, true>;

template <typename>
constexpr bool is_payload_map_v = false;

template <typename T, bool hold>
constexpr bool is_payload_map_v<payload_map<T, hold>> = true;

template <typename T, bool hold>
constexpr bool is_payload_map_v<unordered_payload_map<T, hold>> = true;

}  // namespace reindexer
