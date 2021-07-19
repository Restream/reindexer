#pragma once

#include <memory>
#include <vector>
#include "core/keyvalue/key_string.h"

namespace reindexer {

class Index;

class StringsHolder : private std::vector<key_string> {
	using Base = std::vector<key_string>;

public:
	~StringsHolder();
	void Add(key_string&& str, size_t strSize) {
		memStat_ += sizeof(Base::value_type) + strSize;
		Base::emplace_back(std::move(str));
	}
	void Add(key_string&& str) {
		memStat_ += sizeof(Base::value_type) + sizeof(*str.get()) + str->heap_size();
		Base::emplace_back(std::move(str));
	}
	void Add(const key_string& str) {
		memStat_ += sizeof(Base::value_type) + sizeof(*str.get()) + str->heap_size();
		Base::push_back(str);
	}
	void Add(std::unique_ptr<Index>&&);
	template <typename... T>
	void emplace_back(T&&... v) {
		Base::emplace_back(std::forward<T>(v)...);
		memStat_ += sizeof(Base::value_type) + sizeof(*back().get()) + back()->heap_size();
	}
	void Clear() noexcept;
	size_t MemStat() const noexcept { return memStat_; }
	bool HoldsIndexes() const noexcept { return !indexes_.empty(); }
	const std::vector<std::unique_ptr<Index>>& Indexes() const noexcept { return indexes_; }

private:
	std::vector<std::unique_ptr<Index>> indexes_;
	size_t memStat_{0};
};

using StringsHolderPtr = intrusive_ptr<intrusive_atomic_rc_wrapper<StringsHolder>>;

StringsHolderPtr makeStringsHolder();

}  // namespace reindexer
