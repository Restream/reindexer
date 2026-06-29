#pragma once

#include <memory>
#include <vector>
#include "core/keyvalue/key_string.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

class Index;

class [[nodiscard]] StringsHolder : private std::vector<key_string> {
	using Base = std::vector<key_string>;

public:
	~StringsHolder();
	void Add(key_string&& str, size_t strSize) {
		Base::emplace_back(std::move(str));
		memStat_ += sizeof(Base::value_type) + strSize;
	}
	void Add(key_string&& str) {
		auto& s = Base::emplace_back(std::move(str));
		memStat_ += sizeof(Base::value_type) + s.heap_size();
	}
	void Add(const key_string& str) {
		auto& s = Base::emplace_back(str);
		memStat_ += sizeof(Base::value_type) + s.heap_size();
	}
	void Add(std::unique_ptr<Index>&&);
	template <typename... T>
	void emplace_back(T&&... v) {
		auto& str = Base::emplace_back(std::forward<T>(v)...);
		memStat_ += sizeof(key_string) + str.heap_size();
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
