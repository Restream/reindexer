#pragma once

#include "core/id_type.h"

namespace reindexer {

class [[nodiscard]] FloatVectorId {
public:
	static_assert(sizeof(std::declval<IdType>().ToNumber()) == 4);
	FloatVectorId(IdType id, uint32_t index) noexcept : value_{(uint64_t(id.ToNumber()) << 32) | index} {}
	uint64_t AsNumber() const noexcept { return value_; }
	IdType RowId() const noexcept { return IdType::FromNumber(value_ >> 32); }
	uint32_t ArrayIndex() const noexcept { return value_ & ((uint64_t(1) << 32) - 1); }
	auto operator<=>(const FloatVectorId&) const noexcept = default;
	static FloatVectorId FromNumber(uint64_t v) noexcept { return FloatVectorId{v}; }

private:
	FloatVectorId(uint64_t v) noexcept : value_{v} {}
	uint64_t value_;
};

}  // namespace reindexer

namespace std {

template <>
struct hash<reindexer::FloatVectorId> {
	uint64_t operator()(reindexer::FloatVectorId id) const noexcept {
		constexpr static std::hash<uint64_t> hasher;
		return hasher(id.AsNumber());
	}
};

}  // namespace std
