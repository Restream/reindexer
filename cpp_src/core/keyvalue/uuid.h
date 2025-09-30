#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include "estl/comparation_result.h"
#include "tools/assertrx.h"
#include "tools/errors.h"
#include "variant.h"

namespace reindexer {
class Uuid;
}  // namespace reindexer

namespace std {

template <>
struct [[nodiscard]] hash<reindexer::Uuid> {
	size_t operator()(const reindexer::Uuid&) const noexcept;
};

}  // namespace std

namespace reindexer {

class [[nodiscard]] Uuid {
	friend std::hash<Uuid>;
	friend class Variant;
	friend struct hash_uuid;
	friend class Serializer;
	friend class WrSerializer;

public:
	static constexpr size_t kStrFormLen{36};

	Uuid() noexcept : data_{0, 0} {}
	explicit Uuid(std::string_view);
	explicit Uuid(const std::string& s) : Uuid{std::string_view{s}} {}
	explicit Uuid(const Variant& v) {
		if (v.uuid_.isUuid == 1) {
			data_[0] = v.uuid_.v0;
			data_[0] <<= (64 - 7);
			for (unsigned i = 0; i < 7; ++i) {
				data_[0] |= (uint64_t(v.uuid_.vs[i]) << (64 - 15 - 8 * i));
			}
			data_[0] |= (v.uuid_.v1 >> 63);
			data_[1] = (v.uuid_.v1 | (uint64_t(1) << 63));
		} else {
			if (!v.variant_.type.Is<KeyValueType::Uuid>()) {
				throw Error(errNotValid, "Cannot convert variant containing '" + std::string{v.variant_.type.Name()} + "' data to UUID");
			}
			data_[0] = 0;
			data_[1] = 0;
		}
	}

	template <typename... Ts>
	explicit Uuid(Ts...) = delete;
	explicit operator std::string() const;
	explicit operator key_string() const;
	ComparationResult Compare(const Uuid& other) const noexcept {
		if (data_[0] == other.data_[0]) {
			return data_[1] == other.data_[1] ? ComparationResult::Eq
											  : (data_[1] < other.data_[1] ? ComparationResult::Lt : ComparationResult::Gt);
		} else {
			return data_[0] < other.data_[0] ? ComparationResult::Lt : ComparationResult::Gt;
		}
	}
	bool operator==(Uuid other) const noexcept { return data_[0] == other.data_[0] && data_[1] == other.data_[1]; }
	bool operator!=(Uuid other) const noexcept { return !operator==(other); }
	bool operator<(Uuid other) const noexcept { return data_[0] == other.data_[0] ? data_[1] < other.data_[1] : data_[0] < other.data_[0]; }
	bool operator>(Uuid other) const noexcept { return other.operator<(*this); }
	bool operator<=(Uuid other) const noexcept { return !other.operator<(*this); }
	bool operator>=(Uuid other) const noexcept { return !operator<(other); }

	static std::optional<Uuid> TryParse(std::string_view) noexcept;
	void PutToStr(std::span<char>) const noexcept;

private:
	explicit Uuid(uint64_t v1, uint64_t v2) noexcept : data_{v1, v2} {}
	uint64_t operator[](unsigned i) const noexcept {
		assertrx(i < 2);
		return data_[i];
	}
	static Error tryParse(std::string_view, uint64_t (&)[2]) noexcept;

	uint64_t data_[2];
};

inline std::ostream& operator<<(std::ostream& os, const Uuid& uuid) { return os << '\'' << std::string{uuid} << '\''; }

}  // namespace reindexer

inline size_t std::hash<reindexer::Uuid>::operator()(const reindexer::Uuid& uuid) const noexcept {
	static constexpr std::hash<uint64_t> intHasher;
	return intHasher(uuid.data_[0]) ^ (intHasher(uuid.data_[1]) << 19) ^ (intHasher(uuid.data_[1]) >> 23);
}
