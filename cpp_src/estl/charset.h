#pragma once

#include <initializer_list>
#include <limits>

namespace reindexer::estl {

class [[nodiscard]] Charset {
public:
	constexpr Charset(std::initializer_list<uint8_t> list) {
		for (const auto& c : list) {
			set(c);
		}
	}
	constexpr bool test(uint8_t pos) const noexcept { return getword(pos) & maskbit(pos); }
	constexpr Charset& set(uint8_t pos, bool val = true) noexcept {
		if (val) {
			this->getword(pos) |= maskbit(pos);
		} else {
			this->getword(pos) &= ~maskbit(pos);
		}
		return *this;
	}
	constexpr static size_t max_values_count() noexcept { return kWordsCount * kBitsPerWord; }

private:
	using WordT = uint64_t;
	constexpr static size_t kBitsPerWord = 64;
	constexpr static size_t kWordsCount = 4;

	static constexpr uint8_t whichword(uint8_t pos) noexcept { return pos / kBitsPerWord; }
	static constexpr WordT maskbit(uint8_t pos) noexcept { return WordT(1) << whichbit(pos); }
	static constexpr size_t whichbit(uint8_t pos) noexcept { return pos % kBitsPerWord; }
	constexpr WordT getword(uint8_t pos) const noexcept { return set_[whichword(pos)]; }
	constexpr WordT& getword(uint8_t pos) noexcept { return set_[whichword(pos)]; }

	WordT set_[kWordsCount] = {0};
};

static_assert(size_t(std::numeric_limits<uint8_t>::max() - std::numeric_limits<uint8_t>::min() + 1) == Charset::max_values_count(),
			  "Expecting max uint8_t range of [0, 255] for the simplicity");

}  // namespace reindexer::estl
