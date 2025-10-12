#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace reindexer {

typedef uint16_t SymbolTypeMask;
enum class [[nodiscard]] SymbolType : unsigned int { Common = 0, Accent = 1, Hebrew = 2, Arabic = 3, Cyrillic = 4 };
constexpr SymbolTypeMask GetSymbolTypeMask(SymbolType dt) { return 1u << static_cast<unsigned int>(dt); }

const SymbolTypeMask kRemoveAllDiacriticsMask = std::numeric_limits<SymbolTypeMask>::max();

constexpr SymbolType GetSymbolType(std::string_view st) noexcept {
	if (st == "acc" || st == "accent") {
		return SymbolType::Accent;
	} else if (st == "heb" || st == "hebrew") {
		return SymbolType::Hebrew;
	} else if (st == "ara" || st == "arabic") {
		return SymbolType::Arabic;
	} else if (st == "cyr" || st == "cyrillic") {
		return SymbolType::Cyrillic;
	}
	return SymbolType::Common;
}

namespace custom_locale_impl {

class [[nodiscard]] CustomLocale {
public:
	CustomLocale() noexcept;
	void ToLower(std::wstring& data) const noexcept {
		for (auto& d : data) {
			if (d < UINT16_MAX && d > 0) {
				d = customLocale_[d].lower;
			}
		}
	}
	wchar_t ToLower(uint32_t ofs) const noexcept { return (ofs < UINT16_MAX) ? customLocale_[ofs].lower : wchar_t(ofs); }
	bool IsAlpha(uint32_t ofs) const noexcept { return (ofs < UINT16_MAX) && customLocale_[ofs].isAlpha; }

	uint32_t RemoveDiacritic(uint32_t ofs) const noexcept {
		if (ofs >= UINT16_MAX) {
			return ofs;
		}
		return additionalSymbolProbs_[ofs].WithoutDiacritic();
	}

	bool IsDiacritic(uint32_t ofs) const noexcept {
		if (ofs >= UINT16_MAX) {
			return false;
		}
		return additionalSymbolProbs_[ofs].withoutDiacritic == 0;
	}

	bool FitsMask(uint32_t ofs, SymbolTypeMask mask) const noexcept {
		if (ofs >= UINT16_MAX) {
			return false;
		}
		return additionalSymbolProbs_[ofs].mask & mask;
	}

private:
	CustomLocale(const CustomLocale&) = delete;
	CustomLocale& operator=(const CustomLocale&) = delete;
	struct [[nodiscard]] LocalCtx {
		constexpr LocalCtx() noexcept : lower(0), isAlpha(false) {}

		uint16_t lower;
		bool isAlpha;
	};
	std::array<LocalCtx, UINT16_MAX> customLocale_;

	struct [[nodiscard]] AdditionalProps {
		uint16_t withoutDiacritic = 0;
		SymbolTypeMask mask = 0;

		uint16_t WithoutDiacritic() const { return withoutDiacritic; }

		void AddSymbolType(SymbolType st) noexcept { mask = mask | GetSymbolTypeMask(st); }
	};

	std::array<AdditionalProps, UINT16_MAX> additionalSymbolProbs_;
};

extern const CustomLocale kCustomLocale;

}  // namespace custom_locale_impl

inline static void ToLower(std::wstring& data) noexcept { custom_locale_impl::kCustomLocale.ToLower(data); }
inline static wchar_t ToLower(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.ToLower(ch); }

inline static bool IsAlpha(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.IsAlpha(ch); }
inline static bool IsDigit(wchar_t ch) noexcept { return ch >= '0' && ch <= '9'; }

inline static wchar_t RemoveDiacritic(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.RemoveDiacritic(ch); }

inline static bool IsDiacritic(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.IsDiacritic(ch); }

inline static wchar_t FitsMask(wchar_t ch, SymbolTypeMask mask) noexcept { return custom_locale_impl::kCustomLocale.FitsMask(ch, mask); }

}  // namespace reindexer
