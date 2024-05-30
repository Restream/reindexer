#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace reindexer {
namespace custom_locale_impl {

class CustomLocale {
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

private:
	CustomLocale(const CustomLocale&) = delete;
	CustomLocale& operator=(const CustomLocale&) = delete;
	struct LocalCtx {
		constexpr LocalCtx() noexcept : lower(0), isAlpha(false) {}

		uint16_t lower;
		bool isAlpha;
	};
	std::array<LocalCtx, UINT16_MAX> customLocale_;
};

extern const CustomLocale kCustomLocale;

}  // namespace custom_locale_impl

inline static void ToLower(std::wstring& data) noexcept { custom_locale_impl::kCustomLocale.ToLower(data); }
inline static wchar_t ToLower(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.ToLower(uint32_t(ch)); }

inline static bool IsAlpha(wchar_t ch) noexcept { return custom_locale_impl::kCustomLocale.IsAlpha(uint32_t(ch)); }
inline static bool IsDigit(wchar_t ch) noexcept { return ch >= '0' && ch <= '9'; }

}  // namespace reindexer
