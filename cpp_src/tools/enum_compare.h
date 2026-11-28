#include <cstdint>
#include <type_traits>
#include <utility>

#include "estl/concepts.h"

namespace compare_enum {

template <typename... Enums>
	requires(reindexer::concepts::OneOf<std::underlying_type_t<Enums>, uint8_t> && ...)
class [[nodiscard]] Diff {
public:
	[[nodiscard]] bool Equal() const noexcept { return ((get<Enums>() == 0) && ...); }

	template <typename Enum>
	Diff& Set(Enum val) & {
		get<Enum>() |= std::underlying_type_t<Enum>(val);
		return *this;
	}

	template <auto enum_value>
	Diff& Set(bool isEqual) & {
		if (!isEqual) {
			using t = decltype(enum_value);
			get<t>() |= std::underlying_type_t<t>(enum_value);
		}
		return *this;
	}

	template <auto enum_value>
	[[nodiscard]] Diff&& Set(bool isEqual) && {
		return std::move(Set<enum_value>(isEqual));
	}

	template <typename... OtherEnums>
	Diff& Set(Diff<OtherEnums...> diff) {
		((get<OtherEnums>() = diff.template Get<OtherEnums>()), ...);
		return *this;
	}

	template <typename Enum>
	[[nodiscard]] std::underlying_type_t<Enum> Get() const noexcept {
		return get<Enum>();
	}

	template <typename... CheckEnums>
	[[nodiscard]] bool AllOfIsEqual(CheckEnums... values) const noexcept {
		return (!(get<CheckEnums>() & std::underlying_type_t<CheckEnums>(values)) && ...);
	}

	template <typename... CheckEnums>
	[[nodiscard]] bool AnyOfIsDifferent(CheckEnums... values) const noexcept {
		return !AllOfIsEqual(values...);
	}

	template <typename... OtherEnums>
	Diff& Skip(OtherEnums... values) & {
		((get<OtherEnums>() &= ~std::underlying_type_t<OtherEnums>(values)), ...);
		return *this;
	}

	template <typename... OtherEnums>
	[[nodiscard]] Diff&& Skip(OtherEnums... values) && {
		return std::move(Skip(values...));
	}

private:
	template <typename Enum>
	struct [[nodiscard]] Mask {
		std::underlying_type_t<Enum> value = 0;
	};

	template <typename Enum>
	[[nodiscard]] std::underlying_type_t<Enum> get() const& noexcept {
		return static_cast<const Mask<Enum>&>(diff_).value;
	}

	template <typename Enum>
	[[nodiscard]] std::underlying_type_t<Enum>& get() & noexcept {
		return static_cast<Mask<Enum>&>(diff_).value;
	}

	struct [[nodiscard]] : Mask<Enums>... {
	} diff_;
};
}  // namespace compare_enum