#pragma once

#include <memory>
#include "estl/defines.h"

namespace reindexer {

template <template <typename> typename Templ, typename... Ts>
struct Template;

// CRTP Base
template <typename Derived, typename Arg>
class RestrictedImplBase {
	RX_ALWAYS_INLINE const Derived& asDerived() const noexcept { return static_cast<const Derived&>(*this); }
	RX_ALWAYS_INLINE Derived& asDerived() noexcept { return static_cast<Derived&>(*this); }

public:
	RX_ALWAYS_INLINE decltype(auto) operator()(const Arg& arg) const& noexcept(
		std::is_nothrow_invocable_v<typename Derived::Func, const Arg&>) {
		return asDerived().Function()(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg& arg) const& noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&>) {
		return asDerived().Function()(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg&& arg) const& noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&&>) {
		return asDerived().Function()(std::move(arg));
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(const Arg& arg) & noexcept(std::is_nothrow_invocable_v<typename Derived::Func, const Arg&>) {
		return asDerived().Function()(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg& arg) & noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&>) {
		return asDerived().Function()(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg&& arg) & noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&&>) {
		return asDerived().Function()(std::move(arg));
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(const Arg& arg) && noexcept(
		std::is_nothrow_invocable_v<typename Derived::Func, const Arg&>) {
		return std::move(asDerived().Function())(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg& arg) && noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&>) {
		return std::move(asDerived().Function())(arg);
	}
	RX_ALWAYS_INLINE decltype(auto) operator()(Arg&& arg) && noexcept(std::is_nothrow_invocable_v<typename Derived::Func, Arg&&>) {
		return std::move(asDerived().Function())(std::move(arg));
	}
};

template <typename Derived, template <typename> typename Templ, typename... Args>
class RestrictedImplBase<Derived, Template<Templ, Args...>> : public RestrictedImplBase<Derived, Templ<Args>>... {
public:
	using RestrictedImplBase<Derived, Templ<Args>>::operator()...;
};

template <typename F, typename... Args>
class RestrictedImpl : public RestrictedImplBase<RestrictedImpl<F, Args...>, Args>... {
public:
	using Func = F;
	RestrictedImpl(Func&& f) noexcept(std::is_nothrow_move_constructible_v<Func>) : func_{std::move(f)} {}
	RestrictedImpl(const Func& f) noexcept(std::is_nothrow_constructible_v<Func>) : func_{f} {}

	using RestrictedImplBase<RestrictedImpl<Func, Args...>, Args>::operator()...;

	RX_ALWAYS_INLINE const Func& Function() const noexcept { return func_; }
	RX_ALWAYS_INLINE Func& Function() noexcept { return func_; }

private:
	Func func_;
};

template <typename... Ts>
class Restricted {
public:
	template <typename F>
	RX_ALWAYS_INLINE auto operator()(F&& f) const
		noexcept(noexcept(RestrictedImpl<std::remove_reference_t<F>, Ts...>{std::forward<F>(f)})) {
		return RestrictedImpl<std::remove_reference_t<F>, Ts...>{std::forward<F>(f)};
	}
};

}  // namespace reindexer
