#pragma once

#include <cstring>
#include <memory>
#include <span>
#include "core/enums.h"
#include "tools/assertrx.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] FloatVectorImplView {
	using UnderlingT = std::remove_const_t<T>;
	static constexpr unsigned kDimensionOffset = 48;
	static constexpr uint64_t kPtrMask = (uint64_t(1) << kDimensionOffset) - 1;

public:
	using DataType = T;
	constexpr FloatVectorImplView() noexcept : payload_{0} {}
	explicit FloatVectorImplView(std::span<T> data) noexcept
		: payload_{data.empty() ? 0 : (uint64_t(data.size() << kDimensionOffset) | uint64_t(data.data()))} {
		assertrx_dbg(Dimension().Value() == data.size());
		assertrx_dbg(Dimension().IsZero() || Data() == data.data());
	}
	FloatVectorDimension Dimension() const noexcept {
		return FloatVectorDimension(payload_ >> kDimensionOffset);	// NOLINT(*EnumCastOutOfRange)
	}
	T* Data() const noexcept {
		assertrx(!IsEmpty() && !IsStripped());
		return reinterpret_cast<T*>(payload_ & kPtrMask);
	}
	uint64_t Payload() const noexcept { return payload_; }
	std::span<const UnderlingT> Span() const noexcept {
		if (IsEmpty()) {
			return {};
		} else {
			assertrx(!IsStripped());
			return {Data(), Dimension().Value()};
		}
	}
	bool IsEmpty() const noexcept { return payload_ == 0; }
	bool IsStrippedOrEmpty() const noexcept { return (payload_ & kPtrMask) == 0; }
	bool IsStripped() const noexcept { return IsStrippedOrEmpty() && !IsEmpty(); }

	static FloatVectorImplView FromUint64(uint64_t v) noexcept { return FloatVectorImplView{v}; }
	uint64_t Hash() const noexcept {
		static constexpr std::hash<FloatVectorDimension::value_type> hashDim;
		static constexpr std::hash<UnderlingT> hashData;
		if (IsStripped()) {
			return hashDim(Dimension().Value());
		} else {
			auto res = hashDim(Dimension().Value());
			const auto data{Span()};
			for (size_t i = 0; i < data.size(); ++i) {
				res = (res << 1) | (res >> 63);
				res ^= hashData(data[i]);
			}
			return res;
		}
	}

protected:
	void Strip() noexcept { payload_ &= (~kPtrMask); }
	static FloatVectorImplView CreateStripped(FloatVectorDimension dimension) noexcept {
		return FloatVectorImplView{uint64_t(dimension) << kDimensionOffset};
	}

private:
	explicit FloatVectorImplView(uint64_t v) noexcept : payload_{v} {}
	uint64_t payload_;
};

using FloatVectorView = FloatVectorImplView<float>;

template <typename>
class FloatVectorImpl;

class [[nodiscard]] ConstFloatVectorView : public FloatVectorImplView<const float> {
	using Base = FloatVectorImplView<const float>;

public:
	ConstFloatVectorView(FloatVectorView other) : Base{std::span<const float>(other.Data(), other.Dimension().Value())} {}
	ConstFloatVectorView(Base other) noexcept : Base{other} {}
	template <typename T>
	explicit ConstFloatVectorView(const FloatVectorImpl<T>&) noexcept;
	using Base::Base;

	template <typename T>
	explicit ConstFloatVectorView(FloatVectorImpl<T>&&) = delete;

	using Base::CreateStripped;
	using Base::Strip;
};

template <typename T>
class [[nodiscard]] FloatVectorImpl : private std::unique_ptr<T[]> {
	using Base = std::unique_ptr<T[]>;
	using UnderlingT = std::remove_const_t<T>;

public:
	FloatVectorImpl() noexcept : Base{nullptr} {}
	FloatVectorImpl(const FloatVectorImpl& other) : FloatVectorImpl{other.Span()} {}
	FloatVectorImpl(FloatVectorImpl&& other) noexcept : Base{std::move(other)}, dimension_{other.Dimension()} {}
	FloatVectorImpl& operator=(FloatVectorImpl&& other) noexcept {
		dimension_ = other.dimension_;
		Base::operator=(std::move(other));
		return *this;
	}
	explicit FloatVectorImpl(std::span<const UnderlingT> data) : dimension_(FloatVectorDimension(data.size())) {
		std::unique_ptr<UnderlingT> newData{new UnderlingT[dimension_.Value()]};
		std::memcpy(newData.get(), data.data(), dimension_.Value() * sizeof(T));
		static_cast<Base&>(*this) = Base{newData.release()};
	}
	explicit FloatVectorImpl(ConstFloatVectorView other) : FloatVectorImpl{std::span<const T>{other.Data(), other.Dimension().Value()}} {}
	template <typename U>
	explicit FloatVectorImpl(const FloatVectorImpl<U>& other) : dimension_{other.Dimension()} {
		static_assert(std::is_same_v<std::remove_const_t<U>, UnderlingT>);
		std::unique_ptr<UnderlingT> newData{new UnderlingT[dimension_.Value()]};
		std::memcpy(newData.get(), other.RawData(), dimension_.Value() * sizeof(T));
		static_cast<Base&>(*this) = Base{newData.release()};
	}
	std::span<const UnderlingT> Span() const& noexcept {
		if (IsEmpty()) {
			return {};
		} else {
			return {this->get(), dimension_.Value()};
		}
	}
	ConstFloatVectorView View() const& noexcept { return ConstFloatVectorView{Span()}; }
	UnderlingT* RawData() & noexcept {
		assertrx(!IsEmpty());
		return this->get();
	}
	const UnderlingT* RawData() const& noexcept {
		assertrx(!IsEmpty());
		return this->get();
	}
	UnderlingT* Release() && noexcept {
		assertrx(!IsEmpty());
		return this->release();
	}
	FloatVectorDimension Dimension() const noexcept { return dimension_; }
	bool IsEmpty() const noexcept { return *this == nullptr; }

	static FloatVectorImpl CreateNotInitialized(FloatVectorDimension dimension) {
		if (!dimension.IsZero()) [[likely]] {
			return FloatVectorImpl(dimension);
		}
		return FloatVectorImpl();
	}

	auto Span() const&& = delete;
	auto View() const&& = delete;
	auto RawData() const&& = delete;

private:
	FloatVectorImpl(FloatVectorDimension dimension) : dimension_(dimension) {
		std::unique_ptr<UnderlingT> newData{new UnderlingT[dimension_.Value()]};
		static_cast<Base&>(*this) = Base{newData.release()};
	}

	FloatVectorDimension dimension_;
};

using FloatVector = FloatVectorImpl<float>;
using ConstFloatVector = FloatVectorImpl<const float>;

template <typename T>
inline ConstFloatVectorView::ConstFloatVectorView(const FloatVectorImpl<T>& vec) noexcept : Base{vec.Span()} {}

}  // namespace reindexer
