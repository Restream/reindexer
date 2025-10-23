#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace reindexer {

class WrSerializer;
class Serializer;

namespace builders {
class JsonBuilder;
}  // namespace builders
using builders::JsonBuilder;

namespace detail {
template <typename Derived>
class [[nodiscard]] KnnSearchParamsCRTPBase {
public:
	Derived&& Radius(std::optional<float> radius) && noexcept { return std::move(Radius(std::move(radius))); }
	Derived& Radius(std::optional<float> radius) & noexcept {
		radius_ = radius;
		return *static_cast<Derived*>(this);
	}
	constexpr Derived&& K(std::optional<size_t> k) && noexcept { return std::move(K(std::move(k))); }
	constexpr Derived& K(std::optional<size_t> k) & noexcept {
		k_ = k;
		return *static_cast<Derived*>(this);
	}

	std::optional<size_t> K() const noexcept { return k_; }
	std::optional<float> Radius() const noexcept { return radius_; }
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	template <typename Type>
	void Serialize(WrSerializer&, Type) const;
	static Derived Deserialize(Serializer&, size_t version);
	std::string Dump() const;
	enum class [[nodiscard]] SerializeMask : uint8_t { K = 1 << 0, Radius = K << 1 };

private:
	std::optional<size_t> k_;
	std::optional<float> radius_;
};
}  // namespace detail

/** Base struct with parameters for search in some vector index
 * Usage examples:
 * KnnSearchParamsBase{}.K(20)
 * KnnSearchParamsBase{}.Radius(3.f)
 * KnnSearchParamsBase{}.K(20).Radius(3.f)
 */
class [[nodiscard]] KnnSearchParamsBase : public detail::KnnSearchParamsCRTPBase<KnnSearchParamsBase> {};

/** Struct with parameters for search in HNSWBruteForce-index
 * Usage as for base structure
 */
class [[nodiscard]] BruteForceSearchParams : private detail::KnnSearchParamsCRTPBase<BruteForceSearchParams> {
	using Base = KnnSearchParamsCRTPBase<BruteForceSearchParams>;
	friend Base;

public:
	explicit BruteForceSearchParams() = default;
	using Base::Base;
	using Base::ToDsl;
	using Base::ToSql;
	using Base::K;
	using Base::Radius;
	void Serialize(WrSerializer&) const;
	static BruteForceSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;
};

/** Struct with parameters for search in HNSW-index
 * Usage examples:
 * HnswSearchParams{}.K(20)
 * HnswSearchParams{}.K(20).Radius(3.f).Ef(2)
 * HnswSearchParams{}.K(20).Ef(2)
 * HnswSearchParams{}.Radius(3.f)
 * HnswSearchParams{}.Radius(3.f).Ef(2)
 */
class [[nodiscard]] HnswSearchParams : private detail::KnnSearchParamsCRTPBase<HnswSearchParams> {
	using Base = KnnSearchParamsCRTPBase<HnswSearchParams>;
	friend Base;

public:
	using Base::K;
	using Base::Radius;
	explicit HnswSearchParams() = default;
	size_t Ef() const noexcept { return ef_; }
	constexpr HnswSearchParams&& Ef(size_t ef) && noexcept { return std::move(Ef(ef)); }
	constexpr HnswSearchParams& Ef(size_t ef) & noexcept {
		ef_ = ef;
		return *this;
	}
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	static HnswSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;

private:
	size_t ef_ = 1;
};

/** Struct with parameters for search in Ivf-index
 * Usage examples:
 * IvfSearchParams{}.K(20).Radius(3.f).NProbe(2)
 * IvfSearchParams{}.K(20).NProbe(2)
 * IvfSearchParams{}.Radius(3.f)
 * IvfSearchParams{}.Radius(3.f).NProbe(2)
 */
class [[nodiscard]] IvfSearchParams : private detail::KnnSearchParamsCRTPBase<IvfSearchParams> {
	using Base = KnnSearchParamsCRTPBase<IvfSearchParams>;
	friend Base;

public:
	using Base::K;
	using Base::Radius;
	explicit IvfSearchParams() = default;
	size_t NProbe() const noexcept { return nprobe_; }
	constexpr IvfSearchParams&& NProbe(size_t nprobe) && noexcept { return std::move(NProbe(nprobe)); }
	constexpr IvfSearchParams& NProbe(size_t nprobe) & noexcept {
		nprobe_ = nprobe;
		return *this;
	}
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	static IvfSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;

private:
	size_t nprobe_ = 1;
};

class [[nodiscard]] KnnSearchParams : private std::variant<KnnSearchParamsBase, BruteForceSearchParams, HnswSearchParams, IvfSearchParams> {
	using Base = std::variant<KnnSearchParamsBase, BruteForceSearchParams, HnswSearchParams, IvfSearchParams>;
	const Base& toVariant() const& noexcept { return *this; }
	using Base::Base;

	auto toVariant() const&& = delete;

public:
	enum class [[nodiscard]] Type : uint8_t { Base = 0, BruteForce = 1, Hnsw = 2, Ivf = 3 };
	static constexpr std::string_view kKName = "k";
	static constexpr std::string_view kEfName = "ef";
	static constexpr std::string_view kNProbeName = "nprobe";
	static constexpr std::string_view kRadiusName = "radius";

	KnnSearchParams(const KnnSearchParams&) noexcept = default;
	KnnSearchParams(KnnSearchParams&&) noexcept = default;
	KnnSearchParams& operator=(const KnnSearchParams&) noexcept = default;
	KnnSearchParams& operator=(KnnSearchParams&&) noexcept = default;

	BruteForceSearchParams BruteForce() const;
	HnswSearchParams Hnsw() const;
	IvfSearchParams Ivf() const;

	void Validate() const;

	void ToDsl(JsonBuilder& json) const {
		return std::visit([&json](const auto& p) { return p.ToDsl(json); }, toVariant());
	}
	void ToSql(WrSerializer& ser) const {
		return std::visit([&ser](const auto& p) { return p.ToSql(ser); }, toVariant());
	}
	void Serialize(WrSerializer& ser) const {
		return std::visit([&ser](const auto& p) { return p.Serialize(ser); }, toVariant());
	}
	static KnnSearchParams Deserialize(Serializer&);
	std::string Dump() const {
		return std::visit([](const auto& p) { return p.Dump(); }, toVariant());
	}
};

}  // namespace reindexer
