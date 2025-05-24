#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace reindexer {

class WrSerializer;
class Serializer;
class JsonBuilder;

class KnnSearchParamsBase {
public:
	explicit KnnSearchParamsBase(size_t k);
	size_t K() const noexcept { return k_; }
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	static KnnSearchParamsBase Deserialize(Serializer&, size_t version);
	std::string Dump() const;

private:
	size_t k_;
};

class BruteForceSearchParams : private KnnSearchParamsBase {
	using Base = KnnSearchParamsBase;

public:
	using Base::Base;
	using Base::K;
	using Base::ToDsl;
	using Base::ToSql;
	void Serialize(WrSerializer&) const;
	static BruteForceSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;
};

class HnswSearchParams : private KnnSearchParamsBase {
	using Base = KnnSearchParamsBase;

public:
	explicit HnswSearchParams(size_t k, size_t ef);
	explicit HnswSearchParams(size_t k) : HnswSearchParams{k, k} {}
	size_t Ef() const noexcept { return ef_; }
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	static HnswSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;
	using Base::K;

private:
	size_t ef_;
};

class IvfSearchParams : private KnnSearchParamsBase {
	using Base = KnnSearchParamsBase;

public:
	IvfSearchParams(size_t k, size_t nprobe = 1);
	size_t NProbe() const noexcept { return nprobe_; }
	void ToDsl(JsonBuilder&) const;
	void ToSql(WrSerializer&) const;
	void Serialize(WrSerializer&) const;
	static IvfSearchParams Deserialize(Serializer&, size_t version);
	std::string Dump() const;
	using Base::K;

private:
	size_t nprobe_;
};

class KnnSearchParams : private std::variant<KnnSearchParamsBase, BruteForceSearchParams, HnswSearchParams, IvfSearchParams> {
	using Base = std::variant<KnnSearchParamsBase, BruteForceSearchParams, HnswSearchParams, IvfSearchParams>;
	const Base& toVariant() const& noexcept { return *this; }
	using Base::Base;

	auto toVariant() const&& = delete;

public:
	enum class [[nodiscard]] Type : uint8_t { Base = 0, BruteForce = 1, Hnsw = 2, Ivf = 3 };
	static constexpr std::string_view kKName = "k";
	static constexpr std::string_view kEfName = "ef";
	static constexpr std::string_view kNProbeName = "nprobe";

	KnnSearchParams(const KnnSearchParams&) noexcept = default;
	KnnSearchParams(KnnSearchParams&&) noexcept = default;
	KnnSearchParams& operator=(const KnnSearchParams&) noexcept = default;
	KnnSearchParams& operator=(KnnSearchParams&&) noexcept = default;

	static KnnSearchParams BruteForce(size_t k) { return KnnSearchParams{std::in_place_type<BruteForceSearchParams>, k}; }
	static KnnSearchParams Hnsw(size_t k) { return KnnSearchParams{std::in_place_type<HnswSearchParams>, k}; }
	static KnnSearchParams Hnsw(size_t k, size_t ef) { return KnnSearchParams{std::in_place_type<HnswSearchParams>, k, ef}; }
	static KnnSearchParams Ivf(size_t k) { return KnnSearchParams{std::in_place_type<IvfSearchParams>, k}; }
	static KnnSearchParams Ivf(size_t k, size_t nprobe) { return KnnSearchParams{std::in_place_type<IvfSearchParams>, k, nprobe}; }

	BruteForceSearchParams BruteForce() const;
	HnswSearchParams Hnsw() const;
	IvfSearchParams Ivf() const;

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
