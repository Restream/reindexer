#include "knn_search_params.h"
#include "core/cjson/jsonbuilder.h"
#include "fmt/format.h"

using namespace std::string_view_literals;

static constexpr size_t kKnnParamsBinProtocolVersion = 1;

namespace reindexer {

BruteForceSearchParams KnnSearchParams::BruteForce() const {
	return std::visit(overloaded{
						  [](const BruteForceSearchParams& p) noexcept { return p; },
						  [](const KnnSearchParamsBase& p) noexcept { return BruteForceSearchParams{}.K(p.K()).Radius(p.Radius()); },
						  [](const HnswSearchParams&) -> BruteForceSearchParams {
							  throw Error{errQueryExec, "Expected BruteForceSearchParams but get HnswSearchParams"};
						  },
						  [](const IvfSearchParams&) -> BruteForceSearchParams {
							  throw Error{errQueryExec, "Expected BruteForceSearchParams but get IvfSearchParams"};
						  },
					  },
					  toVariant());
}

HnswSearchParams KnnSearchParams::Hnsw() const {
	return std::visit(
		overloaded{
			[](const HnswSearchParams& p) noexcept { return p; },
			[](const KnnSearchParamsBase& p) { return HnswSearchParams().K(p.K()).Radius(p.Radius()).Ef(p.K() ? *p.K() : 1); },
			[](const BruteForceSearchParams&) -> HnswSearchParams {
				throw Error{errQueryExec, "Expected HnswSearchParams but get BruteForceSearchParams"};
			},
			[](const IvfSearchParams&) -> HnswSearchParams {
				throw Error{errQueryExec, "Expected HnswSearchParams but get IvfSearchParams"};
			},
		},
		toVariant());
}

IvfSearchParams KnnSearchParams::Ivf() const {
	return std::visit(overloaded{
						  [](const IvfSearchParams& p) noexcept { return p; },
						  [](const KnnSearchParamsBase& p) { return IvfSearchParams{}.K(p.K()).Radius(p.Radius()); },
						  [](const BruteForceSearchParams&) -> IvfSearchParams {
							  throw Error{errQueryExec, "Expected IvfSearchParams but get BruteForceSearchParams"};
						  },
						  [](const HnswSearchParams&) -> IvfSearchParams {
							  throw Error{errQueryExec, "Expected IvfSearchParams but get HnswSearchParams"};
						  },
					  },
					  toVariant());
}
template <typename Derived>
void detail::KnnSearchParamsCRTPBase<Derived>::ToDsl(JsonBuilder& json) const {
	if (k_) {
		json.Put(KnnSearchParams::kKName, *k_);
	}
	if (radius_) {
		json.Put(KnnSearchParams::kRadiusName, *radius_);
	}
}

void HnswSearchParams::ToDsl(JsonBuilder& json) const {
	Base::ToDsl(json);
	json.Put(KnnSearchParams::kEfName, Ef());
}

void IvfSearchParams::ToDsl(JsonBuilder& json) const {
	Base::ToDsl(json);
	json.Put(KnnSearchParams::kNProbeName, NProbe());
}
template <typename Derived>
void detail::KnnSearchParamsCRTPBase<Derived>::ToSql(WrSerializer& ser) const {
	ser << fmt::format("{}{}", k_ ? fmt::format("{}={}", KnnSearchParams::kKName, *k_) : "",
					   radius_ ? fmt::format("{}{}={}", k_ ? ", " : "", KnnSearchParams::kRadiusName, *radius_) : "");
}

void HnswSearchParams::ToSql(WrSerializer& ser) const {
	Base::ToSql(ser);
	ser << ", "sv << KnnSearchParams::kEfName << '=' << Ef();
}

void IvfSearchParams::ToSql(WrSerializer& ser) const {
	Base::ToSql(ser);
	ser << ", "sv << KnnSearchParams::kNProbeName << '=' << NProbe();
}
template <typename Derived>
template <typename Type>
void detail::KnnSearchParamsCRTPBase<Derived>::Serialize(WrSerializer& ser, Type type) const {
	ser.PutVarUint(type);
	ser.PutVarUint(kKnnParamsBinProtocolVersion);
	ser.PutUInt8((k_ ? size_t(SerializeMask::K) : 0) | (radius_ ? size_t(SerializeMask::Radius) : 0));
	if (k_) {
		ser.PutVarUint(*k_);
	}
	if (radius_) {
		ser.PutFloat(*radius_);
	}
}

template <typename T>
static auto StructName() noexcept {
	if constexpr (std::is_same_v<T, KnnSearchParamsBase>) {
		return "KnnSearchParamsBase"sv;
	} else if constexpr (std::is_same_v<T, BruteForceSearchParams>) {
		return "BruteForceSearchParams"sv;
	} else if constexpr (std::is_same_v<T, HnswSearchParams>) {
		return "HnswSearchParams"sv;
	} else if constexpr (std::is_same_v<T, IvfSearchParams>) {
		return "IvfSearchParams"sv;
	}
	return ""sv;
}
template <typename Derived>
Derived detail::KnnSearchParamsCRTPBase<Derived>::Deserialize(Serializer& ser, size_t version) {
	if (version != kKnnParamsBinProtocolVersion && version != 0) [[unlikely]] {
		throw Error(errVersion, "Unexpected binary protocol version for {}: {}", StructName<Derived>(), version);
	}

	Derived res;
	if (version == 0) {
		// TODO Support compatibility with the old deserialization until 5.8.0. increase after 5.8.0 release
		res.K(ser.GetVarUInt());
	} else {
		uint8_t mask = ser.GetUInt8();
		res.K(mask & uint8_t(SerializeMask::K) ? std::optional(ser.GetVarUInt()) : std::nullopt);
		res.Radius(mask & uint8_t(SerializeMask::Radius) ? std::optional(ser.GetFloat()) : std::nullopt);
	}
	return res;
}

template <typename Derived>
void detail::KnnSearchParamsCRTPBase<Derived>::Serialize(WrSerializer& ser) const {
	Serialize(ser, KnnSearchParams::Type::Base);
}

void BruteForceSearchParams::Serialize(WrSerializer& ser) const { Base::Serialize(ser, KnnSearchParams::Type::BruteForce); }

BruteForceSearchParams BruteForceSearchParams::Deserialize(Serializer& ser, size_t version) { return Base::Deserialize(ser, version); }

void HnswSearchParams::Serialize(WrSerializer& ser) const {
	Base::Serialize(ser, KnnSearchParams::Type::Hnsw);
	ser.PutVarUint(Ef());
}

HnswSearchParams HnswSearchParams::Deserialize(Serializer& ser, size_t version) {
	return Base::Deserialize(ser, version).Ef(ser.GetVarUInt());
}

void IvfSearchParams::Serialize(WrSerializer& ser) const {
	Base::Serialize(ser, KnnSearchParams::Type::Ivf);
	ser.PutVarUint(NProbe());
}

IvfSearchParams IvfSearchParams::Deserialize(Serializer& ser, size_t version) {
	return Base::Deserialize(ser, version).NProbe(ser.GetVarUInt());
}

KnnSearchParams KnnSearchParams::Deserialize(Serializer& ser) {
	const auto type = KnnSearchParams::Type(ser.GetVarUInt());
	const auto version = ser.GetVarUInt();
	if (version > kKnnParamsBinProtocolVersion) {
		throw Error(errVersion, "Old version. Please update");
	}
	switch (type) {
		case Type::Base:
			return KnnSearchParamsBase::Deserialize(ser, version);
		case Type::BruteForce:
			return BruteForceSearchParams::Deserialize(ser, version);
		case Type::Hnsw:
			return HnswSearchParams::Deserialize(ser, version);
		case Type::Ivf:
			return IvfSearchParams::Deserialize(ser, version);
		default:
			std::cout << "TYPE: " << size_t(type) << std::endl;
			throw_as_assert;
	}
}

static void checkKandRadius(const auto& p) {
	if (!p.K() && !p.Radius()) {
		throw Error(errQueryExec, "K and Radius params can not be empty both");
	};
	if (p.K() && *p.K() == 0) {
		throw Error{errQueryExec, "KNN limit should not be 0"};
	}
}

void KnnSearchParams::Validate() const {
	std::visit(overloaded{[](const auto& p) { checkKandRadius(p); },
						  [](const HnswSearchParams& p) {
							  checkKandRadius(p);
							  if (p.K() && p.Ef() < *p.K()) {
								  throw Error{errQueryExec, "Ef should not be less than k in hnsw query"};
							  }
							  if (p.Ef() < 1) {
								  throw Error{errQueryExec, "Value of 'ef' - {} is out of bounds: [1,{}]", p.Ef(),
											  std::numeric_limits<size_t>::max()};
							  }
						  },
						  [](const IvfSearchParams& p) {
							  checkKandRadius(p);
							  if (p.NProbe() == 0) {
								  throw Error{errQueryExec, "Nprobe should not be 0"};
							  }
						  }},
			   toVariant());
}

template <typename Derived>
std::string detail::KnnSearchParamsCRTPBase<Derived>::Dump() const {
	return fmt::format(" KnnSearchParamsBase{{K: {}, Radius: {}}}", K() ? std::to_string(*K()) : "",
					   Radius() ? std::to_string(*Radius()) : "");
}
std::string BruteForceSearchParams::Dump() const {
	return fmt::format(" BruteForceSearchParams{{K: {}, Radius: {}}}", K() ? std::to_string(*K()) : "",
					   Radius() ? std::to_string(*Radius()) : "");
}
std::string HnswSearchParams::Dump() const {
	return fmt::format(" HnswSearchParams{{K: {}, Radius: {}, Ef: {}}}", K() ? std::to_string(*K()) : "",
					   Radius() ? std::to_string(*Radius()) : "", ef_);
}
std::string IvfSearchParams::Dump() const {
	return fmt::format(" IvfSearchParams{{K: {}, Radius: {}, Nprobe: {}}}", K() ? std::to_string(*K()) : "",
					   Radius() ? std::to_string(*Radius()) : "", nprobe_);
}

template std::string detail::KnnSearchParamsCRTPBase<KnnSearchParamsBase>::Dump() const;
template std::string detail::KnnSearchParamsCRTPBase<BruteForceSearchParams>::Dump() const;
template void detail::KnnSearchParamsCRTPBase<KnnSearchParamsBase>::ToDsl(JsonBuilder&) const;
template void detail::KnnSearchParamsCRTPBase<BruteForceSearchParams>::ToDsl(JsonBuilder&) const;
template void detail::KnnSearchParamsCRTPBase<KnnSearchParamsBase>::ToSql(WrSerializer&) const;
template void detail::KnnSearchParamsCRTPBase<BruteForceSearchParams>::ToSql(WrSerializer&) const;
template void detail::KnnSearchParamsCRTPBase<KnnSearchParamsBase>::Serialize(WrSerializer&) const;
template void detail::KnnSearchParamsCRTPBase<BruteForceSearchParams>::Serialize(WrSerializer&) const;
}  // namespace reindexer
