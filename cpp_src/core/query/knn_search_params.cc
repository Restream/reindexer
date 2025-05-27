#include "knn_search_params.h"
#include "core/cjson/jsonbuilder.h"
#include "fmt/format.h"

using namespace std::string_view_literals;

static constexpr size_t kKnnParamsBinProtocolVersion = 0;

namespace reindexer {

KnnSearchParamsBase::KnnSearchParamsBase(size_t k) : k_{k} {
	if (k_ == 0) {
		throw Error{errQueryExec, "KNN limit should not be 0"};
	}
}

HnswSearchParams::HnswSearchParams(size_t k, size_t ef) : Base{k}, ef_{ef} {
	if (ef_ < K()) {
		throw Error{errQueryExec, "Ef should not be less than k in hnsw query"};
	}
}

IvfSearchParams::IvfSearchParams(size_t k, size_t nprobe) : Base{k}, nprobe_{nprobe} {
	if (nprobe_ == 0) {
		throw Error{errQueryExec, "Nprobe should not be 0"};
	}
}

BruteForceSearchParams KnnSearchParams::BruteForce() const {
	return std::visit(overloaded{
						  [](const BruteForceSearchParams& p) noexcept { return p; },
						  [](const KnnSearchParamsBase& p) { return BruteForceSearchParams{p.K()}; },
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
	return std::visit(overloaded{
						  [](const HnswSearchParams& p) noexcept { return p; },
						  [](const KnnSearchParamsBase& p) { return HnswSearchParams{p.K()}; },
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
						  [](const KnnSearchParamsBase& p) { return IvfSearchParams{p.K()}; },
						  [](const BruteForceSearchParams&) -> IvfSearchParams {
							  throw Error{errQueryExec, "Expected IvfSearchParams but get BruteForceSearchParams"};
						  },
						  [](const HnswSearchParams&) -> IvfSearchParams {
							  throw Error{errQueryExec, "Expected IvfSearchParams but get HnswSearchParams"};
						  },
					  },
					  toVariant());
}

void KnnSearchParamsBase::ToDsl(JsonBuilder& json) const { json.Put(KnnSearchParams::kKName, K()); }

void HnswSearchParams::ToDsl(JsonBuilder& json) const {
	json.Put(KnnSearchParams::kKName, K());
	json.Put(KnnSearchParams::kEfName, Ef());
}

void IvfSearchParams::ToDsl(JsonBuilder& json) const {
	json.Put(KnnSearchParams::kKName, K());
	json.Put(KnnSearchParams::kNProbeName, NProbe());
}

void KnnSearchParamsBase::ToSql(WrSerializer& ser) const { ser << KnnSearchParams::kKName << '=' << K(); }

void HnswSearchParams::ToSql(WrSerializer& ser) const {
	Base::ToSql(ser);
	ser << ", "sv << KnnSearchParams::kEfName << '=' << Ef();
}

void IvfSearchParams::ToSql(WrSerializer& ser) const {
	Base::ToSql(ser);
	ser << ", "sv << KnnSearchParams::kNProbeName << '=' << NProbe();
}

void KnnSearchParamsBase::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(KnnSearchParams::Type::Base);
	ser.PutVarUint(kKnnParamsBinProtocolVersion);
	ser.PutVarUint(K());
}

KnnSearchParamsBase KnnSearchParamsBase::Deserialize(Serializer& ser, size_t version) {
	if rx_unlikely (version != kKnnParamsBinProtocolVersion) {
		throw Error(errVersion, "Unexpected binary protocol version for KnnSearchParamsBase: {}", version);
	}
	return KnnSearchParamsBase{size_t(ser.GetVarUInt())};
}

void BruteForceSearchParams::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(KnnSearchParams::Type::BruteForce);
	ser.PutVarUint(kKnnParamsBinProtocolVersion);
	ser.PutVarUint(K());
}

BruteForceSearchParams BruteForceSearchParams::Deserialize(Serializer& ser, size_t version) {
	if rx_unlikely (version != kKnnParamsBinProtocolVersion) {
		throw Error(errVersion, "Unexpected binary protocol version for BruteForceSearchParams: {}", version);
	}
	return BruteForceSearchParams{size_t(ser.GetVarUInt())};
}

void HnswSearchParams::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(KnnSearchParams::Type::Hnsw);
	ser.PutVarUint(kKnnParamsBinProtocolVersion);
	ser.PutVarUint(K());
	ser.PutVarUint(Ef());
}

HnswSearchParams HnswSearchParams::Deserialize(Serializer& ser, size_t version) {
	if rx_unlikely (version != kKnnParamsBinProtocolVersion) {
		throw Error(errVersion, "Unexpected binary protocol version for HnswSearchParams: {}", version);
	}
	const size_t k = ser.GetVarUInt();
	return HnswSearchParams{k, size_t(ser.GetVarUInt())};
}

void IvfSearchParams::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(KnnSearchParams::Type::Ivf);
	ser.PutVarUint(kKnnParamsBinProtocolVersion);
	ser.PutVarUint(K());
	ser.PutVarUint(NProbe());
}

IvfSearchParams IvfSearchParams::Deserialize(Serializer& ser, size_t version) {
	if rx_unlikely (version != kKnnParamsBinProtocolVersion) {
		throw Error(errVersion, "Unexpected binary protocol version for IvfSearchParams: {}", version);
	}
	const size_t k = ser.GetVarUInt();
	return IvfSearchParams{k, size_t(ser.GetVarUInt())};
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

std::string KnnSearchParamsBase::Dump() const { return fmt::format(" KnnSearchParamsBase{{K: {}}}", K()); }
std::string BruteForceSearchParams::Dump() const { return fmt::format(" BruteForceSearchParams{{K: {}}}", K()); }
std::string HnswSearchParams::Dump() const { return fmt::format(" HnswSearchParams{{K: {}, Ef: {}}}", K(), ef_); }
std::string IvfSearchParams::Dump() const { return fmt::format(" IvfSearchParams{{K: {}, Nprobe: {}}}", K(), nprobe_); }

}  // namespace reindexer
