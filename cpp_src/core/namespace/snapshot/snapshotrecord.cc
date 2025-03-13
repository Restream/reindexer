#include "snapshotrecord.h"
#include "core/cjson/jsonbuilder.h"
#include "vendor/gason/gason.h"

namespace reindexer {

void SnapshotRecord::Deserialize(Serializer& ser) {
	lsn_ = lsn_t(ser.GetVarint());
	auto recSV = ser.GetSlice();
	rec_.resize(recSV.size());
	memcpy(rec_.data(), recSV.data(), recSV.size());
}

void SnapshotRecord::Serilize(WrSerializer& ser) const {
	ser.PutVarint(int64_t(lsn_));
	ser.PutSlice(std::string_view(reinterpret_cast<const char*>(rec_.data()), rec_.size()));
}

void SnapshotChunk::Deserialize(Serializer& ser) {
	opts_ = ser.GetVarUInt();
	auto size = ser.GetVarUInt();
	records.resize(size);
	for (auto& rec : records) {
		rec.Deserialize(ser);
	}
}

void SnapshotChunk::Serilize(WrSerializer& ser) const {
	ser.PutVarUint(opts_);
	ser.PutVarUint(records.size());
	for (auto& rec : records) {
		rec.Serilize(ser);
	}
}

using namespace std::string_view_literals;

Error SnapshotOpts::FromJSON(const gason::JsonNode& root) {
	try {
		auto& fromLsnObj = root["from"sv];
		lsn_t nsVersion, lsn;
		nsVersion.FromJSON(fromLsnObj["ns_version"sv]);
		lsn.FromJSON(fromLsnObj["lsn"sv]);
		from = ExtendedLsn(nsVersion, lsn);
		maxWalDepthOnForceSync = root["max_wal_depth"sv].As<int64_t>();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "SnapshotOpts: {}", ex.what());
	}
	return Error();
}

Error SnapshotOpts::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		return FromJSON(parser.Parse(json));
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "SnapshotOpts: {}", ex.what());
	}
}

void SnapshotOpts::GetJSON(JsonBuilder& jb) const {
	{
		auto fromLsnObj = jb.Object("from"sv);
		{
			auto nsvObj = fromLsnObj.Object("ns_version"sv);
			from.NsVersion().GetJSON(nsvObj);
		}
		{
			auto lsnObj = fromLsnObj.Object("lsn"sv);
			from.LSN().GetJSON(lsnObj);
		}
	}
	jb.Put("max_wal_depth", maxWalDepthOnForceSync);
}

void SnapshotOpts::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	GetJSON(builder);
}

}  // namespace reindexer
