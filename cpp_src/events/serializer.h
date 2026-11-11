#pragma once

#include "events/iexternal_listener.h"
#include "net/cproto/args.h"

namespace reindexer {

class [[nodiscard]] UpdateSerializer {
public:
	explicit UpdateSerializer(WrSerializer& ser) noexcept : ser_(ser) {}

	chunk Serialize(uint32_t streamsMask, const EventsSerializationOpts& opts, const EventRecord& rec) {
		ser_.Reset();

		ser_.PutVarUint(opts.Version());
		ser_.PutVarUint(opts.Options());
		ser_.PutUInt32(streamsMask);
		if (opts.IsWithDBName()) {
			ser_.PutVString(opts.DBName());
		}
		ser_.PutVString(rec.NsName());
		ser_.PutVarint(int(rec.Type()));
		if (opts.IsWithShardID()) {
			ser_.PutVarint(opts.ShardID());
		}
		if (opts.IsWithLSN()) {
			ser_.PutVarint(int64_t(rec.ExtLSN().NsVersion()));
			ser_.PutVarint(int64_t(rec.ExtLSN().LSN()));
		}
		if (opts.IsWithServerID()) {
			ser_.PutVarint(opts.ServerID());
		}
		if (opts.IsWithTimestamp()) {
			ser_.PutVarint(std::chrono::duration_cast<std::chrono::nanoseconds>(rec.Timestamp().time_since_epoch()).count());
		}

		// WrSerializer tmpSer;
		// std::string_view tmpSlice;
		net::cproto::Args data;
		switch (opts.DataType()) {
			case kSubscriptionDataTypeNone:
				break;
			default:;
				// TODO: #1713
				// Pack data somehow. Will be used in the later versions
				// if (rec.data) {
				// 	std::visit(overloaded{[&data, &tmpSlice](const cluster::ItemReplicationRecord& r) {
				// 							  tmpSlice = r.cjson.Slice();
				// 							  data.emplace_back(p_string(&tmpSlice), Variant::no_hold_t{});
				// 						  },
				// 						  [&data](const cluster::MetaReplicationRecord& r) {
				// 							  data.emplace_back(p_string(&r.key), Variant::no_hold_t{});
				// 							  data.emplace_back(p_string(&r.value), Variant::no_hold_t{});
				// 						  },
				// 						  [&data](const cluster::QueryReplicationRecord& r) {
				// 							  data.emplace_back(p_string(&r.sql), Variant::no_hold_t{});
				// 						  },
				// 						  [&data](const cluster::SchemaReplicationRecord& r) {
				// 							  data.emplace_back(p_string(&r.schema), Variant::no_hold_t{});
				// 						  },
				// 						  [&data, &tmpSer, &tmpSlice](const cluster::AddNamespaceReplicationRecord& r) {
				// 							  r.def.GetJSON(tmpSer);
				// 							  tmpSlice = tmpSer.Slice();
				// 							  data.emplace_back(p_string(&tmpSlice), Variant::no_hold_t{});
				// 							  data.emplace_back(r.stateToken);
				// 						  },
				// 						  [&data](const cluster::RenameNamespaceReplicationRecord& r) {
				// 							  data.emplace_back(p_string(&r.dstNsName), Variant::no_hold_t{});
				// 						  },
				// 						  [](const cluster::NodeNetworkCheckRecord&) { assertrx_dbg(false); },
				// 						  [&data, &tmpSer, &tmpSlice](const cluster::TagsMatcherReplicationRecord& r) {
				// 							  r.tm.serialize(tmpSer);
				// 							  tmpSlice = tmpSer.Slice();
				// 							  data.emplace_back(p_string(&tmpSlice), Variant::no_hold_t{});
				// 						  },
				// 						  [](const cluster::ApplyNewShardingCfgRecord&) { assertrx_dbg(false); },
				// 						  [](const cluster::SaveNewShardingCfgRecord&) { assertrx_dbg(false); },
				// 						  [](const cluster::ResetShardingCfgRecord&) { assertrx_dbg(false); },
				// 						  [&data, &tmpSer, &tmpSlice](const cluster::IndexReplicationRecord& r) {
				// 							  r.idef.GetJSON(tmpSer);
				// 							  tmpSlice = tmpSer.Slice();
				// 							  data.emplace_back(p_string(&tmpSlice), Variant::no_hold_t{});
				// 						  }},
				// 			   *rec.data);
				// }
		}
		data.Pack(ser_);
		return ser_.DetachChunk();
	}

private:
	WrSerializer& ser_;
};

}  // namespace reindexer
