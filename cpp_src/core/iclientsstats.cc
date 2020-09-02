#include "iclientsstats.h"
#include "cjson/jsonbuilder.h"
#include "tools/serializer.h"

namespace reindexer {

void ClientStat::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	builder.Put("connection_id", connectionId);
	builder.Put("ip", ip);
	builder.Put("user_name", userName);
	builder.Put("db_name", dbName);
	builder.Put("current_activity", currentActivity);
	builder.Put("sent_bytes", sentBytes);
	builder.Put("recv_bytes", recvBytes);
	builder.Put("user_rights", userRights);
	builder.Put("start_time", startTime);
	builder.Put("client_version", clientVersion);
	builder.Put("app_name", appName);
	builder.Put("tx_count", txCount);
	builder.End();
}

}  // namespace reindexer
