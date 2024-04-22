#include "cproto.h"

namespace reindexer {
namespace net {
namespace cproto {

std::string_view CmdName(uint16_t cmd) noexcept {
	using namespace std::string_view_literals;
	switch (cmd) {
		case kCmdPing:
			return "Ping"sv;
		case kCmdLogin:
			return "Login"sv;
		case kCmdOpenDatabase:
			return "OpenDatabase"sv;
		case kCmdCloseDatabase:
			return "CloseDatabase"sv;
		case kCmdDropDatabase:
			return "DropDatabase"sv;
		case kCmdEnumDatabases:
			return "EnumDatabases"sv;
		case kCmdOpenNamespace:
			return "OpenNamespace"sv;
		case kCmdCloseNamespace:
			return "CloseNamespace"sv;
		case kCmdDropNamespace:
			return "DropNamespace"sv;
		case kCmdTruncateNamespace:
			return "TruncateNamespace"sv;
		case kCmdRenameNamespace:
			return "RenameNamespace"sv;
		case kCmdAddIndex:
			return "AddIndex"sv;
		case kCmdEnumNamespaces:
			return "EnumNamespaces"sv;
		case kCmdDropIndex:
			return "DropIndex"sv;
		case kCmdUpdateIndex:
			return "UpdateIndex"sv;
		case kCmdAddTxItem:
			return "AddTxItem"sv;
		case kCmdCommitTx:
			return "CommitTx"sv;
		case kCmdRollbackTx:
			return "RollbackTx"sv;
		case kCmdStartTransaction:
			return "StartTransaction"sv;
		case kCmdDeleteQueryTx:
			return "DeleteQueryTx"sv;
		case kCmdUpdateQueryTx:
			return "UpdateQueryTx"sv;
		case kCmdCommit:
			return "Commit"sv;
		case kCmdModifyItem:
			return "ModifyItem"sv;
		case kCmdDeleteQuery:
			return "DeleteQuery"sv;
		case kCmdUpdateQuery:
			return "UpdateQuery"sv;
		case kCmdSelect:
			return "Select"sv;
		case kCmdSelectSQL:
			return "SelectSQL"sv;
		case kCmdFetchResults:
			return "FetchResults"sv;
		case kCmdCloseResults:
			return "CloseResults"sv;
		case kShardingControlRequest:
			return "ShardingControlRequest"sv;
		case kCmdGetMeta:
			return "GetMeta"sv;
		case kCmdPutMeta:
			return "PutMeta"sv;
		case kCmdEnumMeta:
			return "EnumMeta"sv;
		case kCmdDeleteMeta:
			return "DeleteMeta"sv;
		case kCmdSetSchema:
			return "SetSchema"sv;
		case kCmdGetReplState:
			return "GetReplState"sv;
		case kCmdCreateTmpNamespace:
			return "CreateTmpNamespace"sv;
		case kCmdSubscribeUpdates:
			return "SubscribeUpdates"sv;
		case kCmdUpdates:
			return "Updates"sv;
		case kCmdGetSQLSuggestions:
			return "GetSQLSuggestions"sv;
		case kCmdGetSnapshot:
			return "GetSnapshot"sv;
		case kCmdFetchSnapshot:
			return "FetchSnapshot"sv;
		case kCmdApplySnapshotCh:
			return "ApplySnapshotChunk"sv;
		case kCmdSetClusterizationStatus:
			return "SetClusterizationStatus"sv;
		case kCmdSuggestLeader:
			return "SuggestLeader"sv;
		case kCmdLeadersPing:
			return "LeadersPing"sv;
		case kCmdGetRaftInfo:
			return "GetRaftInfo"sv;
		case kCmdGetSchema:
			return "GetSchema"sv;
		case kCmdClusterControlRequest:
			return "ClusterControlRequest"sv;
		case kCmdSetTagsMatcherTx:
			return "kCmdSetTagsMatcherTx"sv;
		case kCmdSetTagsMatcher:
			return "kCmdSetTagsMatcher"sv;
		default:
			return "Unknown"sv;
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
