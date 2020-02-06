#include <unordered_map>

#include "cproto.h"

namespace reindexer {
namespace net {
namespace cproto {

string_view CmdName(uint16_t cmd) {
	switch (cmd) {
		case kCmdPing:
			return "Ping"_sv;
		case kCmdLogin:
			return "Login"_sv;
		case kCmdOpenDatabase:
			return "OpenDatabase"_sv;
		case kCmdCloseDatabase:
			return "CloseDatabase"_sv;
		case kCmdDropDatabase:
			return "DropDatabase"_sv;
		case kCmdOpenNamespace:
			return "OpenNamespace"_sv;
		case kCmdCloseNamespace:
			return "CloseNamespace"_sv;
		case kCmdDropNamespace:
			return "DropNamespace"_sv;
		case kCmdTruncateNamespace:
			return "TruncateNamespace"_sv;
		case kCmdRenameNamespace:
			return "RenameNamespace"_sv;
		case kCmdAddIndex:
			return "AddIndex"_sv;
		case kCmdEnumNamespaces:
			return "EnumNamespaces"_sv;
		case kCmdEnumDatabases:
			return "EnumDatabases"_sv;
		case kCmdDropIndex:
			return "DropIndex"_sv;
		case kCmdUpdateIndex:
			return "UpdateIndex"_sv;
		case kCmdAddTxItem:
			return "AddTxItem"_sv;
		case kCmdCommitTx:
			return "CommitTx"_sv;
		case kCmdRollbackTx:
			return "RollbackTx"_sv;
		case kCmdStartTransaction:
			return "StartTransaction"_sv;
		case kCmdDeleteQueryTx:
			return "DeleteQueryTx"_sv;
		case kCmdUpdateQueryTx:
			return "UpdateQueryTx"_sv;
		case kCmdCommit:
			return "Commit"_sv;
		case kCmdModifyItem:
			return "ModifyItem"_sv;
		case kCmdDeleteQuery:
			return "DeleteQuery"_sv;
		case kCmdUpdateQuery:
			return "UpdateQuery"_sv;
		case kCmdSelect:
			return "Select"_sv;
		case kCmdSelectSQL:
			return "SelectSQL"_sv;
		case kCmdFetchResults:
			return "FetchResults"_sv;
		case kCmdCloseResults:
			return "CloseResults"_sv;
		case kCmdGetMeta:
			return "GetMeta"_sv;
		case kCmdPutMeta:
			return "PutMeta"_sv;
		case kCmdEnumMeta:
			return "EnumMeta"_sv;
		case kCmdSubscribeUpdates:
			return "SubscribeUpdates"_sv;
		case kCmdUpdates:
			return "Updates"_sv;
		case kCmdGetSQLSuggestions:
			return "GetSQLSuggestions"_sv;
		default:
			return "Unknown"_sv;
	}
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
