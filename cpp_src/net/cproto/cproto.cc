#include <unordered_map>

#include "cproto.h"
namespace reindexer {
namespace net {
namespace cproto {

std::unordered_map<int, const char *> kRPCCodes = {
	{kCmdPing, "Ping"},
	{kCmdLogin, "Login"},
	{kCmdOpenDatabase, "OpenDatabase"},
	{kCmdCloseDatabase, "CloseDatabase"},
	{kCmdDropDatabase, "DropDatabase"},
	{kCmdOpenNamespace, "OpenNamespace"},
	{kCmdCloseNamespace, "CloseNamespace"},
	{kCmdDropNamespace, "DropNamespace"},
	{kCmdAddIndex, "AddIndex"},
	{kCmdEnumNamespaces, "EnumNamespaces"},
	{kCmdDropIndex, "DropIndex"},
	{kCmdUpdateIndex, "UpdateIndex"},
	{kCmdAddTxItem, "AddTxItem"},
	{kCmdCommitTx, "CommitTx"},
	{kCmdRollbackTx, "RollbackTx"},
	{kCmdStartTransaction, "StartTransaction"},
	{kCmdCommit, "Commit"},
	{kCmdModifyItem, "ModifyItem"},
	{kCmdDeleteQuery, "DeleteQuery"},
	{kCmdUpdateQuery, "UpdateQuery"},
	{kCmdSelect, "Select"},
	{kCmdSelectSQL, "SelectSQL"},
	{kCmdFetchResults, "FetchResults"},
	{kCmdCloseResults, "CloseResults"},
	{kCmdGetMeta, "GetMeta"},
	{kCmdPutMeta, "PutMeta"},
	{kCmdEnumMeta, "EnumMeta"},
	{kCmdSubscribeUpdates, "SubscribeUpdates"},
	{kCmdUpdates, "Updates"},
	{kCmdGetSQLSuggestions, "GetSQLSuggestions"},
};

const char *CmdName(CmdCode cmd) {
	auto it = kRPCCodes.find(cmd);
	if (it != kRPCCodes.end()) {
		return it->second;
	}
	return "Unknown";
}

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
