#pragma once

#include <stdint.h>
#include <string_view>

namespace reindexer {
namespace net {
namespace cproto {
enum CmdCode : uint16_t {
	kCmdPing = 0,
	kCmdLogin = 1,
	kCmdOpenDatabase = 2,
	kCmdCloseDatabase = 3,
	kCmdDropDatabase = 4,
	kCmdEnumDatabases = 5,
	kCmdOpenNamespace = 16,
	kCmdCloseNamespace = 17,
	kCmdDropNamespace = 18,
	kCmdTruncateNamespace = 19,
	kCmdRenameNamespace = 20,
	kCmdAddIndex = 21,
	kCmdEnumNamespaces = 22,
	kCmdDropIndex = 24,
	kCmdUpdateIndex = 25,

	kCmdAddTxItem = 26,
	kCmdCommitTx = 27,
	kCmdRollbackTx = 28,
	kCmdStartTransaction = 29,
	kCmdDeleteQueryTx = 30,
	kCmdUpdateQueryTx = 31,

	kCmdCommit = 32,
	kCmdModifyItem = 33,
	kCmdDeleteQuery = 34,
	kCmdUpdateQuery = 35,

	kCmdSelect = 48,
	kCmdSelectSQL = 49,
	kCmdFetchResults = 50,
	kCmdCloseResults = 51,

	kCmdGetMeta = 64,
	kCmdPutMeta = 65,
	kCmdEnumMeta = 66,

	kCmdSetSchema = 67,

	kCmdGetReplState = 68,
	kCmdCreateTmpNamespace = 69,
	kCmdGetSnapshot = 70,
	kCmdFetchSnapshot = 71,
	kCmdApplySnapshotCh = 72,
	kCmdSetClusterizationStatus = 73,

	kCmdPutTxMeta = 74,

	kCmdSubscribeUpdates = 90,	// Deprecated
	kCmdUpdates = 91,			// Deprecated

	kCmdGetSQLSuggestions = 92,

	kCmdSuggestLeader = 100,
	kCmdLeadersPing = 101,
	kCmdGetRaftInfo = 102,
	kCmdClusterControlRequest = 103,

	kCmdGetSchema = 110,

	kCmdCodeMax = 128,

};

std::string_view CmdName(uint16_t code);

// Maximum number of active queries per client
const uint32_t kMaxConcurentQueries = 256;

// Maximum number of active snapshots per client
const uint32_t kMaxConcurentSnapshots = 8;

const uint32_t kCprotoMagic = 0xEEDD1132;
const uint32_t kCprotoVersion = 0x103;
const uint32_t kCprotoMinCompatVersion = 0x101;
const uint32_t kCprotoMinSnappyVersion = 0x103;

#pragma pack(push, 1)
struct CProtoHeader {
	uint32_t magic;
	uint16_t version : 10;
	uint16_t compressed : 1;
	uint16_t _reserved : 5;
	uint16_t cmd;
	uint32_t len;
	uint32_t seq;
};

#pragma pack(pop)

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
