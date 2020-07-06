#pragma once

#include <stdint.h>

#include "estl/string_view.h"

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

	kCmdSubscribeUpdates = 90,
	kCmdUpdates = 91,

	kCmdGetSQLSuggestions = 92,

	kCmdCodeMax = 128
};

string_view CmdName(uint16_t code);

// Maximum number of active queries per client
const uint32_t kMaxConcurentQueries = 256;

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
