#pragma once

#include <stdint.h>

namespace reindexer {
namespace net {
namespace cproto {
enum CmdCode {
	kCmdPing = 0,
	kCmdLogin = 1,
	kCmdOpenDatabase = 2,
	kCmdCloseDatabase = 3,
	kCmdDropDatabase = 4,
	kCmdOpenNamespace = 16,
	kCmdCloseNamespace = 17,
	kCmdDropNamespace = 18,
	kCmdAddIndex = 21,
	kCmdEnumNamespaces = 22,
	kCmdConfigureIndex = 23,
	kCmdDropIndex = 24,

	kCmdCommit = 32,
	kCmdModifyItem = 33,
	kCmdDeleteQuery = 34,

	kCmdSelect = 48,
	kCmdSelectSQL = 49,
	kCmdFetchResults = 50,
	kCmdCloseResults = 51,

	kCmdGetMeta = 64,
	kCmdPutMeta = 65,
	kCmdEnumMeta = 66,

	kCmdCodeMax = 128
};

const char *CmdName(CmdCode code);

// Maximum number of active queries per cleint
const uint32_t kMaxConcurentQueries = 40;

const uint32_t kCprotoMagic = 0xEEDD1132;
const uint32_t kCprotoVersion = 0x100;

#pragma pack(push, 1)
struct CProtoHeader {
	uint32_t magic;
	uint32_t version;
	uint32_t len;
	uint32_t cmd;
	uint32_t seq;
};
#pragma pack(pop)

}  // namespace cproto
}  // namespace net
}  // namespace reindexer
