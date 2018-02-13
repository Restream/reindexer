#pragma once

#include <stdint.h>

namespace reindexer {
namespace net {
namespace cproto {
enum CmdCode {
	kCmdPing = 0,
	kCmdOpenNamespace = 1,
	kCmdCloseNamespace = 2,
	kCmdDropNamespace = 3,
	kCmdRenameNamespace = 4,
	kCmdCloneNamespace = 5,
	kCmdAddIndex = 6,
	kCmdEnumNamespaces = 7,
	kCmdConfigureIndex = 8,

	kCmdCommit = 16,
	kCmdModifyItem = 17,
	kCmdDeleteQuery = 18,

	kCmdSelect = 32,
	kCmdSelectSQL = 33,
	kCmdFetchResults = 34,

	kCmdGetMeta = 48,
	kCmdPutMeta = 49,
	kCmdEnumMeta = 50,

	kCmdCodeMax = 128
};

// Maximum number of active queries per cleint
const uint32_t kMaxConcurentQueries = 4;

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
