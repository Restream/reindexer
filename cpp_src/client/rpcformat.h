#pragma once

#include "core/type_consts.h"

namespace reindexer {
namespace client {

enum class [[nodiscard]] RPCDataFormat { CJSON = DataFormat::FormatCJson, MsgPack = DataFormat::FormatMsgPack };

}
}  // namespace reindexer
