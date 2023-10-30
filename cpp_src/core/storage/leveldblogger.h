#pragma once

namespace leveldb {
class Options;
}

namespace reindexer {
namespace datastorage {

void SetDummyLogger(leveldb::Options& options);

}
}  // namespace reindexer
