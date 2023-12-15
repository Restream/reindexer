#pragma once

namespace leveldb {
struct Options;
}

namespace reindexer {
namespace datastorage {

void SetDummyLogger(leveldb::Options& options);

}
}  // namespace reindexer
