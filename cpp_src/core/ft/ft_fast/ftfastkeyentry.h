#pragma once

#include "core/index/keyentry.h"
namespace reindexer {

class FtFastKeyEntry : public KeyEntry<IdSetPlain> {
public:
	size_t vdoc_id_;
};
}  // namespace reindexer
