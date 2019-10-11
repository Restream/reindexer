#pragma once

#include "core/payload/payloadvalue.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "tools/logger.h"

namespace reindexer {

static const int kDefaultQueryResultsSize = 32;
struct ItemRef {
	ItemRef() = default;
	ItemRef(IdType iid, const PayloadValue &ivalue, uint16_t iproc = 0, uint16_t insid = 0, bool iraw = false)
		: id(iid), proc(iproc), raw(iraw), nsid(insid), value(ivalue) {}

	IdType id = 0;
	uint16_t proc : 15;
	uint16_t raw : 1;
	uint16_t nsid = 0;
	PayloadValue value;
};

using ItemRefVector = h_vector<ItemRef, kDefaultQueryResultsSize>;
}  // namespace reindexer
