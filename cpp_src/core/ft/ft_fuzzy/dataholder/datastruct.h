#pragma once
#include <stdint.h>
#include <map>
#include <memory>
#include <set>
#include "core/type_consts.h"
#include "smardeque.h"

namespace search_engine {

using std::shared_ptr;
using std::map;
struct IdContext;

typedef map<IdType, IdContext> id_map;
typedef map<PosType, ProcType> proc_map;

struct [[nodiscard]] DataStruct {};

struct [[nodiscard]] IdContext {
	uint16_t proc_;
	uint8_t pos[2];
	uint32_t tota_size_;
};

struct [[nodiscard]] Info {
	SmartDeque<IdContext, 100> true_ids_;
};

struct [[nodiscard]] SeacrhResult {
	IdType id;
	ProcType procent;
};
}  // namespace search_engine
