#pragma once
#include <stdint.h>
#include <map>
#include <memory>
#include "core/idset.h"

namespace search_engine {

using std::shared_ptr;
using std::map;

struct IdContext;
typedef float ProcType;
typedef uint32_t HashType;
typedef uint16_t PosType;
typedef map<IdType, IdContext> id_map;
typedef map<PosType, ProcType> proc_map;

struct IdContext {
	proc_map proc_;
	uint32_t tota_size_;
};

struct Info {
	id_map ids_;
};

// most hot place
struct DataStruct {
	// TODO think about this fuking pointer

	Info *info;
	uint32_t hash;
};

struct SeacrhResult {
	IdType id;
	ProcType procent;
};
}  // namespace search_engine
