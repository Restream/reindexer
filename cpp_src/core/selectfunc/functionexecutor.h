#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class Namespace;
struct SelectFuncStruct;

class FunctionExecutor {
public:
	explicit FunctionExecutor(Namespace& ns);
	Variant Execute(SelectFuncStruct& funcData);

private:
	Namespace& ns_;
};

}  // namespace reindexer
