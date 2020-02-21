#pragma once

#include "core/keyvalue/variant.h"

namespace reindexer {

class NamespaceImpl;
struct SelectFuncStruct;

class FunctionExecutor {
public:
	explicit FunctionExecutor(NamespaceImpl& ns);
	Variant Execute(SelectFuncStruct& funcData);

private:
	NamespaceImpl& ns_;
};

}  // namespace reindexer
