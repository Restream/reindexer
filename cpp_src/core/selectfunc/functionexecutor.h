#pragma once

#include "updates/updaterecord.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

class NamespaceImpl;
struct SelectFuncStruct;
class NsContext;

class FunctionExecutor {
public:
	explicit FunctionExecutor(NamespaceImpl& ns, h_vector<updates::UpdateRecord, 2>& replUpdates) noexcept : ns_(ns), replUpdates_(replUpdates) {}
	Variant Execute(SelectFuncStruct& funcData, const NsContext& ctx);

private:
	NamespaceImpl& ns_;
	h_vector<updates::UpdateRecord, 2>& replUpdates_;
};

}  // namespace reindexer
