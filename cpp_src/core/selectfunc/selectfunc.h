#pragma once
#include <set>
#include "core/query/query.h"
#include "core/query/queryresults.h"
#include "ctx/basefunctionctx.h"
#include "nsselectfuncinterface.h"
#include "selectfuncparser.h"

namespace reindexer {
class Namespace;

class SelectFunction {
public:
	typedef shared_ptr<SelectFunction> Ptr;
	SelectFunction(const Query& q, NsSelectFuncInterface& nm);

	bool ProcessItem(ItemRef& res, PayloadType& pl_type);
	BaseFunctionCtx::Ptr CreateCtx(int IndexNo);
	bool Empty() const { return functions_.empty(); }

private:
	BaseFunctionCtx::Ptr createCtx(SelectFuncStruct& data, BaseFunctionCtx::Ptr ctx, IndexType index_type);
	void createFunc(SelectFuncStruct& data);
	BaseFunctionCtx::Ptr createFuncForProc(int indexNo);

	fast_hash_map<int, SelectFuncStruct> functions_;
	NsSelectFuncInterface nm_;

	// You won't find these fields in the list of regular indexes
	// (for example in PayloadType or ns_->indexes), you can only
	// acces them by Set/GetByJsonPath in PayloadIFace.
	int currCjsonFieldIdx_;
};

class SelectFunctionsHolder {
public:
	SelectFunction::Ptr AddNamespace(const Query& q, const Namespace& nm, bool force);
	void Process(QueryResults& res);

private:
	bool force_only_ = true;
	unique_ptr<fast_hash_map<string, SelectFunction::Ptr>> querys_;
};
}  // namespace reindexer
