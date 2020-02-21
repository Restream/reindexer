#pragma once
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"
#include "ctx/basefunctionctx.h"
#include "nsselectfuncinterface.h"

namespace reindexer {
class NamespaceImpl;

/// Represents sql function in a query
/// (like avg(x) or sum(x)).
class SelectFunction {
public:
	typedef shared_ptr<SelectFunction> Ptr;
	SelectFunction(const Query& q, NsSelectFuncInterface& nm);

	/// Processes selected item to apply sql function.
	/// @param res - ItemRef containing payload value.
	/// @param pl_type - Payload type of item being processed.
	/// @return bool - result of performing operation.
	bool ProcessItem(ItemRef& res, PayloadType& pl_type);
	/// Creates context of function for certain index.
	/// @param indexNo - number of index.
	/// @return pointer to a function context or null if some error happened.
	BaseFunctionCtx::Ptr CreateCtx(int indexNo);

private:
	BaseFunctionCtx::Ptr createCtx(SelectFuncStruct& data, BaseFunctionCtx::Ptr ctx, IndexType index_type);
	void createFunc(SelectFuncStruct& data);
	BaseFunctionCtx::Ptr createFuncForProc(int indexNo);

	/// Containers of functions by index number.
	fast_hash_map<int, SelectFuncStruct> functions_;
	/// Interface to NsSelector object.
	NsSelectFuncInterface nm_;

	/// You won't find these fields in the list of regular indexes
	/// (for example in PayloadType or ns_->indexes), you can only
	/// acces them by Set/GetByJsonPath in PayloadIFace.
	int currCjsonFieldIdx_;
};

/// Keeps all the select functions for each namespace.
class SelectFunctionsHolder {
public:
	/// Creates SelectFunction object for a query.
	/// @param q - query that contains sql function.
	/// @param nm - namespace of the query to be executed.
	/// @param force - forces to create SelectFunction object
	/// even if the list of sql-functions in this query is empty.
	/// @return pointer to SelectFunction object or nullptr in case of error.
	SelectFunction::Ptr AddNamespace(const Query& q, const NamespaceImpl& nm, bool force);
	/// Processing of results of an executed query.
	/// @param res - results of query execution.
	void Process(QueryResults& res);

private:
	/// Indicates if object is empty and was created wuth flag force = true.
	bool force_only_ = true;
	/// Container of sql functions for every namespace.
	std::unique_ptr<fast_hash_map<string, SelectFunction::Ptr>> querys_;
};
}  // namespace reindexer
