#pragma once
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"
#include "nsselectfuncinterface.h"
#include "selectfuncparser.h"

namespace reindexer {
class NamespaceImpl;

/// Represents sql function in a query
/// (like avg(x) or sum(x)).
class SelectFunction : public intrusive_atomic_rc_base {
public:
	typedef intrusive_ptr<SelectFunction> Ptr;
	SelectFunction(const Query& q, NsSelectFuncInterface&& nm);

	/// Processes selected item to apply sql function.
	/// @param res - ItemRef containing payload value.
	/// @param pl_type - Payload type of item being processed.
	/// @param stringsHolder - Holder for temporary strings
	/// @return bool - result of performing operation.
	bool ProcessItem(ItemRef& res, PayloadType& pl_type, std::vector<key_string>& stringsHolder);
	/// Creates context of function for certain index.
	/// @param indexNo - number of index.
	/// @return pointer to a function context or null if some error happened.
	BaseFunctionCtx::Ptr CreateCtx(int indexNo);

private:
	BaseFunctionCtx::Ptr createCtx(SelectFuncStruct& data, BaseFunctionCtx::Ptr ctx, IndexType index_type);
	void createFunc(SelectFuncStruct& data);
	BaseFunctionCtx::Ptr createFuncForProc(int indexNo);

	/// Containers of functions by index number.
	RHashMap<int, SelectFuncStruct> functions_;
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
	/// @param nsid - unique ns ID in the query.
	/// @param force - forces to create SelectFunction object
	/// even if the list of sql-functions in this query is empty.
	/// @return pointer to SelectFunction object or nullptr in case of error.
	SelectFunction::Ptr AddNamespace(const Query& q, const NamespaceImpl& nm, uint32_t nsid, bool force);
	/// Processing of results of an executed query.
	/// @param res - results of query execution.
	void Process(LocalQueryResults& res);

private:
	using MapT = h_vector<SelectFunction::Ptr, 4>;

	/// Indicates if object is empty and was created wuth flag force = true.
	bool force_only_ = true;
	/// Container of sql functions for every namespace.
	MapT queries_;
};
}  // namespace reindexer
