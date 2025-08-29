#pragma once
#include "core/ft/ftctx.h"
#include "core/namespace/ns_ft_func_interface.h"
#include "core/query/expression/function_parser.h"
#include "core/query/query.h"
#include "debugrank.h"
#include "highlight.h"
#include "snippet.h"

namespace reindexer {

class NamespaceImpl;
class LocalQueryResults;
class ItemRef;

struct FtFuncStruct;

class [[nodiscard]] FuncNone {
public:
	bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>&) noexcept { return false; }
};

struct [[nodiscard]] FtFuncStruct : public ParsedQueryFunction {
	explicit FtFuncStruct(ParsedQueryFunction&&);

	FtFuncVariant func{std::in_place_type<FuncNone>};
	FtCtx::Ptr ctx;
	TagsPath tagsPath;
	int indexNo = IndexValueType::NotSet;
	int fieldNo = 0;
};

/// Represents sql function in a query
/// (like avg(x) or sum(x))
class [[nodiscard]] FtFunction : public intrusive_atomic_rc_base {
public:
	typedef intrusive_ptr<FtFunction> Ptr;
	FtFunction(const Query& q, NsFtFuncInterface&& nm);

	/// Processes selected item to apply sql function.
	/// @param res - ItemRef containing payload value.
	/// @param pl_type - Payload type of item being processed.
	/// @param stringsHolder - Holder for temporary strings
	/// @return bool - result of performing operation.
	bool ProcessItem(ItemRef& res, PayloadType& pl_type, std::vector<key_string>& stringsHolder);
	/// Creates context of function for certain index.
	/// @param indexNo - number of index.
	/// @return pointer to a function context or null if some error happened.
	FtCtx::Ptr CreateCtx(int indexNo, const RanksHolder::Ptr&);
	bool Empty() const noexcept;

private:
	FtCtx::Ptr createCtx(FtFuncStruct& data, FtCtx::Ptr ctx, IndexType index_type, const RanksHolder::Ptr&);
	void createFunc(FtFuncStruct&&);
	FtCtx::Ptr createFuncForRank(int indexNo, const RanksHolder::Ptr&);

	/// Containers of functions by index number.
	RHashMap<int, FtFuncStruct> functions_;
	/// Interface to NsSelector object.
	NsFtFuncInterface nm_;

	/// You won't find these fields in the list of regular indexes
	/// (for example in PayloadType or ns_->indexes), you can only
	/// access them by Set/GetByJsonPath in PayloadIFace.
	int currCjsonFieldIdx_;
};

/// Keeps all the select functions for each namespace.
class [[nodiscard]] FtFunctionsHolder {
public:
	/// Creates FtFunction object for a query.
	/// @param q - query that contains sql function.
	/// @param nm - namespace of the query to be executed.
	/// @param nsid - unique ns ID in the query.
	/// @param force - forces to create FtFunction object
	/// even if the list of sql-functions in this query is empty.
	/// @return pointer to FtFunction object or nullptr in case of error.
	FtFunction::Ptr AddNamespace(const Query& q, const NamespaceImpl& nm, uint32_t nsid, bool force);
	/// Processing of results of an executed query.
	/// @param res - results of query execution.
	void Process(LocalQueryResults& res) const;

private:
	using MapT = h_vector<FtFunction::Ptr, 4>;

	/// Indicates if object is empty and was created with flag force = true.
	bool force_only_ = true;
	/// Container of sql functions for every namespace.
	MapT queries_;
};
}  // namespace reindexer
