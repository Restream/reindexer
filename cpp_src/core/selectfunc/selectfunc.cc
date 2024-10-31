#include "selectfunc.h"
#include <cctype>
#include <memory>
#include "core/payload/fieldsset.h"
#include "ctx/ftctx.h"
#include "functions/highlight.h"
#include "functions/snippet.h"

namespace reindexer {

inline void ltrim(std::string& s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
inline void rtrim(std::string& s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

// trim from both ends (in place)
inline void trim(std::string& s) {
	ltrim(s);
	rtrim(s);
}

SelectFunction::Ptr SelectFunctionsHolder::AddNamespace(const Query& q, const NamespaceImpl& nm, uint32_t nsid, bool force) {
	if (q.selectFunctions_.empty() && !force) {
		return nullptr;
	} else if (!q.selectFunctions_.empty()) {
		force_only_ = false;
	}

	if (queries_.size() <= nsid) {
		queries_.resize(nsid + 1);
	}
	queries_[nsid] = make_intrusive<SelectFunction>(q, NsSelectFuncInterface(nm));
	return queries_[nsid];
}

SelectFunction::SelectFunction(const Query& q, NsSelectFuncInterface&& nm) : nm_(std::move(nm)), currCjsonFieldIdx_(nm_.getIndexesCount()) {
	for (auto& func : q.selectFunctions_) {
		SelectFuncParser parser;
		SelectFuncStruct& result = parser.Parse(func);
		if (!result.isFunction) {
			continue;
		}
		createFunc(result);
	}
}

void SelectFunction::createFunc(SelectFuncStruct& data) {
	int indexNo = IndexValueType::NotSet;
	if (data.indexNo == IndexValueType::NotSet) {
		if (!nm_.getIndexByName(data.field, indexNo)) {
			trim(data.field);
			if (!nm_.getIndexByName(data.field, indexNo)) {
				return;
			}
		}
	} else {
		indexNo = data.indexNo;
	}

	// if index is composite then create function for inner use only
	if (IsComposite(nm_.getIndexType(indexNo))) {
		int fieldNo = 0;
		const FieldsSet& fields = nm_.getIndexFields(indexNo);

		int jsPathIdx = 0;
		for (auto field : fields) {
			data.fieldNo = fieldNo;
			if (field == IndexValueType::SetByJsonPath) {
				data.field = fields.getJsonPath(jsPathIdx);
				data.tagsPath = nm_.getTagsPathForField(data.field);
				data.indexNo = currCjsonFieldIdx_++;
				functions_.emplace(data.indexNo, data);
			} else {
				data.field = nm_.getIndexName(field);
				data.indexNo = field;
				functions_.emplace(field, data);
			}
		}
	} else {
		data.indexNo = indexNo;
		data.field = nm_.getIndexName(indexNo);
		data.fieldNo = 0;
		functions_.emplace(std::make_pair(indexNo, data));
	}
}

BaseFunctionCtx::Ptr SelectFunction::createFuncForProc(int indexNo) {
	SelectFuncStruct data;
	data.isFunction = true;
	data.indexNo = indexNo;
	int lastCjsonIdx = currCjsonFieldIdx_;
	createFunc(data);
	if (IsComposite(nm_.getIndexType(indexNo))) {
		auto field = nm_.getIndexFields(indexNo)[0];
		if (field == IndexValueType::SetByJsonPath) {
			field = lastCjsonIdx;
		}
		auto it = functions_.find(field);
		assertrx(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo));
	} else {
		auto it = functions_.find(indexNo);
		assertrx(it != functions_.end());
		return createCtx(it->second, nullptr, nm_.getIndexType(indexNo));
	}
}

BaseFunctionCtx::Ptr SelectFunction::CreateCtx(int indexNo) {
	// we use this hack because ft always needs ctx to generate proc in response
	if (functions_.empty() && IsFullText(nm_.getIndexType(indexNo))) {
		return createFuncForProc(indexNo);
	} else if (functions_.empty()) {
		return nullptr;
	}

	BaseFunctionCtx::Ptr ctx;
	const IndexType indexType = nm_.getIndexType(indexNo);

	if (IsComposite(indexType)) {
		int fieldNo = 0;
		int cjsonFieldIdx = nm_.getIndexesCount();
		for (auto field : nm_.getIndexFields(indexNo)) {
			if (field == IndexValueType::SetByJsonPath) {
				field = cjsonFieldIdx++;
			}
			auto it = functions_.find(field);
			if (it != functions_.end()) {
				it->second.fieldNo = fieldNo;
				ctx = createCtx(it->second, ctx, indexType);
			}
			fieldNo++;
		}
	} else {
		auto it = functions_.find(indexNo);
		if (it != functions_.end()) {
			it->second.fieldNo = 0;
			ctx = createCtx(it->second, ctx, indexType);
		}
	}
	if (!ctx && IsFullText(indexType)) {
		return createFuncForProc(indexNo);
	}
	return ctx;
}
void SelectFunctionsHolder::Process(LocalQueryResults& res) {
	if (queries_.empty() || force_only_) {
		return;
	}
	bool hasFuncs = false;
	for (auto& q : queries_) {
		if (q) {
			hasFuncs = true;
			break;
		}
	}
	if (!hasFuncs) {
		return;
	}

	bool changed = false;
	for (auto& item : res.Items()) {
		const auto nsid = item.Nsid();
		if (queries_.size() <= nsid) {
			continue;
		}
		if (auto& funcPtr = queries_[nsid]; funcPtr && funcPtr->ProcessItem(item, res.getPayloadType(nsid), res.stringsHolder_)) {
			changed = true;
		}
	}
	res.nonCacheableData = changed;
}
bool SelectFunction::ProcessItem(ItemRef& res, PayloadType& pl_type, std::vector<key_string>& stringsHolder) {
	bool changed = false;
	for (auto& func : functions_) {
		if (func.second.ctx &&
			std::visit([&](auto& f) -> bool { return f.Process(res, pl_type, func.second, stringsHolder); }, func.second.func)) {
			changed = true;
		}
	}
	return changed;
}

BaseFunctionCtx::Ptr SelectFunction::createCtx(SelectFuncStruct& data, BaseFunctionCtx::Ptr ctx, IndexType index_type) {
	if (IsFullText(index_type)) {
		if (!ctx) {
			switch (SelectFuncType(data.func.index())) {
				case SelectFuncType::None:
					data.ctx = make_intrusive<FtCtx>(BaseFunctionCtx::CtxType::kFtCtx);
					break;
				case SelectFuncType::Snippet:
				case SelectFuncType::Highlight:
				case SelectFuncType::SnippetN:
					data.ctx = make_intrusive<FtCtx>(BaseFunctionCtx::CtxType::kFtArea);
					break;
				case SelectFuncType::DebugRank:
					data.ctx = make_intrusive<FtCtx>(BaseFunctionCtx::CtxType::kFtAreaDebug);
					break;
				case SelectFuncType::Max:
					throw reindexer::Error(errLogic, "incorrect function type 'Max'");
			}
		} else {
			switch (SelectFuncType(data.func.index())) {
				case SelectFuncType::None:
					if (ctx->type != BaseFunctionCtx::CtxType::kFtCtx) {
						throw reindexer::Error(errLogic, "The existing calling context type '%d' does not allow this function",
											   int(ctx->type));
					}
					break;
				case SelectFuncType::Snippet:
				case SelectFuncType::Highlight:
				case SelectFuncType::SnippetN:
					if (ctx->type != BaseFunctionCtx::CtxType::kFtArea) {
						throw reindexer::Error(errLogic, "The existing calling context type '%d' does not allow this function",
											   int(ctx->type));
					}
					break;
				case SelectFuncType::DebugRank:
					if (ctx->type != BaseFunctionCtx::CtxType::kFtAreaDebug) {
						throw reindexer::Error(errLogic, "The existing calling context type '%d' does not allow this function",
											   int(ctx->type));
					}
					break;
				case SelectFuncType::Max:
					throw reindexer::Error(errLogic, "incorrect function type 'Max'");
			}
			data.ctx = std::move(ctx);
		}
		const std::string& indexName = (data.indexNo >= nm_.getIndexesCount()) ? data.field : nm_.getIndexName(data.indexNo);
		data.ctx->AddFunction(indexName, SelectFuncType(data.func.index()));
	}
	return data.ctx;
}

}  // namespace reindexer
