#include "highlight.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/itemref.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfuncparser.h"
#include "core/queryresults/itemref.h"

namespace reindexer {

bool Highlight::Process(ItemRef& res, PayloadType& pl_type, const SelectFuncStruct& func, std::vector<key_string>& stringsHolder) {
	if (func.funcArgs.size() < 2) {
		throw Error(errParams, "Invalid highlight params need minimum 2 - have {}", func.funcArgs.size());
	}
	if (!func.ctx || func.ctx->Type() != BaseFunctionCtx::CtxType::kFtArea) {
		return false;
	}
	if (!func.tagsPath.empty()) {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}
	FtCtx::Ptr ftctx = reindexer::static_ctx_pointer_cast<FtCtx>(func.ctx);
	FtCtxAreaData<Area>& dataFtCtx = *(reindexer::static_ctx_pointer_cast<FtCtxAreaData<Area>>(ftctx->GetData()));
	if (!dataFtCtx.holders.has_value()) {
		return false;
	}

	auto it = dataFtCtx.holders->find(res.Id());
	if (it == dataFtCtx.holders->end()) {
		return false;
	}

	Payload pl(pl_type, res.Value());

	VariantArray kr;
	pl.Get(func.field, kr);

	if (kr.empty() || !kr[0].Type().IsSame(KeyValueType::String{})) {
		throw Error(errLogic, "Unable to apply highlight function to the non-string field '{}'", func.field);
	}

	const std::string_view data = std::string_view(p_string(kr[0]));

	auto pva = dataFtCtx.area[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) {
		return false;
	}
	auto& va = *pva;

	std::string result_string;
	result_string.reserve(data.size() + va.Size() * (func.funcArgs[0].size() + func.funcArgs[1].size()));
	result_string.append(data);

	auto splitterTask = ftctx->GetData()->splitter->CreateTask();
	splitterTask->SetText(data);

	int offset = 0;
	for (auto area : va.GetData()) {
		std::pair<int, int> pos =
			ftctx->GetData()->isWordPositions ? splitterTask->Convert(area.start, area.end) : std::make_pair(area.start, area.end);

		result_string.insert(pos.first + offset, func.funcArgs[0]);
		offset += func.funcArgs[0].size();

		result_string.insert(pos.second + offset, func.funcArgs[1]);
		offset += func.funcArgs[1].size();
	}

	stringsHolder.emplace_back(make_key_string(std::move(result_string)));
	res.Value().Clone();

	pl.Set(func.field, Variant{stringsHolder.back()});

	return true;
}

}  // namespace reindexer
