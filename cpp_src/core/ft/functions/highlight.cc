#include "highlight.h"
#include "core/ft/ftctx.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/itemref.h"
#include "ft_function.h"

namespace reindexer {

bool Highlight::Process(ItemRef& res, PayloadType& pl_type, const FtFuncStruct& func, std::vector<key_string>& stringsHolder) {
	if (func.funcArgs.size() < 2) {
		throw Error(errParams, "Invalid highlight params need minimum 2 - have {}", func.funcArgs.size());
	}
	if (!func.ctx || func.ctx->Type() != FtCtxType::kFtArea) {
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

	if (kr.empty()) {
		throw Error(errLogic, "Unable to apply highlight function to the non-string field '{}'", func.field);
	}

	auto pva = dataFtCtx.area[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) {
		return false;
	}
	initAreas(kr.size(), pva->GetData());
	sortAndJoinIntersectingAreas();

	plArr_.resize(0);
	plArr_.reserve(kr.size());

	for (size_t arrIdx = 0; arrIdx < kr.size(); arrIdx++) {
		auto& k = kr[arrIdx];
		if (!k.Type().IsSame(KeyValueType::String{})) {
			throw Error(errLogic, "Unable to apply highlight function to the non-string field '{}'", func.field);
		}

		std::string_view data = std::string_view(p_string(k));
		std::string result_string;

		result_string.reserve(data.size() + pva->Size() * (func.funcArgs[0].size() + func.funcArgs[1].size()));
		result_string.append(data);

		auto splitterTask = ftctx->GetData()->splitter->CreateTask();
		splitterTask->SetText(data);
		size_t offset = 0;

		for (auto& area : areasByArrayIdxs_[arrIdx]) {
			std::pair<int, int> pos = ftctx->GetData()->isWordPositions ? splitterTask->Convert(area.first, area.second)
																		: std::make_pair<int, int>(area.first, area.second);

			result_string.insert(pos.first + offset, func.funcArgs[0]);
			offset += func.funcArgs[0].size();

			result_string.insert(pos.second + offset, func.funcArgs[1]);
			offset += func.funcArgs[1].size();
		}

		stringsHolder.emplace_back(make_key_string(std::move(result_string)));
		plArr_.emplace_back(stringsHolder.back());
	}

	res.Value().Clone();
	pl.Set(func.field, plArr_);

	return true;
}

}  // namespace reindexer
