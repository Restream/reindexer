#include "highlight.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/ctx/ftctx.h"
namespace reindexer {

bool Highlight::Process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func, std::vector<key_string> &stringsHolder) {
	if (func.funcArgs.size() < 2) throw Error(errParams, "Invalid highlight params need minimum 2 - have %d", func.funcArgs.size());

	if (!func.ctx || func.ctx->type != BaseFunctionCtx::kFtCtx) return false;

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(func.ctx);
	auto dataFtCtx = ftctx->GetData();
	auto it = dataFtCtx->holders_.find(res.Id());
	if (it == dataFtCtx->holders_.end()) {
		return false;
	}

	Payload pl(pl_type, res.Value());

	VariantArray kr;
	if (func.tagsPath.empty()) {
		pl.Get(func.field, kr);
	} else {
		pl.GetByJsonPath(func.tagsPath, kr, KeyValueType::Undefined{});
	}

	if (kr.empty() || !kr[0].Type().IsSame(KeyValueType::String{})) {
		throw Error(errLogic, "Unable to apply highlight function to the non-string field '%s'", func.field);
	}

	const std::string *data = p_string(kr[0]).getCxxstr();
	auto pva = dataFtCtx->area_[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) return false;
	auto &va = *pva;

	std::string result_string;
	result_string.reserve(data->size() + va.Size() * (func.funcArgs[0].size() + func.funcArgs[1].size()));
	result_string = *data;

	Word2PosHelper word2pos(*data, ftctx->GetData()->extraWordSymbols_);

	int offset = 0;
	for (auto area : va.GetData()) {
		std::pair<int, int> pos =
			ftctx->GetData()->isWordPositions_ ? word2pos.convert(area.start, area.end) : std::make_pair(area.start, area.end);

		result_string.insert(pos.first + offset, func.funcArgs[0]);
		offset += func.funcArgs[0].size();

		result_string.insert(pos.second + offset, func.funcArgs[1]);

		offset += func.funcArgs[1].size();
	}

	stringsHolder.emplace_back(make_key_string(std::move(result_string)));
	res.Value().Clone();

	if (func.tagsPath.empty()) {
		pl.Set(func.field, Variant{stringsHolder.back()});
	} else {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}

	return true;
}

}  // namespace reindexer
