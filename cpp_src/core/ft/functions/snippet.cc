#include "snippet.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/itemref.h"
#include "ft_function.h"
#include "highlight.h"
#include "tools/errors.h"

namespace reindexer {

void Snippet::init(const FtFuncStruct& func) {
	if (isInit_) {
		return;
	}
	if (func.funcArgs.size() < 4) {
		throw Error(errParams, "Invalid snippet params need minimum 4 - have {}", func.funcArgs.size());
	}
	try {
		std::size_t pos;
		before_ = stoul(func.funcArgs[2], &pos);
		if (pos != func.funcArgs[2].size()) {
			throw Error(errParams, "Invalid snippet param before - {} is not a number", func.funcArgs[2]);
		}
	} catch (std::exception&) {
		throw Error(errParams, "Invalid snippet param before - {} is not a number", func.funcArgs[2]);
	}

	try {
		std::size_t pos;
		after_ = stoul(func.funcArgs[3], &pos);
		if (pos != func.funcArgs[3].size()) {
			throw Error(errParams, "Invalid snippet param after - {} is not a number", func.funcArgs[3]);
		}
	} catch (std::exception&) {
		throw Error(errParams, "Invalid snippet param after - {} is not a number", func.funcArgs[3]);
	}
	if (std::holds_alternative<Snippet>(func.func)) {
		if (func.funcArgs.size() > 4) {
			preDelim_ = func.funcArgs[4];
		}
		if (func.funcArgs.size() > 5) {
			postDelim_ = func.funcArgs[5];
		}
	} else if (std::holds_alternative<SnippetN>(func.func)) {
		{
			auto namedArg = func.namedArgs.find("pre_delim");
			if (namedArg != func.namedArgs.end()) {
				preDelim_ = namedArg->second;
			}
		}
		{
			auto namedArg = func.namedArgs.find("post_delim");
			if (namedArg != func.namedArgs.end()) {
				postDelim_ = namedArg->second;
			}
		}
	}
	if (auto f = func.namedArgs.find("with_area"); f != func.namedArgs.end() && f->second == "1") {
		needAreaStr_ = true;
	}
	if (auto f = func.namedArgs.find("left_bound"); f != func.namedArgs.end() && !f->second.empty()) {
		leftBound_ = f->second;
	}
	if (auto f = func.namedArgs.find("right_bound"); f != func.namedArgs.end() && !f->second.empty()) {
		rightBound_ = f->second;
	}

	markerBefore_ = func.funcArgs[0];
	markerAfter_ = func.funcArgs[1];

	isInit_ = true;
}

void Snippet::addSnippet(std::string& resultString, std::string_view data, const Area& snippetAreaPrev,
						 const Area& snippetAreaPrevChar) const {
	resultString.append(preDelim_);

	resultString += '[';
	resultString += std::to_string(snippetAreaPrevChar.start);
	resultString += ',';
	resultString += std::to_string(snippetAreaPrevChar.end);
	resultString += ']';

	resultString.append(data.begin() + snippetAreaPrev.start, data.begin() + zonesList_[0].start);
	bool firstZone = true;
	size_t z = 0;
	for (; z < zonesList_.size(); z++) {
		if (!firstZone) {
			resultString.append(data.begin() + zonesList_[z - 1].end, data.begin() + zonesList_[z].start);
		}
		resultString.append(markerBefore_);
		resultString.append(data.begin() + zonesList_[z].start, data.begin() + zonesList_[z].end);
		resultString.append(markerAfter_);
		firstZone = false;
	}
	resultString.append(data.begin() + zonesList_[z - 1].end, data.begin() + snippetAreaPrev.end);
	resultString.append(postDelim_);
}

struct [[nodiscard]] Areas {
	Area zoneArea;
	Area snippetArea;
};

struct [[nodiscard]] AreasEx : public Areas {
	Area snippetAreaChar;
};

template <typename A>
A Snippet::RecalcZoneHelper::RecalcZoneToOffset(const std::pair<unsigned, unsigned>& area) {
	using PointType = std::conditional_t<std::is_same_v<Areas, A>, WordPosition, WordPositionEx>;
	constexpr bool needChar = std::is_same_v<AreasEx, A>;
	A outAreas;
	auto splitterTask = splitter_->CreateTask();
	splitterTask->SetText(str_);

	PointType p1;
	splitterTask->WordToByteAndCharPos(area.first - wordCount_, p1);

	wordCount_ += area.first - wordCount_;
	outAreas.zoneArea.start = p1.StartByte() + stringBeginOffsetByte_;
	if (area.second - area.first > 1) {
		wordCount_++;
		stringBeginOffsetByte_ += p1.EndByte();
		if constexpr (needChar) {
			outAreas.snippetAreaChar.start = p1.start.ch + stringBeginOffsetChar_;
			stringBeginOffsetChar_ += p1.end.ch;
		}
		str_ = str_.substr(p1.EndByte());
		splitterTask->SetText(str_);
		PointType p2;
		splitterTask->WordToByteAndCharPos(area.second - wordCount_ - 1, p2);
		wordCount_ += area.second - wordCount_;
		outAreas.zoneArea.end = p2.EndByte() + stringBeginOffsetByte_;
		if constexpr (needChar) {
			outAreas.snippetAreaChar.end = p2.end.ch + stringBeginOffsetChar_;
			stringBeginOffsetChar_ += p2.end.ch;
		}

		str_ = str_.substr(p2.EndByte());
		stringBeginOffsetByte_ += p2.EndByte();

	} else {
		outAreas.zoneArea.end = p1.EndByte() + stringBeginOffsetByte_;
		if constexpr (needChar) {
			outAreas.snippetAreaChar.start = p1.start.ch + stringBeginOffsetChar_;
			outAreas.snippetAreaChar.end = p1.end.ch + stringBeginOffsetChar_;
			stringBeginOffsetChar_ += p1.end.ch;
		}
		str_ = str_.substr(p1.EndByte());
		stringBeginOffsetByte_ += p1.EndByte();
		wordCount_++;
	}

	// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
	auto b = calcUtf8BeforeDelims(data_.data(), outAreas.zoneArea.start, before_, leftBound_);
	auto a = calcUtf8AfterDelims({data_.data() + outAreas.zoneArea.end, data_.size() - outAreas.zoneArea.end}, after_, rightBound_);
	outAreas.snippetArea.start = outAreas.zoneArea.start - b.first;
	outAreas.snippetArea.end = outAreas.zoneArea.end + a.first;
	if constexpr (needChar) {
		outAreas.snippetAreaChar.start -= b.second;
		outAreas.snippetAreaChar.end += a.second;
	}
	return outAreas;
}

void Snippet::buildResult(RecalcZoneHelper& recalcZoneHelper, const h_vector<std::pair<unsigned, unsigned>, 10>& areas,
						  std::string_view data, std::string& resultString) {
	// resultString = preDelim_ + with_area_str + data_str_before + marker_before + zone_str + marker_after + data_strAfter + postDelim_
	Area snippetAreaPrev;
	zonesList_.clear<false>();

	for (const auto& area : areas) {
		Areas a = recalcZoneHelper.template RecalcZoneToOffset<Areas>(area);

		if (snippetAreaPrev.start == 0 && snippetAreaPrev.end == 0) {
			snippetAreaPrev = a.snippetArea;
			zonesList_.emplace_back(a.zoneArea);

			resultString.append(preDelim_);
			resultString.append(data.begin() + snippetAreaPrev.start, data.begin() + a.zoneArea.start);
			resultString.append(markerBefore_);
			resultString.append(data.begin() + a.zoneArea.start, data.begin() + a.zoneArea.end);
			resultString.append(markerAfter_);
		} else {
			if (snippetAreaPrev.Concat(a.snippetArea)) {
				resultString.append(data.begin() + zonesList_[0].end, data.begin() + a.zoneArea.start);
				resultString.append(markerBefore_);
				resultString.append(data.begin() + a.zoneArea.start, data.begin() + a.zoneArea.end);
				resultString.append(markerAfter_);
				zonesList_[0] = a.zoneArea;
			} else {
				resultString.append(data.begin() + zonesList_[0].end, data.begin() + snippetAreaPrev.end);
				resultString.append(postDelim_);
				resultString.append(preDelim_);
				resultString.append(data.begin() + a.snippetArea.start, data.begin() + a.zoneArea.start);
				resultString.append(markerBefore_);
				resultString.append(data.begin() + a.zoneArea.start, data.begin() + a.zoneArea.end);
				resultString.append(markerAfter_);
				zonesList_.clear<false>();
				snippetAreaPrev = a.snippetArea;
				zonesList_.emplace_back(a.zoneArea);
			}
		}
	}
	resultString.append(data.begin() + zonesList_[0].end, data.begin() + snippetAreaPrev.end);
	resultString.append(postDelim_);
}

void Snippet::buildResultWithPrefix(RecalcZoneHelper& recalcZoneHelper, const h_vector<std::pair<unsigned, unsigned>, 10>& areas,
									std::string_view data, std::string& resultString) {
	// resultString = preDelim_ + with_area_str + data_str_before + marker_before + zone_str + marker_after + data_strAfter + postDelim_
	Area snippetAreaPrev;
	Area snippetAreaPrevChar;
	zonesList_.clear<false>();

	for (const auto& area : areas) {
		AreasEx a = recalcZoneHelper.template RecalcZoneToOffset<AreasEx>(area);
		if (snippetAreaPrev.start == 0 && snippetAreaPrev.end == 0) {
			snippetAreaPrev = a.snippetArea;
			snippetAreaPrevChar = a.snippetAreaChar;
			zonesList_.emplace_back(a.zoneArea);
		} else {
			if (snippetAreaPrev.Concat(a.snippetArea)) {
				[[maybe_unused]] bool r = snippetAreaPrevChar.Concat(a.snippetAreaChar);
				zonesList_.emplace_back(a.zoneArea);
			} else {
				addSnippet(resultString, data, snippetAreaPrev, snippetAreaPrevChar);
				zonesList_.clear<false>();
				snippetAreaPrevChar = a.snippetAreaChar;
				snippetAreaPrev = a.snippetArea;
				zonesList_.emplace_back(a.zoneArea);
			}
		}
	}
	addSnippet(resultString, data, snippetAreaPrev, snippetAreaPrevChar);
}

bool Snippet::Process(ItemRef& res, PayloadType& plType, const FtFuncStruct& func, std::vector<key_string>& stringsHolder) {
	if (!func.ctx) {
		return false;
	}
	if (!func.tagsPath.empty()) {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}

	init(func);

	FtCtx::Ptr ftctx = reindexer::static_ctx_pointer_cast<FtCtx>(func.ctx);
	auto& dataFtCtx = *(reindexer::static_ctx_pointer_cast<FtCtxAreaData<Area>>(ftctx->GetData()));

	if (!dataFtCtx.isWordPositions) {
		throw Error(errParams, "Snippet function does not work with ft_fuzzy index");
	}
	if (!dataFtCtx.holders.has_value()) {
		return false;
	}

	auto it = dataFtCtx.holders->find(res.Id());
	if (it == dataFtCtx.holders->end()) {
		return false;
	}
	Payload pl(plType, res.Value());

	VariantArray kr;
	pl.Get(func.field, kr);

	if (kr.empty()) {
		throw Error(errLogic, "Unable to apply snippet function to the non-string field '{}'", func.field);
	}

	for (const auto& k : kr) {
		if (!k.Type().IsSame(KeyValueType::String{})) {
			throw Error(errLogic, "Unable to apply snippet function to the non-string field '{}'", func.field);
		}
	}

	auto pva = dataFtCtx.area[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) {
		return false;
	}

	initAreas(kr.size(), pva->GetData());
	sortAndJoinIntersectingAreas();

	plArr_.resize(0);
	plArr_.reserve(kr.size());

	for (size_t arrIdx = 0; arrIdx < kr.size(); ++arrIdx) {
		const std::string_view data = std::string_view(p_string(kr[arrIdx]));
		std::string resultString;
		resultString.reserve(data.size());

		RecalcZoneHelper recalcZoneHelper(data, ftctx->GetData()->splitter, after_, before_, leftBound_, rightBound_);
		if (needAreaStr_) {
			buildResultWithPrefix(recalcZoneHelper, areasByArrayIdxs_[arrIdx], data, resultString);
		} else {
			buildResult(recalcZoneHelper, areasByArrayIdxs_[arrIdx], data, resultString);
		}

		stringsHolder.emplace_back(make_key_string(std::move(resultString)));
		plArr_.emplace_back(stringsHolder.back());
	}

	res.Value().Clone();

	pl.Set(func.field, plArr_);
	return true;
}
}  // namespace reindexer
