#include "snippet.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "highlight.h"
#include "tools/errors.h"
#include "utf8cpp/utf8.h"
namespace reindexer {

class Utf8CharCalculator {
public:
	Utf8CharCalculator(std::string_view str) noexcept : str_(str), iterator_(str_.begin()) {}
	// throw on error
	unsigned Next(unsigned byteOffset) {
		for (; iterator_ != str_.end(); ++charCounter_, utf8::unchecked::next(iterator_)) {
			const unsigned diff = iterator_ - str_.begin();
			if (diff == byteOffset) {
				return charCounter_;
			}
			if (diff > byteOffset) {
				throw Error(errParams, "Incorrect input. Byte offset not char.");
			}
		}
		if (byteOffset != str_.size()) {
			throw Error(errParams, "Incorrect input. Byte offset is to long.");
		}
		return charCounter_;
	}

private:
	std::string_view str_;
	std::string_view::const_iterator iterator_;
	unsigned long charCounter_ = 0;
};

void Snippet::init(const SelectFuncStruct &func) {
	if (isInit_) return;
	if (func.funcArgs.size() < 4) throw Error(errParams, "Invalid snippet params need minimum 4 - have %d", func.funcArgs.size());
	try {
		std::size_t pos;
		before_ = stoul(func.funcArgs[2], &pos);
		if (pos != func.funcArgs[2].size()) {
			throw Error(errParams, "Invalid snippet param before - %s is not a number", func.funcArgs[2]);
		}
	} catch (std::exception &) {
		throw Error(errParams, "Invalid snippet param before - %s is not a number", func.funcArgs[2]);
	}

	try {
		std::size_t pos;
		after_ = stoul(func.funcArgs[3], &pos);
		if (pos != func.funcArgs[3].size()) {
			throw Error(errParams, "Invalid snippet param after - %s is not a number", func.funcArgs[3]);
		}
	} catch (std::exception &) {
		throw Error(errParams, "Invalid snippet param after - %s is not a number", func.funcArgs[3]);
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

void Snippet::addSnippet(std::string &resultString, const std::string &data, const Area &snippetAreaPrev,
						 const Area &snippetAreaPrevChar) const {
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
};

struct Areas {
	Area zoneArea;
	Area snippetArea;
};

struct AreasEx : public Areas {
	Area snippetAreaChar;
};

template <typename A>
A Snippet::RecalcZoneHelper::RecalcZoneToOffset(const Area &area) {
	using PointType = std::conditional_t<std::is_same_v<Areas, A>, WordPosition, WordPositionEx>;
	constexpr bool needChar = std::is_same_v<AreasEx, A>;
	A outAreas;
	PointType p1 = wordToByteAndCharPos<PointType>(str_, area.start - wordCount_, extraWordSymbols_);

	wordCount_ += area.start - wordCount_;
	outAreas.zoneArea.start = p1.StartByte() + stringBeginOffsetByte_;
	if (area.end - area.start > 1) {
		wordCount_++;
		stringBeginOffsetByte_ += p1.EndByte();
		if constexpr (needChar) {
			outAreas.snippetAreaChar.start = p1.start.ch + stringBeginOffsetChar_;
			stringBeginOffsetChar_ += p1.end.ch;
		}
		str_ = str_.substr(p1.EndByte());
		auto p2 = wordToByteAndCharPos<PointType>(str_, area.end - wordCount_ - 1, extraWordSymbols_);
		wordCount_ += area.end - wordCount_;
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

void Snippet::buildResult(RecalcZoneHelper &recalcZoneHelper, const AreaBuffer &pva, const std::string &data, std::string &resultString) {
	// resultString =preDelim_+with_area_str+data_str_before+marker_before+zone_str+marker_after+data_strAfter+postDelim_
	Area snippetAreaPrev;
	Area snippetAreaPrevChar;
	zonesList_.clear<false>();

	for (const auto &area : pva.GetData()) {
		Areas a = recalcZoneHelper.RecalcZoneToOffset<Areas>(area);

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

void Snippet::buildResultWithPrefix(RecalcZoneHelper &recalcZoneHelper, const AreaBuffer &pva, const std::string &data,
									std::string &resultString) {
	// resultString =preDelim_+with_area_str+data_str_before+marker_before+zone_str+marker_after+data_strAfter+postDelim_
	Area snippetAreaPrev;
	Area snippetAreaPrevChar;
	zonesList_.clear<false>();

	for (const auto &area : pva.GetData()) {
		AreasEx a = recalcZoneHelper.RecalcZoneToOffset<AreasEx>(area);
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

bool Snippet::Process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func, std::vector<key_string> &stringsHolder) {
	if (!func.ctx) return false;
	init(func);

	FtCtx::Ptr ftctx = reindexer::reinterpret_pointer_cast<FtCtx>(func.ctx);
	auto dataFtCtx = ftctx->GetData();
	if (!dataFtCtx->isWordPositions_) {
		throw Error(errParams, "Snippet function does not work with ft_fuzzy index.");
	}
	if (!func.tagsPath.empty()) {
		throw Error(errConflict, "SetByJsonPath is not implemented yet!");
	}
	auto it = dataFtCtx->holders_.find(res.Id());
	if (it == dataFtCtx->holders_.end()) {
		return false;
	}
	Payload pl(pl_type, res.Value());

	VariantArray kr;
	pl.Get(func.field, kr);
	if (kr.empty() || !kr[0].Type().IsSame(KeyValueType::String{})) {
		throw Error(errLogic, "Unable to apply snippet function to the non-string field '%s'", func.field);
	}

	const std::string *data = p_string(kr[0]).getCxxstr();
	auto pva = dataFtCtx->area_[it->second].GetAreas(func.fieldNo);
	if (!pva || pva->Empty()) return false;

	std::string resultString;
	resultString.reserve(data->size());

	RecalcZoneHelper recalcZoneHelper(*data, ftctx->GetData()->extraWordSymbols_, after_, before_, leftBound_, rightBound_);

	if (needAreaStr_) {
		buildResultWithPrefix(recalcZoneHelper, *pva, *data, resultString);
	} else {
		buildResult(recalcZoneHelper, *pva, *data, resultString);
	}

	stringsHolder.emplace_back(make_key_string(std::move(resultString)));
	res.Value().Clone();

	pl.Set(func.field, Variant{stringsHolder.back()});
	return true;
}
}  // namespace reindexer
