#pragma once
#include "core/ft/areaholder.h"
#include "core/item.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

struct SelectFuncStruct;

class Snippet {
public:
	bool Process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func, std::vector<key_string> &stringsHolder);

private:
	void init(const SelectFuncStruct &func);
	void addSnippet(std::string &resultString, const std::string &data, const Area &snippetAreaPrev, const Area &snippetAreaPrevChar) const;

	class RecalcZoneHelper {
	public:
		RecalcZoneHelper(std::string_view data, const std::string &extra, unsigned int after, unsigned int before,
						 std::string_view leftBound, std::string_view rightBound) noexcept
			: str_(data),
			  data_(data),
			  extraWordSymbols_(extra),
			  after_(after),
			  before_(before),
			  leftBound_(leftBound),
			  rightBound_(rightBound) {}
		template <typename A>
		A RecalcZoneToOffset(const Area &area);

	private:
		std::string_view str_;
		const std::string_view data_;
		size_t wordCount_ = 0;
		int stringBeginOffsetByte_ = 0;
		int stringBeginOffsetChar_ = 0;
		const std::string &extraWordSymbols_;
		unsigned int after_, before_;
		std::string_view leftBound_, rightBound_;
	};

	void buildResult(RecalcZoneHelper &recalcZoneHelper, const AreaBuffer &pva, const std::string &data, std::string &resultString);
	void buildResultWithPrefix(RecalcZoneHelper &recalcZoneHelper, const AreaBuffer &pva, const std::string &data,
							   std::string &resultString);

	bool isInit_ = false;
	bool needAreaStr_ = false;
	unsigned int after_, before_;
	std::string_view preDelim_ = "";
	std::string_view postDelim_ = " ";
	std::string_view leftBound_;
	std::string_view rightBound_;
	std::string_view markerBefore_;
	std::string_view markerAfter_;
	RVector<Area, 10> zonesList_;
};

class SnippetN : public Snippet {};

} // namespace reindexer
