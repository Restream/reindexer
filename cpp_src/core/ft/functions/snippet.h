#pragma once

#include "core/ft/areaholder.h"
#include "core/ft/ft_fast/splitter.h"

namespace reindexer {

struct FtFuncStruct;
class PayloadType;
class ItemRef;
class key_string;

class [[nodiscard]] Snippet {
public:
	[[nodiscard]] bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>& stringsHolder);

private:
	void init(const FtFuncStruct& func);
	void addSnippet(std::string& resultString, std::string_view data, const Area& snippetAreaPrev, const Area& snippetAreaPrevChar) const;

	class [[nodiscard]] RecalcZoneHelper {
	public:
		RecalcZoneHelper(std::string_view data, intrusive_ptr<const ISplitter> splitter, unsigned int after, unsigned int before,
						 std::string_view leftBound, std::string_view rightBound) noexcept
			: str_(data),
			  data_(data),
			  splitter_(std::move(splitter)),
			  after_(after),
			  before_(before),
			  leftBound_(leftBound),
			  rightBound_(rightBound) {}
		template <typename A>
		A RecalcZoneToOffset(const Area& area);

	private:
		std::string_view str_;
		const std::string_view data_;
		size_t wordCount_ = 0;
		int stringBeginOffsetByte_ = 0;
		int stringBeginOffsetChar_ = 0;
		const intrusive_ptr<const ISplitter> splitter_;
		unsigned int after_, before_;
		std::string_view leftBound_, rightBound_;
	};

	void buildResult(RecalcZoneHelper& recalcZoneHelper, const AreasInField<Area>& pva, std::string_view data, std::string& resultString);
	void buildResultWithPrefix(RecalcZoneHelper& recalcZoneHelper, const AreasInField<Area>& pva, std::string_view data,
							   std::string& resultString);

	bool isInit_ = false;
	bool needAreaStr_ = false;
	unsigned int after_, before_;
	std::string_view preDelim_ = "";
	std::string_view postDelim_ = " ";
	std::string_view leftBound_;
	std::string_view rightBound_;
	std::string_view markerBefore_;
	std::string_view markerAfter_;
	h_vector<Area, 10> zonesList_;
};

class [[nodiscard]] SnippetN : public Snippet {};

}  // namespace reindexer
