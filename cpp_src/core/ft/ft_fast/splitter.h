#pragma once
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "tools/stringstools.h"

namespace reindexer {

class ISplitterTask {
public:
	virtual ~ISplitterTask();
	virtual void SetText(std::string_view t) noexcept = 0;
	virtual const std::vector<std::string_view>& GetResults() = 0;
	virtual std::pair<int, int> Convert(unsigned int wordPosStart, unsigned int wordPosEnd) = 0;
	virtual void WordToByteAndCharPos(int wordPosition, WordPosition& out) = 0;
	virtual void WordToByteAndCharPos(int wordPosition, WordPositionEx& out) = 0;
};

class FastTextSplitter;

class SplitterTaskFast final : public ISplitterTask {
public:
	void SetText(std::string_view t) noexcept override {
		convertedText_.clear();
		text_ = t;
		words_.clear();
	}
	const std::vector<std::string_view>& GetResults() override;
	std::pair<int, int> Convert(unsigned int wordPosStart, unsigned int wordPosEnd) override;

	void WordToByteAndCharPos(int wordPosition, WordPosition& out) override;
	void WordToByteAndCharPos(int wordPosition, WordPositionEx& out) override;

	friend class FastTextSplitter;

private:
	SplitterTaskFast(const FastTextSplitter& s) : splitter_(s) {}

	template <typename Pos>
	Pos wordToByteAndCharPos(std::string_view str, int wordPosition, std::string_view extraWordSymbols);

	std::string_view text_;
	std::vector<std::string_view> words_;  // string_view point to convertedText_
	std::string convertedText_;
	unsigned int lastWordPos_ = 0, lastOffset_ = 0;
	const FastTextSplitter& splitter_;
};

class ISplitter : public intrusive_atomic_rc_base {
public:
	virtual std::shared_ptr<ISplitterTask> CreateTask() const = 0;
};

class FastTextSplitter final : public ISplitter {
public:
	FastTextSplitter(const std::string& extraWordSymbols) : extraWordSymbols_(extraWordSymbols) {}
	std::shared_ptr<ISplitterTask> CreateTask() const override;
	std::string_view GetExtraWordsSymbols() const noexcept { return extraWordSymbols_; }

private:
	const std::string extraWordSymbols_;
};
}  // namespace reindexer
