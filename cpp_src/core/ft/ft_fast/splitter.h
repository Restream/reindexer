#pragma once
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "tools/stringstools.h"

namespace reindexer {

class [[nodiscard]] ISplitterTask {
public:
	virtual ~ISplitterTask();
	virtual void SetText(std::string_view t) noexcept = 0;
	virtual const std::vector<WordWithPos>& GetResults() = 0;
	virtual std::pair<int, int> Convert(unsigned int wordPosStart, unsigned int wordPosEnd) = 0;
	virtual void WordToByteAndCharPos(int wordPosition, WordPosition& out) = 0;
	virtual void WordToByteAndCharPos(int wordPosition, WordPositionEx& out) = 0;
};

class FastTextSplitter;

class [[nodiscard]] SplitterTaskFast final : public ISplitterTask {
public:
	void SetText(std::string_view t) noexcept override {
		convertedText_.clear();
		text_ = t;
		words_.clear();
	}
	const std::vector<WordWithPos>& GetResults() override;
	std::pair<int, int> Convert(unsigned int wordPosStart, unsigned int wordPosEnd) override;

	void WordToByteAndCharPos(int wordPosition, WordPosition& out) override;
	void WordToByteAndCharPos(int wordPosition, WordPositionEx& out) override;

	friend class FastTextSplitter;

private:
	SplitterTaskFast(const FastTextSplitter& s) : splitter_(s) {}

	template <typename Pos>
	Pos wordToByteAndCharPos(std::string_view str, int wordPosition, const SplitOptions& splitOptions);

	std::string_view text_;
	std::vector<WordWithPos> words_;  // string_view point to convertedText_
	std::string convertedText_;
	unsigned int lastWordPos_ = 0, lastOffset_ = 0;
	const FastTextSplitter& splitter_;
};

class [[nodiscard]] ISplitter : public intrusive_atomic_rc_base {
public:
	virtual std::shared_ptr<ISplitterTask> CreateTask() const = 0;
};

class [[nodiscard]] FastTextSplitter final : public ISplitter {
public:
	FastTextSplitter(const SplitOptions opts) : opts_(opts) {}
	std::shared_ptr<ISplitterTask> CreateTask() const override;
	const SplitOptions& GetSplitOptions() const noexcept { return opts_; }

private:
	const SplitOptions opts_;
};
}  // namespace reindexer
