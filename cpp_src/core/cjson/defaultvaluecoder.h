#pragma once

#include "cjsondecoder.h"

namespace reindexer {

class DefaultValueCoder : public Recoder {
public:
	DefaultValueCoder(std::string_view ns, const PayloadFieldType& fld, std::vector<TagsPath>&& tps, int16_t fieldIdx);
	RX_ALWAYS_INLINE TagType Type(TagType tt) noexcept override final { return tt; }
	[[nodiscard]] bool Match(int f) noexcept override final;
	[[nodiscard]] bool Match(TagType tt, const TagsPath& tp) override final;
	RX_ALWAYS_INLINE void Recode(Serializer&, WrSerializer&) const noexcept override final { assertrx(false); }
	RX_ALWAYS_INLINE void Recode(Serializer&, Payload&, int, WrSerializer&) noexcept override final { assertrx(false); }
	void Serialize(WrSerializer& wrser) override final;
	bool Reset() noexcept override final;

private:
	void match(const TagsPath& tp);
	[[nodiscard]] bool write(WrSerializer& wrser) const;
	[[nodiscard]] RX_ALWAYS_INLINE bool blocked() const noexcept { return ((state_ == State::found) || (state_ == State::write)); }
	[[nodiscard]] RX_ALWAYS_INLINE bool ready() const noexcept { return ((state_ == State::wait) || (state_ == State::match)); }

private:
	const std::string ns_;
	const std::string field_;
	const std::vector<TagsPath> tags_;
	const int16_t fieldIdx_{0};
	const TagType type_;
	const bool array_{false};

	const TagsPath* basePath_{nullptr};
	enum class State { wait, found, match, write } state_{State::wait};
	uint32_t nestingLevel_{1};
	uint32_t copyPos_{0};

	// bool inArray_{false};
	// int16_t arrField_{0};
};

}  // namespace reindexer
