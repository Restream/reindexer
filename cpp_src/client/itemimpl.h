#pragma once

#include <deque>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "tools/serializer.h"

namespace reindexer {
namespace client {
class ItemImpl {
public:
	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher)
		: payloadType_(std::move(type)),
		  payloadValue_(payloadType_.TotalSize(), 0, payloadType_.TotalSize() + 0x100),
		  tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher)
		: payloadType_(std::move(type)), payloadValue_(std::move(v)), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImpl(const ItemImpl &) = delete;
	ItemImpl(ItemImpl &&o) = default;
	ItemImpl &operator=(const ItemImpl &) = delete;
	ItemImpl &operator=(ItemImpl &&) noexcept;

	void SetField(int field, const VariantArray &krs);
	Variant GetField(int field);

	std::string_view GetJSON();
	Error FromJSON(std::string_view slice, char **endp = nullptr, bool pkOnly = false);
	void FromCJSON(ItemImpl *other);

	std::string_view GetCJSON();
	void FromCJSON(std::string_view slice);

	std::string_view GetMsgPack();
	Error FromMsgPack(std::string_view slice, size_t &offset);

	PayloadType Type() const noexcept { return payloadType_; }
	PayloadValue &Value() noexcept { return payloadValue_; }
	Payload GetPayload() noexcept { return Payload(payloadType_, payloadValue_); }
	ConstPayload GetConstPayload() const noexcept { return ConstPayload(payloadType_, payloadValue_); }

	TagsMatcher &tagsMatcher() { return tagsMatcher_; }

	void SetPrecepts(const std::vector<std::string> &precepts) { precepts_ = precepts; }
	const std::vector<std::string> &GetPrecepts() const noexcept { return precepts_; }
	void Unsafe(bool enable) noexcept { unsafe_ = enable; }

protected:
	// Index fields payload data
	PayloadType payloadType_;
	PayloadValue payloadValue_;
	TagsMatcher tagsMatcher_;

	WrSerializer ser_;
	std::string tupleData_;

	std::vector<std::string> precepts_;
	bool unsafe_ = false;
	std::deque<std::string> holder_;
	std::vector<std::unique_ptr<char[]>> largeJSONStrings_;
};
}  // namespace client
}  // namespace reindexer
