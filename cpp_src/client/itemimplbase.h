#pragma once

#include <chrono>
#include <deque>
#include <vector>
#include "client/internalrdxcontext.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "core/query/query.h"
#include "gason/gason.h"
#include "tools/serializer.h"

namespace reindexer {
namespace client {

class RPCClient;

class ItemImplBase {
public:
	// Construct empty item
	ItemImplBase() = default;
	ItemImplBase(PayloadType type, const TagsMatcher &tagsMatcher)
		: payloadType_(type), payloadValue_(type.TotalSize(), 0, type.TotalSize() + 0x100), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImplBase(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher)
		: payloadType_(type), payloadValue_(v), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImplBase(const ItemImplBase &) = delete;
	ItemImplBase &operator=(const ItemImplBase &) = delete;
	ItemImplBase(ItemImplBase &&) = default;
	ItemImplBase &operator=(ItemImplBase &&) = default;

	virtual ~ItemImplBase() = default;

	std::string_view GetJSON();
	Error FromJSON(std::string_view slice, char **endp = nullptr, bool pkOnly = false);
	Error FromCJSON(ItemImplBase *other);

	std::string_view GetCJSON();
	Error FromCJSON(std::string_view slice);

	std::string_view GetMsgPack();
	Error FromMsgPack(std::string_view slice, size_t &offset);

	PayloadType Type() const { return payloadType_; }
	PayloadValue &Value() noexcept { return payloadValue_; }
	Payload GetPayload() { return Payload(payloadType_, payloadValue_); }
	ConstPayload GetConstPayload() const { return ConstPayload(payloadType_, payloadValue_); }

	TagsMatcher &tagsMatcher() noexcept { return tagsMatcher_; }
	void addTagNamesFrom(const TagsMatcher &tm) { tagsMatcher_.add_names_from(tm); }
	void setTagsMatcher(TagsMatcher tm) noexcept { tagsMatcher_ = std::move(tm); }

	void SetPrecepts(std::vector<std::string> &&precepts) { precepts_ = std::move(precepts); }
	const std::vector<std::string> &GetPrecepts() const noexcept { return precepts_; }
	void GetPrecepts(WrSerializer &ser);
	void Unsafe(bool enable) noexcept { unsafe_ = enable; }
	static bool HasBundledTm(std::string_view cjson) {
		Serializer rser(cjson);
		return ReadBundledTmTag(rser);
	}
	static bool ReadBundledTmTag(Serializer &ser) { return ser.GetVarUint() == TAG_END; }

protected:
	virtual Error tryToUpdateTagsMatcher() = 0;

	// Index fields payload data
	PayloadType payloadType_;
	PayloadValue payloadValue_;
	TagsMatcher tagsMatcher_;

	WrSerializer ser_;
	std::string_view tupleData_;
	std::unique_ptr<uint8_t[]> tupleHolder_;

	std::vector<std::string> precepts_;
	bool unsafe_ = false;
	std::unique_ptr<char[]> holder_;
	std::vector<std::unique_ptr<char[]>> largeJSONStrings_;
};

}  // namespace client
}  // namespace reindexer
