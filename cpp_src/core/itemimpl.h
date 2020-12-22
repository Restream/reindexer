#pragma once

#include <deque>
#include <vector>

#include "core/cjson/msgpackdecoder.h"
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "tools/serializer.h"

using std::vector;

namespace reindexer {

struct ItemImplRawData {
	ItemImplRawData(PayloadValue v) : payloadValue_(v) {}
	ItemImplRawData() {}
	ItemImplRawData(const ItemImplRawData &) = delete;
	ItemImplRawData(ItemImplRawData &&) noexcept;
	ItemImplRawData &operator=(const ItemImplRawData &) = delete;
	ItemImplRawData &operator=(ItemImplRawData &&) noexcept;

	PayloadValue payloadValue_;
	std::unique_ptr<uint8_t[]> tupleData_;
	std::unique_ptr<char[]> sourceData_;
	vector<string> precepts_;
	std::unique_ptr<std::deque<std::string>> holder_;
};

class Namespace;
class Schema;

class ItemImpl : public ItemImplRawData {
public:
	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher, const FieldsSet &pkFields = {}, std::shared_ptr<const Schema> schema = {})
		: ItemImplRawData(PayloadValue(type.TotalSize(), 0, type.TotalSize() + 0x100)),
		  payloadType_(type),
		  tagsMatcher_(tagsMatcher),
		  pkFields_(pkFields),
		  schema_(std::move(schema)) {
		tagsMatcher_.clearUpdated();
	}

	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher, const FieldsSet &pkFields, std::shared_ptr<const Schema> schema,
			 ItemImplRawData &&rawData)
		: ItemImplRawData(std::move(rawData)),
		  payloadType_(type),
		  tagsMatcher_(tagsMatcher),
		  pkFields_(pkFields),
		  schema_(std::move(schema)) {}

	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher)
		: ItemImplRawData(v), payloadType_(type), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImpl(const ItemImpl &) = delete;
	ItemImpl(ItemImpl &&) = default;
	ItemImpl &operator=(const ItemImpl &) = delete;
	ItemImpl &operator=(ItemImpl &&) noexcept;

	void ModifyField(string_view jsonPath, const VariantArray &keys, FieldModifyMode mode);
	void SetField(int field, const VariantArray &krs);
	void SetField(string_view jsonPath, const VariantArray &keys);
	void DropField(string_view jsonPath);
	Variant GetField(int field);
	void GetField(int field, VariantArray &);
	FieldsSet PkFields() const { return pkFields_; }
	int NameTag(string_view name) const { return tagsMatcher_.name2tag(name); }

	VariantArray GetValueByJSONPath(string_view jsonPath);

	string_view GetJSON();
	Error FromJSON(string_view slice, char **endp = nullptr, bool pkOnly = false);
	Error FromCJSON(ItemImpl *other);

	string_view GetCJSON(bool withTagsMatcher = false);
	string_view GetCJSON(WrSerializer &ser, bool withTagsMatcher = false);
	Error FromCJSON(const string_view &slice, bool pkOnly = false);
	Error FromMsgPack(string_view sbuf, size_t &offset);
	Error FromProtobuf(string_view sbuf);
	Error GetMsgPack(WrSerializer &wrser);
	Error GetProtobuf(WrSerializer &wrser);

	PayloadType Type() { return payloadType_; }
	PayloadValue &Value() { return payloadValue_; }
	PayloadValue &RealValue() { return realValue_; }
	Payload GetPayload() { return Payload(payloadType_, payloadValue_); }
	ConstPayload GetConstPayload() { return ConstPayload(payloadType_, payloadValue_); }
	std::shared_ptr<const Schema> GetSchema() { return schema_; }

	TagsMatcher &tagsMatcher() { return tagsMatcher_; }

	void SetPrecepts(const vector<string> &precepts) {
		precepts_ = precepts;
		cjson_ = string_view();
	}
	const vector<string> &GetPrecepts() { return precepts_; }
	void Unsafe(bool enable) { unsafe_ = enable; }
	void Clear() {
		tagsMatcher_ = TagsMatcher();
		precepts_.clear();
		cjson_ = string_view();
		holder_.reset();
		sourceData_.reset();
		tupleData_.reset();
		ser_ = WrSerializer();

		GetPayload().Reset();
		payloadValue_.SetLSN(-1);

		unsafe_ = false;
		ns_.reset();
		realValue_ = PayloadValue();
	}
	void SetNamespace(std::shared_ptr<Namespace> ns) { ns_ = std::move(ns); }
	std::shared_ptr<Namespace> GetNamespace() { return ns_; }

protected:
	// Index fields payload data
	PayloadType payloadType_;
	PayloadValue realValue_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;
	std::shared_ptr<const Schema> schema_;

	WrSerializer ser_;

	bool unsafe_ = false;
	string_view cjson_;
	std::shared_ptr<Namespace> ns_;
	std::unique_ptr<MsgPackDecoder> msgPackDecoder_;
};

}  // namespace reindexer
