#pragma once

#include <deque>
#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/key_string.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "tools/serializer.h"

using std::vector;

namespace reindexer {

class Namespace;
class ItemImpl {
public:
	// Construct empty item
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher, const FieldsSet &pkFields = {})
		: payloadType_(type), payloadValue_(type.TotalSize(), 0, type.TotalSize() + 0x100), tagsMatcher_(tagsMatcher), pkFields_(pkFields) {
		tagsMatcher_.clearUpdated();
	}

	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher)
		: payloadType_(type), payloadValue_(v), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	ItemImpl(const ItemImpl &) = delete;
	ItemImpl(ItemImpl &&) = default;
	ItemImpl &operator=(const ItemImpl &) = delete;
	ItemImpl &operator=(ItemImpl &&) noexcept;

	void SetField(int field, const VariantArray &krs);
	void SetField(string_view jsonPath, const VariantArray &keys);
	Variant GetField(int field);
	FieldsSet PkFields() const { return pkFields_; }

	VariantArray GetValueByJSONPath(string_view jsonPath);

	string_view GetJSON();
	Error FromJSON(const string_view &slice, char **endp = nullptr, bool pkOnly = false);
	Error FromCJSON(ItemImpl *other);

	string_view GetCJSON(bool withTagsMatcher = false);
	string_view GetCJSON(WrSerializer &ser, bool withTagsMatcher = false);
	Error FromCJSON(const string_view &slice, bool pkOnly = false);

	PayloadType Type() { return payloadType_; }
	PayloadValue &Value() { return payloadValue_; }
	PayloadValue &RealValue() { return realValue_; }
	Payload GetPayload() { return Payload(payloadType_, payloadValue_); }

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
		holder_.clear();
		ser_ = WrSerializer();
		GetPayload().Reset();
		payloadValue_.SetLSN(-1);
		unsafe_ = false;
		ns_.reset();
		realValue_ = PayloadValue();
	}
	void SetNamespace(std::shared_ptr<Namespace> ns) { ns_ = ns; }
	std::shared_ptr<Namespace> GetNamespace() { return ns_; }

protected:
	// Index fields payload data
	PayloadType payloadType_;
	PayloadValue payloadValue_;
	PayloadValue realValue_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;

	WrSerializer ser_;
	string tupleData_;

	vector<string> precepts_;
	bool unsafe_ = false;
	std::deque<std::string> holder_;
	string_view cjson_;
	std::shared_ptr<Namespace> ns_;
};

}  // namespace reindexer
