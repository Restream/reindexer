#include "core/itemimpl.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonencoder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/jsonencoder.h"

using std::move;

namespace reindexer {
ItemImpl &ItemImpl::operator=(ItemImpl &&other) noexcept {
	if (&other != this) {
		payloadType_ = std::move(other.payloadType_);
		payloadValue_ = std::move(other.payloadValue_);
		tagsMatcher_ = std::move(other.tagsMatcher_);
		jsonAllocator_ = std::move(other.jsonAllocator_);
		ser_ = std::move(other.ser_);
		tupleData_ = std::move(other.tupleData_);
		precepts_ = std::move(other.precepts_);
		unsafe_ = other.unsafe_;
		holder_ = std::move(other.holder_);
	}
	return *this;
}

void ItemImpl::SetField(int field, const KeyRefs &krs) {
	payloadValue_.AllocOrClone(0);
	if (!unsafe_ && !krs.empty() && krs[0].Type() == KeyValueString) {
		KeyRefs krsCopy;
		krsCopy.reserve(krs.size());
		for (auto &kr : krs) {
			holder_.push_back(kr.As<string>());
			krsCopy.push_back(KeyRef(p_string(&holder_.back())));
		}

		GetPayload().Set(field, krsCopy, false);

	} else {
		GetPayload().Set(field, krs, false);
	}
}

KeyRef ItemImpl::GetField(int field) {
	KeyRefs kr;
	GetPayload().Get(field, kr);
	return kr[0];
}

// Construct item from compressed json
Error ItemImpl::FromCJSON(const Slice &slice) {
	GetPayload().Reset();
	Slice data = slice;
	if (!unsafe_) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	Serializer rdser(data.data(), data.size());
	// check tags matcher update
	uint32_t tmOffset = rdser.GetUInt32();
	uint32_t cacheToken = 0;

	if (tmOffset) {
		// read tags matcher update
		Serializer tser(slice.data() + tmOffset, slice.size() - tmOffset);
		cacheToken = tser.GetVarUint();
		if (cacheToken != tagsMatcher_.cacheToken()) {
			return Error(errParams, "cacheToken mismatch:  %08X, need %08X. Can't process item\n", cacheToken, tagsMatcher_.cacheToken());
		}
		if (!tser.Eof()) {
			tagsMatcher_.deserialize(tser);
			tagsMatcher_.setUpdated();
		}
	}

	Payload pl = GetPayload();
	CJsonDecoder decoder(tagsMatcher_);
	auto err = decoder.Decode(&pl, rdser, ser_);

	if (err.ok()) {
		assertf(rdser.Eof() || rdser.Pos() == tmOffset, "Internal error - left unparsed data %d", int(rdser.Pos()));
		tupleData_ = make_key_string(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
		pl.Set(0, {KeyRef(tupleData_)});
	} else {
		printf("Error decode cjson %s dumping tm: %s, tmOffset=%d %08X\n", err.what().c_str(), tagsMatcher_.dump().c_str(), int(tmOffset),
			   cacheToken);
		abort();
	}

	return err;
}

Error ItemImpl::FromJSON(const Slice &slice, char **endp) {
	Slice data = slice;
	if (!unsafe_ && endp == nullptr) {
		holder_.push_back(slice.ToString());
		data = holder_.back();
	}

	const char *json = data.data();
	payloadValue_.AllocOrClone(0);
	char *endptr = nullptr;
	JsonValue value;
	int status = jsonParse(const_cast<char *>(json), &endptr, &value, jsonAllocator_);
	if (status != JSON_OK) return Error(errLogic, "Error parsing json - status %d\n", status);
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_);
	Payload pl = GetPayload();
	auto err = decoder.Decode(&pl, ser_, value);

	if (err.ok()) {
		// Put tuple to field[0]
		tupleData_ = make_key_string(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
		pl.Set(0, {KeyRef(tupleData_)});
	}
	return err;
}

Error ItemImpl::FromCJSON(ItemImpl *other) {
	auto cjson = other->GetCJSON();
	auto err = FromCJSON(cjson);
	assert(err.ok());
	return err;
}

Slice ItemImpl::GetJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	JsonPrintFilter filter;
	JsonEncoder encoder(tagsMatcher_, filter);

	ser_.Reset();
	encoder.Encode(&pl, ser_);

	return Slice(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
}

Slice ItemImpl::GetCJSON() {
	ConstPayload pl(payloadType_, payloadValue_);
	CJsonEncoder encoder(tagsMatcher_);

	ser_.Reset();
	ser_.PutUInt32(0);
	encoder.Encode(&pl, ser_);

	return Slice(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
}

}  // namespace reindexer
