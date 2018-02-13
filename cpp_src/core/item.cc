#include "core/item.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsonencoder.h"
#include "core/cjson/jsondecoder.h"
#include "core/cjson/jsonencoder.h"

using std::move;

namespace reindexer {

Error ItemImpl::SetField(const string &index, const KeyRefs &krs) {
	try {
		payloadData_.AllocOrClone(0);
		Set(index, krs, true);
	} catch (const Error &err) {
		return err;
	}
	return 0;
}
Error ItemImpl::SetField(const string &index, const KeyRef &kr) {
	try {
		KeyRefs krs;
		krs.push_back(kr);
		payloadData_.AllocOrClone(0);
		Set(index, krs, false);
	} catch (const Error &err) {
		return err;
	}
	return 0;
}

KeyRef ItemImpl::GetField(const string &index) {
	KeyRefs kr;
	Get(index, kr);
	return kr[0];
}

// Construct item from compressed json
Error ItemImpl::FromCJSON(const Slice &slice) {
	Reset();
	Serializer rdser(slice.data(), slice.size());
	// check tags matcher update
	uint32_t tmOffset = rdser.GetUInt32();
	if (tmOffset) {
		// read tags matcher update
		Serializer tser(slice.data() + tmOffset, slice.size() - tmOffset);
		tagsMatcher_.deserialize(tser);
		tagsMatcher_.setUpdated();
	}

	CJsonDecoder decoder(tagsMatcher_);
	auto err = decoder.Decode(this, rdser, ser_);

	if (err.ok()) {
		assertf(rdser.Eof() || rdser.Pos() == tmOffset, "Internal error - left unparsed data %d", int(rdser.Pos()));
		tupleData_ = make_key_string(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
		Set(0, {KeyRef(tupleData_)});
	}

	return err;
}

Error ItemImpl::FromJSON(const Slice &slice, char **endp = nullptr) {
	const char *json = slice.data();
	payloadData_.AllocOrClone(0);
	char *endptr = nullptr;
	JsonValue value;
	int status = jsonParse(const_cast<char *>(json), &endptr, &value, jsonAllocator_);
	if (status != JSON_OK) return Error(errLogic, "Error parsing json - status %d\n", status);
	if (endp) {
		*endp = endptr;
	}

	// Split parsed json into indexes and tuple
	JsonDecoder decoder(tagsMatcher_);
	auto err = decoder.Decode(this, ser_, value);

	if (err.ok()) {
		// Put tuple to field[0]
		tupleData_ = make_key_string(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
		Set(0, {KeyRef(tupleData_)});
	}
	return err;
}  // namespace reindexer

Slice ItemImpl::GetJSON() {
	ConstPayload pl(Type(), Value());
	JsonPrintFilter filter;
	JsonEncoder encoder(tagsMatcher_, filter);

	ser_.Reset();
	encoder.Encode(&pl, ser_);

	return Slice(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
}

Slice ItemImpl::GetCJSON() {
	ConstPayload pl(Type(), Value());
	CJsonEncoder encoder(tagsMatcher_);

	ser_.Reset();
	ser_.PutUInt32(0);
	encoder.Encode(&pl, ser_);

	return Slice(reinterpret_cast<const char *>(ser_.Buf()), ser_.Len());
}

}  // namespace reindexer
