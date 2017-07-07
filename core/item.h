#pragma once

#include "cbinding/serializer.h"
#include "core/basepayload.h"
#include "core/keyvalue.h"
#include "gason/gason.h"

namespace reindexer {

struct ItemRef {
	ItemRef(IdType iid = 0, int iversion = 0) : id(iid), version(iversion) {}
	ItemRef(IdType iid, int iversion, const PayloadData &idata) : id(iid), version(iversion), data(idata) {}
	IdType id;
	int version;
	PayloadData data;
};
class Item {
public:
	virtual ~Item(){};
	virtual Error SetField(const string &index, const KeyRefs &kvs) = 0;
	Slice Serialize();
	virtual void Deserialize(const Slice &slice) = 0;
	virtual Error FromJSON(const Slice &slice) = 0;
	virtual Slice GetJSON() = 0;
	virtual Error Status() = 0;
	virtual ItemRef GetRef() = 0;
};

class ItemTagsMatcher {
public:
	virtual ~ItemTagsMatcher(){};
	virtual int name2tag(const string &name, const string &path) = 0;
	virtual const char *tag2name(int tag) const = 0;
	virtual int tag2field(int tag) const = 0;
};

class ItemImpl : public Item, public Payload {
public:
	// Construct empty item
	ItemImpl(const PayloadType &type, ItemTagsMatcher *tagsMatcher)
		: Payload(type, &payloadData_), payloadData_(type.TotalSize(), 0, type.TotalSize() + 0x10000), tagsMatcher_(tagsMatcher) {}

	// Construct item with payload copy
	ItemImpl(const Payload &pl, ItemTagsMatcher *tagsMatcher)
		: Payload(pl.Type(), &payloadData_), payloadData_(*pl.Data()), tagsMatcher_(tagsMatcher) {}

	// Construct with error
	ItemImpl(const Error &err)
		: Payload(invalidType_, &payloadData_), payloadData_(invalidType_.TotalSize()), tagsMatcher_(nullptr), err_(err) {}

	~ItemImpl() {}

	Error SetField(const string &index, const KeyRefs &krs) override;
	Slice Serialize();
	void Deserialize(const Slice &) override;

	Slice GetJSON() override;
	virtual Error FromJSON(const Slice &slice) override;

	Error Status() override { return err_; }

	ItemRef GetRef() override { return id_; }

	// Internal interface
	void SetID(IdType id, int version) {
		id_.id = id;
		id_.version = version;
	}

	static bool MergeJson(ConstPayload *pl, const ItemTagsMatcher *tagsMatcher, WrSerializer &wrser, Serializer &rdser, bool first,
						  int *fieldsoutcnt);

protected:
	void splitJson(JsonValue &v, int tag);
	void splitJsonObject(JsonValue &v);
	// Data of item. Blob with user data (gob in current implementation)
	string userData_;
	// Index fields ayload data
	PayloadData payloadData_;

	ItemTagsMatcher *tagsMatcher_;

	// Status
	Error err_;

	static PayloadType invalidType_;

	ItemRef id_;
	string jsonPath_;
	JsonAllocator jsonAllocator_;
	WrSerializer ser_;
	key_string tupleData_;
};

}  // namespace reindexer
