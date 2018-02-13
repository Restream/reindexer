#pragma once

#include <vector>
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/keyvalue.h"
#include "core/payload/payloadiface.h"
#include "gason/gason.h"
#include "tools/serializer.h"

using std::vector;

namespace reindexer {

struct ItemRef {
	ItemRef(IdType iid = 0, int iversion = 0) : id(iid), version(iversion) {}
	ItemRef(IdType iid, int iversion, const PayloadValue &ivalue, uint8_t iproc = 0, uint8_t insid = 0)
		: id(iid), version(iversion), proc(iproc), nsid(insid), value(ivalue) {}
	IdType id;
	int16_t version;
	uint8_t proc;
	uint8_t nsid;
	PayloadValue value;
};

class Item {
public:
	virtual ~Item() {}
	virtual Error SetField(const string &index, const KeyRefs &kvs) = 0;
	virtual Error SetField(const string &index, const KeyRef &kvs) = 0;
	virtual KeyRef GetField(const string &index) = 0;
	virtual Error FromJSON(const Slice &slice, char **endp = nullptr) = 0;
	virtual Slice GetJSON() = 0;
	virtual Error Status() = 0;
	virtual ItemRef GetRef() = 0;
	virtual Slice GetCJSON() = 0;
	virtual Error FromCJSON(const Slice &) = 0;

	virtual void SetPrecepts(const vector<string> &precepts) = 0;
	virtual const vector<string> &GetPrecepts() = 0;
};

class ItemImpl : public Item, public Payload {
public:
	// Construct empty item
	ItemImpl(PayloadType::Ptr type, const TagsMatcher &tagsMatcher)
		: Payload(type, payloadData_),
		  payloadType_(type),
		  payloadData_(type->TotalSize(), 0, type->TotalSize() + 0x100),
		  tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	// Construct item with payload copy
	ItemImpl(const Payload &pl, const TagsMatcher &tagsMatcher)
		: Payload(pl.Type(), &payloadData_), payloadData_(*pl.Value()), tagsMatcher_(tagsMatcher) {
		tagsMatcher_.clearUpdated();
	}

	// Construct with error
	ItemImpl(const Error &err) : Payload(nullptr, payloadData_), tagsMatcher_(nullptr), err_(err) {}
	ItemImpl() : Payload(nullptr, payloadData_), tagsMatcher_(nullptr) {}

	ItemImpl(const ItemImpl &) = delete;
	ItemImpl(ItemImpl &&o) noexcept
		: Payload(o.payloadType_, o.payloadData_),
		  payloadType_(move(o.payloadType_)),
		  payloadData_(move(o.payloadData_)),
		  tagsMatcher_(move(o.tagsMatcher_)),
		  err_(o.err_),
		  id_(o.id_),
		  jsonAllocator_(move(o.jsonAllocator_)),
		  ser_(move(o.ser_)),
		  tupleData_(move(o.tupleData_)),
		  precepts_(move(o.precepts_)) {
		o.payloadType_ = nullptr;
	}
	~ItemImpl() {}
	ItemImpl &operator=(const ItemImpl &) = delete;
	ItemImpl &operator=(ItemImpl &&other) noexcept {
		if (this != &other) {
			payloadType_ = other.payloadType_;
			payloadData_ = move(other.payloadData_);
			tagsMatcher_ = move(other.tagsMatcher_);

			err_ = other.err_;
			id_ = other.id_;

			ser_ = move(other.ser_);
			tupleData_ = move(other.tupleData_);
			jsonAllocator_ = move(other.jsonAllocator_);

			precepts_ = move(other.precepts_);

			other.payloadType_ = nullptr;
		}

		return *this;
	}

	Error SetField(const string &index, const KeyRefs &krs) override final;
	Error SetField(const string &index, const KeyRef &kr) override final;
	KeyRef GetField(const string &index) override final;

	Slice GetJSON() final override;
	Error FromJSON(const Slice &slice, char **endp) override final;

	Slice GetCJSON() final override;
	Error FromCJSON(const Slice &slice) override final;

	Error Status() override final { return err_; }

	ItemRef GetRef() override final { return id_; }

	// Internal interface
	void SetID(IdType id, int version) {
		id_.id = id;
		id_.version = version;

		err_ = id == -1 ? Error(errLogic, "Item has not been updated/inserted") : 0;
	}

	TagsMatcher &getTagsMatcher() { return tagsMatcher_; }

	void SetPrecepts(const vector<string> &precepts) override final { precepts_ = precepts; }
	const vector<string> &GetPrecepts() override final { return precepts_; }

protected:
	// Index fields payload data
	PayloadType::Ptr payloadType_;
	PayloadValue payloadData_;

	TagsMatcher tagsMatcher_;

	// Status
	Error err_;

	ItemRef id_;
	JsonAllocator jsonAllocator_;
	WrSerializer ser_;
	key_string tupleData_;

	vector<string> precepts_;
};

}  // namespace reindexer
