#include "msgpackparser.h"

MsgPackValue::MsgPackValue(const msgpack_object* _p) : p(_p) {}

MsgPackTag MsgPackValue::getTag() const {
	assertrx(p);
	return MsgPackTag(p->type);
}

MsgPackValue MsgPackValue::operator[](std::string_view key) const {
	if (getTag() != MSGPACK_MAP) {
		throw reindexer::Error(errParseMsgPack, "Can't convert msgpack field '{}' to object or array", key.data());
	}
	if (p->via.map.ptr->key.type != MSGPACK_OBJECT_STR) {
		throw reindexer::Error(errParseMsgPack, "Maps with string keys are only allowed for MsgPack!");
	}
	for (msgpack_object_kv* it = p->via.map.ptr; it != p->via.map.ptr + p->via.map.size; ++it) {
		if (std::string_view(it->key.via.str.ptr, it->key.via.str.size) == key) {
			return MsgPackValue(&it->val);
		}
	}
	static MsgPackValue emptyValue{nullptr};
	return emptyValue;
}

uint32_t MsgPackValue::size() const {
	uint32_t size = 0;
	if (isValid()) {
		int tag = getTag();
		if (tag == MSGPACK_ARRAY) {
			size = p->via.array.size;
		} else if (tag == MSGPACK_MAP) {
			size = p->via.map.size;
		}
	}
	return size;
}

bool MsgPackValue::isValid() const { return (p != nullptr); }

MsgPackIterator::MsgPackIterator(uint32_t idx, const MsgPackValue* v) : index(idx), val(v) {}

void MsgPackIterator::operator++() {
	assertrx(val);
	int tag = val->getTag();
	if (tag == MSGPACK_ARRAY || tag == MSGPACK_MAP) ++index;
}

bool MsgPackIterator::operator!=(const MsgPackIterator& x) const { return index != x.index; }

const MsgPackValue& MsgPackIterator::operator*() const {
	assertrx(val && val->p);
	int tag = val->getTag();
	if (tag == MSGPACK_MAP) {
		msgpack_object_kv* kv = val->p->via.map.ptr + index;
		assert(kv);
		elemValue = MsgPackValue{&kv->val};
		return elemValue;
	} else if (tag == MSGPACK_ARRAY) {
		elemValue = MsgPackValue{val->p->via.array.ptr + index};
		return elemValue;
	} else {
		return *val;
	}
}

bool MsgPackIterator::isValid() const { return (val != nullptr); }

MsgPackParser::MsgPackParser() : unpacked_(), inUse_(false) {}
MsgPackParser::~MsgPackParser() { dispose(); }

void MsgPackParser::prepare() {
	if (inUse_) {
		dispose();
	}
	msgpack_unpacked_init(&unpacked_);
	inUse_ = true;
}

void MsgPackParser::dispose() {
	if (inUse_) {
		msgpack_unpacked_destroy(&unpacked_);
		inUse_ = false;
	}
}
