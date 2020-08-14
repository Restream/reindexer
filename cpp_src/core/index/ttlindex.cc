#include "ttlindex.h"

namespace reindexer {

template <typename T>
Index *TtlIndex<T>::Clone() {
	return new TtlIndex<T>(*this);
}

template <typename T>
TtlIndex<T>::TtlIndex(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields)
	: IndexOrdered<T>(idef, payloadType, fields), expireAfter_(idef.expireAfter_) {}

template <typename T>
TtlIndex<T>::TtlIndex(const TtlIndex<T> &other) : IndexOrdered<T>(other), expireAfter_(other.expireAfter_) {}

template <typename T>
int64_t TtlIndex<T>::GetTTLValue() const {
	return expireAfter_;
}

Index *TtlIndex_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	if (idef.opts_.IsPK() || idef.opts_.IsDense()) {
		return new TtlIndex<number_map<int64_t, Index::KeyEntryPlain>>(idef, payloadType, fields);
	}
	return new TtlIndex<number_map<int64_t, Index::KeyEntry>>(idef, payloadType, fields);
}

}  // namespace reindexer
