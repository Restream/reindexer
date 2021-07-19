#include "ttlindex.h"

namespace reindexer {

template <typename T>
std::unique_ptr<Index> TtlIndex<T>::Clone() {
	return std::unique_ptr<Index>{new TtlIndex<T>(*this)};
}

template <typename T>
TtlIndex<T>::TtlIndex(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields)
	: IndexOrdered<T>(idef, std::move(payloadType), fields), expireAfter_(idef.expireAfter_) {}

template <typename T>
TtlIndex<T>::TtlIndex(const TtlIndex<T> &other) : IndexOrdered<T>(other), expireAfter_(other.expireAfter_) {}

template <typename T>
int64_t TtlIndex<T>::GetTTLValue() const {
	return expireAfter_;
}

std::unique_ptr<Index> TtlIndex_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields) {
	if (idef.opts_.IsPK() || idef.opts_.IsDense()) {
		return std::unique_ptr<Index>{new TtlIndex<number_map<int64_t, Index::KeyEntryPlain>>(idef, std::move(payloadType), fields)};
	}
	return std::unique_ptr<Index>{new TtlIndex<number_map<int64_t, Index::KeyEntry>>(idef, std::move(payloadType), fields)};
}

}  // namespace reindexer
