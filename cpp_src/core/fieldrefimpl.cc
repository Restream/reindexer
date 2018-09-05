#include "fieldrefimpl.h"
#include "core/itemimpl.h"

namespace reindexer {

IndexedFieldRefImpl::IndexedFieldRefImpl(int field, ItemImpl *itemImpl) : field_(field), itemImpl_(itemImpl) {}
IndexedFieldRefImpl::~IndexedFieldRefImpl() {}

const string &IndexedFieldRefImpl::Name() { return itemImpl_->Type().Field(field_).Name(); }

KeyRef IndexedFieldRefImpl::GetValue() {
	KeyRefs kr;
	itemImpl_->GetPayload().Get(field_, kr);
	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

KeyRefs IndexedFieldRefImpl::GetValues() {
	KeyRefs kr;
	return itemImpl_->GetPayload().Get(field_, kr);
}

void IndexedFieldRefImpl::SetValue(KeyRef &kr) { itemImpl_->SetField(field_, KeyRefs{kr}); }
void IndexedFieldRefImpl::SetValue(const KeyRefs &krefs) { itemImpl_->SetField(field_, krefs); }

NonIndexedFieldRefImpl::NonIndexedFieldRefImpl(const std::string &jsonPath, ItemImpl *itemImpl)
	: jsonPath_(jsonPath), itemImpl_(itemImpl) {}
NonIndexedFieldRefImpl::~NonIndexedFieldRefImpl() {}

const string &NonIndexedFieldRefImpl::Name() { return jsonPath_; }

KeyRef NonIndexedFieldRefImpl::GetValue() {
	KeyRefs kr = itemImpl_->GetValueByJSONPath(jsonPath_);
	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

KeyRefs NonIndexedFieldRefImpl::GetValues() { return itemImpl_->GetValueByJSONPath(jsonPath_); }

void NonIndexedFieldRefImpl::SetValue(KeyRef &) { throw Error(errConflict, "CjsonFieldRefImpl::SetValue not implemented yet"); }
void NonIndexedFieldRefImpl::SetValue(const KeyRefs &) { throw Error(errConflict, "CjsonFieldRefImpl::SetValue not implemented yet"); }

}  // namespace reindexer
