#include "fieldrefimpl.h"
#include "core/itemimpl.h"

namespace reindexer {

RegularFieldRefImpl::RegularFieldRefImpl(int field, ItemImpl *itemImpl) : field_(field), itemImpl_(itemImpl) {}
RegularFieldRefImpl::~RegularFieldRefImpl() {}

const string &RegularFieldRefImpl::Name() { return itemImpl_->Type().Field(field_).Name(); }

KeyRef RegularFieldRefImpl::GetValue() {
	KeyRefs kr;
	itemImpl_->GetPayload().Get(field_, kr);
	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

KeyRefs RegularFieldRefImpl::GetValues() {
	KeyRefs kr;
	return itemImpl_->GetPayload().Get(field_, kr);
}

void RegularFieldRefImpl::SetValue(KeyRef &kr) { itemImpl_->SetField(field_, KeyRefs{kr}); }
void RegularFieldRefImpl::SetValue(const KeyRefs &krefs) { itemImpl_->SetField(field_, krefs); }

CjsonFieldRefImpl::CjsonFieldRefImpl(const std::string &jsonPath, ItemImpl *itemImpl) : jsonPath_(jsonPath), itemImpl_(itemImpl) {}
CjsonFieldRefImpl::~CjsonFieldRefImpl() {}

const string &CjsonFieldRefImpl::Name() { return jsonPath_; }

KeyRef CjsonFieldRefImpl::GetValue() {
	KeyRefs kr = itemImpl_->GetValueByJSONPath(jsonPath_);
	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}

KeyRefs CjsonFieldRefImpl::GetValues() { return itemImpl_->GetValueByJSONPath(jsonPath_); }

void CjsonFieldRefImpl::SetValue(KeyRef &) { throw Error(errConflict, "CjsonFieldRefImpl::SetValue not implemented yet"); }
void CjsonFieldRefImpl::SetValue(const KeyRefs &) { throw Error(errConflict, "CjsonFieldRefImpl::SetValue not implemented yet"); }

}  // namespace reindexer
