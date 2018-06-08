
#include "core/item.h"
#include "core/itemimpl.h"

namespace reindexer {

Item::Item(Item &&other) noexcept : impl_(other.impl_), status_(std::move(other.status_)), id_(other.id_), version_(other.version_) {
	other.impl_ = nullptr;
}

Item &Item::operator=(Item &&other) noexcept {
	if (&other != this) {
		delete impl_;
		impl_ = other.impl_;
		status_ = std::move(other.status_);
		id_ = other.id_;
		version_ = other.version_;
		other.impl_ = nullptr;
	}
	return *this;
}

const string &Item::FieldRef::Name() { return impl_->Type().Field(field_).Name(); }

Item::FieldRef::operator KeyRef() {
	KeyRefs kr;
	impl_->GetPayload().Get(field_, kr);
	if (kr.size() != 1) {
		throw Error(errParams, "Invalid array access");
	}
	return kr[0];
}
Item::FieldRef::operator KeyRefs() {
	KeyRefs kr;
	return impl_->GetPayload().Get(field_, kr);
}

Item::FieldRef &Item::FieldRef::operator=(KeyRef kr) {
	impl_->SetField(field_, KeyRefs{kr});
	return *this;
}

Item::FieldRef &Item::FieldRef::operator=(const KeyRefs &krs) {
	impl_->SetField(field_, krs);
	return *this;
}

Item::~Item() { delete impl_; }

Error Item::FromJSON(const string_view &slice, char **endp, bool pkOnly) { return impl_->FromJSON(slice, endp, pkOnly); }
Error Item::FromCJSON(const string_view &slice, bool pkOnly) { return impl_->FromCJSON(slice, pkOnly); }
string_view Item::GetCJSON() { return impl_->GetCJSON(); }
string_view Item::GetJSON() { return impl_->GetJSON(); }

int Item::NumFields() { return impl_->Type().NumFields(); }
Item::FieldRef Item::operator[](int field) {
	assert(field >= 0 && field < impl_->Type().NumFields());
	return FieldRef(field, impl_);
}
Item::FieldRef Item::operator[](const string &name) { return FieldRef(impl_->Type().FieldByName(name), impl_); }
Item::FieldRef Item::operator[](const char *name) { return FieldRef(impl_->Type().FieldByName(name), impl_); }
void Item::SetPrecepts(const vector<string> &precepts) { impl_->SetPrecepts(precepts); }
bool Item::IsTagsUpdated() { return impl_->tagsMatcher().isUpdated(); }
Item &Item::Unsafe(bool enable) {
	impl_->Unsafe(enable);
	return *this;
}

}  // namespace reindexer
