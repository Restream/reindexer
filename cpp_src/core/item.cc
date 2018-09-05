
#include "core/item.h"
#include "core/fieldrefimpl.h"
#include "core/itemimpl.h"

namespace reindexer {

Item::FieldRef::FieldRef(int field, ItemImpl *itemImpl) : impl_(std::make_shared<IndexedFieldRefImpl>(field, itemImpl)) {}
Item::FieldRef::FieldRef(const string &jsonPath, ItemImpl *itemImpl)
	: impl_(std::make_shared<NonIndexedFieldRefImpl>(jsonPath, itemImpl)) {}

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

const string &Item::FieldRef::Name() { return impl_->Name(); }
Item::FieldRef::operator KeyRef() { return impl_->GetValue(); }
Item::FieldRef::operator KeyRefs() { return impl_->GetValues(); }

Item::FieldRef &Item::FieldRef::operator=(KeyRef kr) {
	impl_->SetValue(kr);
	return *this;
}

Item::FieldRef &Item::FieldRef::operator=(const KeyRefs &krs) {
	impl_->SetValue(krs);
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

Item::FieldRef Item::operator[](const string &name) {
	int field = 0;
	if (impl_->Type().FieldByName(name, field)) {
		return FieldRef(field, impl_);
	} else {
		return FieldRef(name, impl_);
	}
}

Item::FieldRef Item::operator[](const char *name) {
	int field = 0;
	if (impl_->Type().FieldByName(name, field)) {
		return FieldRef(field, impl_);
	} else {
		return FieldRef(name, impl_);
	}
}

void Item::SetPrecepts(const vector<string> &precepts) { impl_->SetPrecepts(precepts); }
bool Item::IsTagsUpdated() { return impl_->tagsMatcher().isUpdated(); }
Item &Item::Unsafe(bool enable) {
	impl_->Unsafe(enable);
	return *this;
}

}  // namespace reindexer
