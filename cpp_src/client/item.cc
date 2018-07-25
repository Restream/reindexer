
#include "client/item.h"
#include "client/itemimpl.h"

namespace reindexer {
namespace client {

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

Item::~Item() { delete impl_; }
Error Item::FromJSON(const string_view &slice, char **endp, bool pkOnly) { return impl_->FromJSON(slice, endp, pkOnly); }
Error Item::FromCJSON(const string_view &slice) { return impl_->FromCJSON(slice); }
string_view Item::GetCJSON() { return impl_->GetCJSON(); }
string_view Item::GetJSON() { return impl_->GetJSON(); }
void Item::SetPrecepts(const vector<string> &precepts) { impl_->SetPrecepts(precepts); }
bool Item::IsTagsUpdated() { return impl_->tagsMatcher().isUpdated(); }
Item &Item::Unsafe(bool enable) {
	impl_->Unsafe(enable);
	return *this;
}

}  // namespace client
}  // namespace reindexer
