
#include "client/item.h"
#include "client/itemimpl.h"

namespace reindexer {
namespace client {

Item::Item() : status_(errNotValid) {}

Item::Item(Item &&other) noexcept : impl_(std::move(other.impl_)), status_(std::move(other.status_)), id_(other.id_) {}

Item::Item(ItemImpl *impl) : impl_(impl) {}
Item::Item(const Error &err) : impl_(nullptr), status_(err) {}

Item &Item::operator=(Item &&other) noexcept {
	if (&other != this) {
		impl_ = std::move(other.impl_);
		status_ = std::move(other.status_);
		id_ = other.id_;
	}
	return *this;
}

Item::~Item() {}

Item::operator bool() const { return impl_ != nullptr; }

Error Item::FromJSON(const string_view &slice, char **endp, bool pkOnly) { return impl_->FromJSON(slice, endp, pkOnly); }
Error Item::FromCJSON(const string_view &slice) { return impl_->FromCJSON(slice); }
Error Item::FromMsgPack(const string_view &slice, size_t &offset) { return impl_->FromMsgPack(slice, offset); }
string_view Item::GetCJSON() { return impl_->GetCJSON(); }
string_view Item::GetJSON() { return impl_->GetJSON(); }
string_view Item::GetMsgPack() { return impl_->GetMsgPack(); }
void Item::SetPrecepts(const vector<string> &precepts) { impl_->SetPrecepts(precepts); }
bool Item::IsTagsUpdated() { return impl_->tagsMatcher().isUpdated(); }
int Item::GetStateToken() { return impl_->tagsMatcher().stateToken(); }

Item &Item::Unsafe(bool enable) {
	impl_->Unsafe(enable);
	return *this;
}

}  // namespace client
}  // namespace reindexer
