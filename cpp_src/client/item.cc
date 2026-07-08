#include "client/item.h"
#include "client/itemimplbase.h"
#include "tools/catch_and_return.h"

namespace reindexer::client {

const static Error kInvalidItemStatus{errNotValid, "Item is not valid"};

// NOLINTNEXTLINE (bugprone-throw-keyword-missing)
Item::Item() : status_(kInvalidItemStatus) {}

Item::Item(Item&& other) noexcept = default;

Item::Item(ItemImplBase* impl) : impl_(impl) {}
Item::Item(Error err) : impl_(nullptr), status_(std::move(err)) {}

Item& Item::operator=(Item&& other) noexcept = default;

Item::~Item() = default;

Error Item::FromJSON(std::string_view slice, char** endp, bool pkOnly) { return impl_->FromJSON(slice, endp, pkOnly); }
Error Item::FromCJSON(std::string_view slice) & noexcept {
	try {
		impl_->FromCJSON(slice);
	}
	CATCH_AND_RETURN;
	return {};
}
void Item::FromCJSONImpl(std::string_view slice) & { impl_->FromCJSON(slice); }
Error Item::FromMsgPack(std::string_view slice, size_t& offset) { return impl_->FromMsgPack(slice, offset); }
std::string_view Item::GetCJSON() { return impl_->GetCJSON(); }
std::string_view Item::GetJSON() { return impl_->GetJSON(); }
std::string_view Item::GetMsgPack() { return impl_->GetMsgPack(); }
void Item::SetPrecepts(std::vector<std::string> precepts) { impl_->SetPrecepts(std::move(precepts)); }
bool Item::IsTagsUpdated() const noexcept { return impl_->tagsMatcher().isUpdated(); }
int Item::GetStateToken() const noexcept { return impl_->tagsMatcher().stateToken(); }
Item& Item::Unsafe(bool enable) noexcept {
	impl_->Unsafe(enable);
	return *this;
}

}  // namespace reindexer::client
