#pragma once
#include "client/itemimplbase.h"

#include "debug/backtrace.h"

namespace reindexer {
namespace client {

template <typename C>
class ItemImpl : public ItemImplBase {
public:
	ItemImpl() = default;
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher, C *client, std::chrono::milliseconds requestTimeout)
		: ItemImplBase(type, tagsMatcher), requestTimeout_(requestTimeout), client_(client) {}
	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher, C *client, std::chrono::milliseconds requestTimeout)
		: ItemImplBase(type, v, tagsMatcher), requestTimeout_(requestTimeout), client_(client) {}
	ItemImpl(ItemImpl<C> &&) = default;
	ItemImpl(const ItemImpl<C> &) = delete;
	ItemImpl<C> &operator=(ItemImpl<C> &&) = default;
	ItemImpl<C> &operator=(const ItemImpl<C> &) = delete;

protected:
	Error tryToUpdateTagsMatcher() override final;

	std::chrono::milliseconds requestTimeout_ = std::chrono::milliseconds{0};
	C *client_ = nullptr;
};

}  // namespace client
}  // namespace reindexer
