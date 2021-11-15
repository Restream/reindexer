#pragma once
#include "client/itemimplbase.h"

#include "debug/backtrace.h"

namespace reindexer {
namespace client {

class SyncCoroReindexerImpl;
class CoroRPCClient;
class CoroQueryResults;

template <typename C>
class ItemImpl : public ItemImplBase {
public:
	ItemImpl(PayloadType type, const TagsMatcher &tagsMatcher, C *client, std::chrono::milliseconds requestTimeout)
		: ItemImplBase(type, tagsMatcher), requestTimeout_(requestTimeout), client_(client) {}

	ItemImpl(PayloadType type, PayloadValue v, const TagsMatcher &tagsMatcher, C *client, std::chrono::milliseconds requestTimeout)
		: ItemImplBase(type, v, tagsMatcher), requestTimeout_(requestTimeout), client_(client) {}

protected:
	Error tryToUpdateTagsMatcher() override final;

	std::chrono::milliseconds requestTimeout_ = std::chrono::milliseconds{0};
	C *client_ = nullptr;
};

}  // namespace client
}  // namespace reindexer
