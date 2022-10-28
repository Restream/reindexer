
#include "client/namespace.h"
#include "client/itemimpl.h"

namespace reindexer {
namespace client {

Namespace::Namespace(std::string name)
	: name_(std::move(name)), payloadType_(name_, {PayloadFieldType(KeyValueString, "-tuple", {}, false)}), tagsMatcher_(payloadType_) {}
Item Namespace::NewItem() {
	shared_lock<shared_timed_mutex> lk(lck_);
	auto impl = new ItemImpl(payloadType_, tagsMatcher_);
	return Item(impl);
}

}  // namespace client
}  // namespace reindexer
