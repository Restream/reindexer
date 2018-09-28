
#include "client/namespace.h"
#include "client/itemimpl.h"

namespace reindexer {
namespace client {

Namespace::Namespace(const string &name)
	: name_(name), payloadType_(name, {PayloadFieldType(KeyValueString, "-tuple", "", false)}), tagsMatcher_(payloadType_) {}
Item Namespace::NewItem() {
	auto impl = new ItemImpl(payloadType_, tagsMatcher_);
	return Item(impl);
}

}  // namespace client
}  // namespace reindexer
