
#include "client/namespace.h"
#include "client/itemimpl.h"
#include "client/rpcclient.h"

namespace reindexer {
namespace client {

Namespace::Namespace(std::string _name)
	: name(std::move(_name)),
	  payloadType(name, {PayloadFieldType(KeyValueType::String{}, "-tuple", {}, IsArray_False)}),
	  tagsMatcher_(payloadType) {}

Item Namespace::NewItem() {
	shared_lock<shared_timed_mutex> lk(lck_);
	auto impl = new ItemImpl<RPCClient>(payloadType, tagsMatcher_, nullptr, std::chrono::milliseconds(0));
	return Item(impl);
}

template <typename ClientT>
Item Namespace::NewItem(ClientT& client, std::chrono::milliseconds execTimeout) {
	shared_lock<shared_timed_mutex> lk(lck_);
	auto impl = new ItemImpl<ClientT>(payloadType, tagsMatcher_, &client, execTimeout);
	return Item(impl);
}

void Namespace::TryReplaceTagsMatcher(TagsMatcher&& tm, bool checkVersion) {
	std::lock_guard lk(lck_);
	if (checkVersion && tagsMatcher_.version() >= tm.version() && tagsMatcher_.stateToken() == tm.stateToken()) {
		return;
	}
	tagsMatcher_ = std::move(tm);
}

template Item Namespace::NewItem(RPCClient& client, std::chrono::milliseconds);
template Item Namespace::NewItem(ReindexerImpl& client, std::chrono::milliseconds);

}  // namespace client
}  // namespace reindexer
