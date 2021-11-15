
#include "client/namespace.h"
#include "client/coroqueryresults.h"
#include "client/itemimpl.h"
#include "cororpcclient.h"
#include "synccororeindexerimpl.h"

namespace reindexer {
namespace client {

Namespace::Namespace(const string& _name)
	: name(_name), payloadType(name, {PayloadFieldType(KeyValueString, "-tuple", {}, false)}), tagsMatcher_(payloadType) {}

Item Namespace::NewItem() {
	shared_lock<shared_timed_mutex> lk(lck_);
	auto impl = new ItemImpl<CoroRPCClient>(payloadType, tagsMatcher_, nullptr, std::chrono::milliseconds(0));
	return Item(impl);
}

template <typename ClientT>
Item Namespace::NewItem(ClientT& client, std::chrono::milliseconds execTimeout) {
	shared_lock<shared_timed_mutex> lk(lck_);
	auto impl = new ItemImpl<ClientT>(payloadType, tagsMatcher_, &client, execTimeout);
	return Item(impl);
}

void Namespace::UpdateTagsMatcher(const TagsMatcher& tm) {
	std::lock_guard lk(lck_);
	if (tm.version() > tagsMatcher_.version() && !tagsMatcher_.try_merge(tm)) {
		WrSerializer wrser;
		tm.serialize(wrser);
		std::string_view buf = wrser.Slice();
		Serializer ser(buf.data(), buf.length());
		tagsMatcher_.deserialize(ser, tm.version(), tm.stateToken());
	}
}

void Namespace::TryReplaceTagsMatcher(TagsMatcher&& tm) {
	std::lock_guard lk(lck_);
	if (tagsMatcher_.version() >= tm.version() && tagsMatcher_.stateToken() == tm.stateToken()) {
		return;
	}
	tagsMatcher_ = std::move(tm);
}

template Item Namespace::NewItem(CoroRPCClient& client, std::chrono::milliseconds);
template Item Namespace::NewItem(SyncCoroReindexerImpl& client, std::chrono::milliseconds);

}  // namespace client
}  // namespace reindexer
