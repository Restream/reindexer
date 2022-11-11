#pragma once

#include <mutex>
#include "client/item.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "estl/shared_mutex.h"

namespace reindexer {
namespace client {

class Namespace {
public:
	typedef std::shared_ptr<Namespace> Ptr;

	Namespace(std::string name);
	Item NewItem();

	template <typename ClientT>
	Item NewItem(ClientT& client, std::chrono::milliseconds execTimeout);
	void TryReplaceTagsMatcher(TagsMatcher&& tm, bool checkVersion = true);
	TagsMatcher GetTagsMatcher() const {
		shared_lock<shared_timed_mutex> lk(lck_);
		return tagsMatcher_;
	}

	const std::string name;
	const PayloadType payloadType;

private:
	TagsMatcher tagsMatcher_;
	mutable shared_timed_mutex lck_;  // TODO: Remove this mutex. SyncCoro* classes probably have to have own copies of tm/pt
};

}  // namespace client
}  // namespace reindexer
