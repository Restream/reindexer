#pragma once

#include "client/item.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "estl/shared_mutex.h"
#include "estl/thread_annotation_attributes.h"

namespace reindexer {
namespace client {

class [[nodiscard]] Namespace final : public intrusive_atomic_rc_base {
public:
	Namespace(std::string name);
	Item NewItem() RX_REQUIRES(!mtx_);

	template <typename ClientT>
	Item NewItem(ClientT& client, std::chrono::milliseconds execTimeout) RX_REQUIRES(!mtx_);
	void TryReplaceTagsMatcher(TagsMatcher&& tm, bool checkVersion = true) RX_REQUIRES(!mtx_);
	TagsMatcher GetTagsMatcher() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lk(mtx_);
		return tagsMatcher_;
	}
	int GetStateToken() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lk(mtx_);
		return int(tagsMatcher_.stateToken());
	}

	const std::string name;
	const PayloadType payloadType;

private:
	TagsMatcher tagsMatcher_ RX_GUARDED_BY(mtx_);
	mutable shared_timed_mutex mtx_;  // TODO: Remove this mutex. SyncCoro* classes probably have to have own copies of tm/pt
};

}  // namespace client
}  // namespace reindexer
