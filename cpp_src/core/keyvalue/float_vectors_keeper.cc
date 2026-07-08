#include "core/keyvalue/float_vectors_keeper.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/keyvalue/float_vector.h"

namespace reindexer {

namespace {
constexpr uint32_t kLimitNumberRemovedElements = 1000;
}  // namespace

FloatVectorsKeeper::KeeperTag::~KeeperTag() {
	if (keeper_) {
		keeper_->Deregister(*this);
	}
}

FloatVectorsKeeper::KeeperTag FloatVectorsKeeper::Register() {
	lock_guard lock(lock_);

	if (currOwner_ == kInvalidOwnerId) {
		throw Error(errLogic, "Owner's identification has been exhausted");
	}

	auto keeper = shared_from_this();
	auto it = queue_.emplace(queue_.cend(), currOwner_, FloatVector(), map_.end());
	return KeeperTag(currOwner_++, it, keeper);
}

void FloatVectorsKeeper::Deregister(const KeeperTag& tag) noexcept {
	lock_guard lock(lock_);

	assertrx_dbg(!tag.Get()->deleted);
	tag.Get()->deleted = true;
}

void FloatVectorsKeeper::Remove(FloatVectorId id) {
	lock_guard lock(lock_);

	auto itVector = map_.find(id);
	if (itVector != map_.end()) {
		itVector->second->deleted = true;
		map_.erase(itVector);
	}
}

void FloatVectorsKeeper::RemoveUnused() {
	DataQueue tmp;
	do {
		{
			lock_guard lock(lock_);

			if (queue_.empty()) {
				break;	// stop
			}

			uint32_t removedCount = 0;
			auto it = queue_.begin();
			while (it != queue_.end() && removedCount <= kLimitNumberRemovedElements && (it->deleted || it->mapIt != map_.end())) {
				if (!it->deleted) {
					map_.erase(it->mapIt);
				}
				++removedCount;
				++it;
			}

			if (removedCount == 0) {
				break;	// stop
			}

			tmp.splice(tmp.end(), queue_, queue_.begin(), it);
		}
		tmp.clear();
	} while (true);
}

size_t FloatVectorsKeeper::GetMemStat() const {
	static constexpr auto classSize = sizeof(*this);

	static constexpr auto pointerSize = sizeof(void*);

	static constexpr auto mapNodeSize = sizeof(DocsMap::value_type::first_type) + sizeof(DocsMap::value_type::second_type);

	static constexpr auto queueNodeSize = (2 * pointerSize) + sizeof(DataQueue::value_type);

	static const auto vectorSize = index_.Dimension().Value() * sizeof(float);
	static const auto queueNodeFullSize = queueNodeSize + vectorSize;

	lock_guard lock(lock_);
	auto queueSize = queue_.size() * queueNodeFullSize;
	auto mapSize = map_.bucket_count() * pointerSize + map_.size() * mapNodeSize;

	return classSize + queueSize + mapSize;
}

}  // namespace reindexer
