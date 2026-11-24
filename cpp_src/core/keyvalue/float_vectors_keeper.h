#pragma once

#include <span>
#include "core/index/float_vector/float_vector_id.h"
#include "core/keyvalue/float_vector.h"
#include "estl/elist.h"
#include "estl/mutex.h"

namespace reindexer {

class FloatVectorIndex;

class [[nodiscard]] FloatVectorsKeeper final : public std::enable_shared_from_this<FloatVectorsKeeper> {
private:
	using OwnerIdType = uint64_t;
	static constexpr auto kInvalidOwnerId{std::numeric_limits<OwnerIdType>::max()};

	struct Descriptor;
	using DataQueue = elist<Descriptor>;

	struct [[nodiscard]] Descriptor {
		Descriptor(OwnerIdType o, FloatVector&& v, std::unordered_map<FloatVectorId, DataQueue::iterator>::iterator it)
			: owner(o), vect(std::move(v)), mapIt(it) {}
		Descriptor(Descriptor&& o) noexcept = default;
		Descriptor(const Descriptor& o) noexcept = default;
		Descriptor& operator=(const Descriptor& o) noexcept = delete;
		Descriptor& operator=(Descriptor&&) noexcept = delete;

		bool deleted{false};
		OwnerIdType owner{kInvalidOwnerId};
		FloatVector vect;
		std::unordered_map<FloatVectorId, DataQueue::iterator>::iterator mapIt;
	};

	FloatVectorsKeeper(const FloatVectorIndex& index) : index_(index) {}

public:
	class [[nodiscard]] KeeperTag final {
	public:
		KeeperTag(KeeperTag&&) noexcept = default;
		KeeperTag(const KeeperTag&) noexcept = delete;
		KeeperTag& operator=(const KeeperTag&) noexcept = delete;
		KeeperTag& operator=(KeeperTag&&) noexcept = default;
		~KeeperTag();

	private:
		friend class FloatVectorsKeeper;

		KeeperTag(OwnerIdType id, DataQueue::iterator it, const std::shared_ptr<FloatVectorsKeeper>& keeper) noexcept
			: id_(id), it_(it), keeper_(keeper) {}
		[[nodiscard]] DataQueue::iterator Get() const noexcept { return it_; }
		[[nodiscard]] OwnerIdType GetID() const noexcept { return id_; }

		OwnerIdType id_{kInvalidOwnerId};
		DataQueue::iterator it_;
		std::shared_ptr<FloatVectorsKeeper> keeper_;
	};

	static std::shared_ptr<FloatVectorsKeeper> Create(const FloatVectorIndex& index) {
		return std::shared_ptr<FloatVectorsKeeper>{new FloatVectorsKeeper(index)};
	}
	~FloatVectorsKeeper() = default;

	KeeperTag Register();
	void Deregister(const KeeperTag& tag) noexcept;

	void Remove(FloatVectorId);
	void RemoveUnused();

	size_t GetMemStat() const;

private:
	friend class FloatVectorsHolderMap;

	void getFloatVectors(const KeeperTag& tag, std::span<FloatVectorId> ids, std::vector<ConstFloatVectorView>& vectorsData,
						 auto&& floatVectorGetter);

	const FloatVectorIndex& index_;
	OwnerIdType currOwner_{0};

	DataQueue queue_;
	using DocsMap = std::unordered_map<FloatVectorId, DataQueue::iterator>;
	DocsMap map_;

	mutable mutex lock_;
};

}  // namespace reindexer
