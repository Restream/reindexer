#pragma once

#include <optional>
#include <vector>
#include "core/keyvalue/float_vector.h"
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"

namespace reindexer {

class [[nodiscard]] SingleIndexData {
public:
	struct [[nodiscard]] Vector {
		using Type = float;
		Vector(FloatVectorDimension dimensions) noexcept : id{-1}, vec{FloatVector::CreateNotInitialized(dimensions)} {}

		void Reset() noexcept { id = -1; }
		bool IsValid() const noexcept { return id >= 0; }

		IdType id;
		FloatVectorImpl<Type> vec;
	};

	explicit SingleIndexData(int field, size_t expectedRows, FloatVectorDimension dimensions) : field_(field), dimensions_{dimensions} {
		assertrx_dbg(field > 0);
		vectors_.reserve(expectedRows);
		mapping_.reserve(expectedRows);
	}

	std::pair<ConstFloatVectorView, bool> Upsert(IdType id, ConstFloatVectorView data) {
		assertrx(data.IsEmpty() || data.Dimension() == dimensions_);
		size_t vecID = 0;
		bool added = false;
		if (auto [it, emplaced] = mapping_.try_emplace(id, 0); emplaced) {
			vecID = getEmptyVectorID(added);
			it->second = vecID;
		} else {
			vecID = it->second;
		}
		auto& vecRef = vectors_[vecID];
		vecRef.id = id;
		if (!data.IsEmpty()) {
			if (vecRef.vec.IsEmpty()) {
				vecRef.vec = FloatVector::CreateNotInitialized(dimensions_);
			}
			std::memcpy(vecRef.vec.RawData(), data.Data(), sizeof(Vector::Type) * data.Dimension().Value());
		} else if (!vecRef.vec.IsEmpty()) {
			vecRef.vec = FloatVectorImpl<SingleIndexData::Vector::Type>();
		}
		return std::make_pair(ConstFloatVectorView{vecRef.vec}, added);
	}
	bool Delete(IdType id) {
		if (auto found = mapping_.find(id); found != mapping_.end()) {
			const size_t vecID = found->second;
			mapping_.erase(found);
			assertrx_dbg(vectors_[vecID].IsValid());
			vectors_[vecID].Reset();
			vacantIDs_.emplace_back(vecID);
			return true;
		}
		return false;
	}
	std::optional<ConstFloatVectorView> TryGetValue(IdType id) {
		if (auto found = mapping_.find(id); found != mapping_.end()) {
			const size_t vecID = found->second;
			assertrx_dbg(vectors_[vecID].IsValid());
			return ConstFloatVectorView(vectors_[vecID].vec);
		}
		return std::nullopt;
	}
	size_t Field() const noexcept { return field_; }
	size_t Buckets() const noexcept { return vectors_.size(); }
	const Vector* operator[](size_t idx) const noexcept { return &vectors_[idx]; }

private:
	size_t getEmptyVectorID(bool& added) {
		auto ret = vectors_.size();
		added = vacantIDs_.empty();
		if (vacantIDs_.empty()) {
			vectors_.emplace_back(dimensions_);
		} else {
			ret = vacantIDs_.back();
			vacantIDs_.pop_back();
		}
		return ret;
	}

	const size_t field_{0};
	const FloatVectorDimension dimensions_;
	fast_hash_map<IdType, size_t> mapping_;
	std::vector<Vector> vectors_;
	std::vector<IdType> vacantIDs_;
};

class LocalTransaction;
class NamespaceImpl;

class [[nodiscard]] TransactionContext {
public:
	TransactionContext(const NamespaceImpl& ns, const LocalTransaction& tx);

	bool HasMultithreadIndexes() const noexcept { return !indexesData_.empty(); }
	ConstFloatVectorView Upsert(int field, IdType id, ConstFloatVectorView data) {
		if (auto indexData = getIndexData(field); indexData) {
			auto [vec, sizeChanged] = indexData->Upsert(id, data);
			buckets_ += size_t(sizeChanged);
			return vec;
		}
		return ConstFloatVectorView();
	}
	bool Delete(int field, IdType id) {
		if (auto indexData = getIndexData(field); indexData) {
			return indexData->Delete(id);
		}
		return false;
	}
	std::optional<ConstFloatVectorView> TryGetValue(int field, IdType id) {
		if (auto indexData = getIndexData(field); indexData) {
			return indexData->TryGetValue(id);
		}
		return std::nullopt;
	}
	size_t Buckets() const noexcept { return buckets_; }
	std::pair<size_t, const SingleIndexData::Vector*> operator[](size_t idx) const noexcept {
		size_t offset = 0;
		for (auto& d : indexesData_) {
			if (offset + d.Buckets() >= idx) {
				return std::make_pair(d.Field(), d[idx - offset]);
			}
			offset += d.Buckets();
		}
		std::abort();  // Do no expecting overflow here
	}

private:
	SingleIndexData* getIndexData(int field) {
		assertrx_dbg(field > 0);
		for (auto& d : indexesData_) {
			if (d.Field() == size_t(field)) {
				return &d;
			}
		}
		return nullptr;
	}

	std::vector<SingleIndexData> indexesData_;
	size_t buckets_{0};
};

class [[nodiscard]] TransactionConcurrentInserter {
public:
	TransactionConcurrentInserter(NamespaceImpl& ns, size_t threads) noexcept : ns_{ns}, threads_{threads} {}

	void operator()(const TransactionContext& ctx) noexcept;

private:
	void threadFn(std::atomic<size_t>& nextId, const TransactionContext& ctx) noexcept;

	NamespaceImpl& ns_;
	const size_t threads_;
};

}  // namespace reindexer
