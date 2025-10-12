#pragma once

#include <optional>
#include "core/keyvalue/float_vectors_keeper.h"
#include "core/namespace/float_vectors_indexes.h"
#include "estl/h_vector.h"

namespace reindexer {

class NamespaceImpl;
class ItemRef;
class PayloadType;
class FieldsFilter;

class [[nodiscard]] FloatVectorsHolderVector : private h_vector<FloatVector, 1> {
	using Base = h_vector<FloatVector, 1>;

public:
	using Base::reserve;
	using Base::size;
	using Base::empty;
	using Base::resize;

	bool Add(FloatVector&& vect) {
		if (vect.IsEmpty()) {
			return false;
		}
		emplace_back(std::move(vect));
		return true;
	}
	bool Add(ConstFloatVectorView vect) {
		if (vect.IsEmpty()) {
			return false;
		}
		emplace_back(std::move(vect));
		return true;
	}
	ConstFloatVectorView Back() const noexcept { return ConstFloatVectorView{back()}; }
	ConstFloatVectorView Get(size_t i) const noexcept { return ConstFloatVectorView{operator[](i)}; }
};

class [[nodiscard]] FloatVectorsHolderMap {
public:
	FloatVectorsHolderMap() noexcept = default;
	FloatVectorsHolderMap(const FloatVectorsHolderMap&) noexcept = delete;
	FloatVectorsHolderMap(FloatVectorsHolderMap&&) noexcept = default;
	FloatVectorsHolderMap& operator=(const FloatVectorsHolderMap&) noexcept = delete;
	FloatVectorsHolderMap& operator=(FloatVectorsHolderMap&&) noexcept = default;
	~FloatVectorsHolderMap() = default;

	template <typename It>
	void Add(const NamespaceImpl&, const It& begin, const It& end, const FieldsFilter&);
	bool Empty() const noexcept;

private:
	struct [[nodiscard]] FloatVectorIndexInfo {
		FloatVectorIndexInfo() = delete;
		FloatVectorIndexInfo(const FloatVectorIndexData& i, FloatVectorsKeeper::KeeperTag&& t) noexcept : index(i), tag(std::move(t)) {}
		FloatVectorIndexInfo(FloatVectorIndexInfo&&) noexcept = default;
		FloatVectorIndexInfo(const FloatVectorIndexInfo&) noexcept = delete;
		FloatVectorIndexInfo& operator=(const FloatVectorIndexInfo&) noexcept = delete;
		FloatVectorIndexInfo& operator=(FloatVectorIndexInfo&&) noexcept = default;

		FloatVectorIndexData index;
		FloatVectorsKeeper::KeeperTag tag;
	};

	using FloatVectorIndexList = std::vector<FloatVectorIndexInfo>;

	template <typename It>
	void updatePayload(const NamespaceImpl& ns, const FloatVectorIndexData& index, It it, const It& end,
					   std::span<ConstFloatVectorView> vectorsData);
	template <typename It>
	void add(const NamespaceImpl& ns, const FloatVectorIndexInfo& indexInfo, It it, const It& end, std::vector<IdType>& ids,
			 std::vector<ConstFloatVectorView>& vectorsData);

	std::vector<std::pair<const NamespaceImpl*, FloatVectorIndexList>> vectorsByNs_;
};

}  // namespace reindexer
