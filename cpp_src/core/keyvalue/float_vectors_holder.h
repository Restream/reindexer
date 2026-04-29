#pragma once

#include <optional>
#include "core/index/float_vector/float_vector_id.h"
#include "core/keyvalue/float_vector.h"
#include "estl/h_vector.h"

namespace reindexer {

class NamespaceImpl;
class ItemRef;
class PayloadType;
class FieldsFilter;
struct FloatVectorIndexData;

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
		emplace_back(vect);
		return true;
	}
	ConstFloatVectorView Back() const noexcept { return ConstFloatVectorView{back()}; }
	ConstFloatVectorView Get(size_t i) const noexcept { return ConstFloatVectorView{operator[](i)}; }
};

class [[nodiscard]] FloatVectorsHolderMap {
public:
	FloatVectorsHolderMap() noexcept;
	FloatVectorsHolderMap(const FloatVectorsHolderMap&) noexcept = delete;
	FloatVectorsHolderMap(FloatVectorsHolderMap&&) noexcept;
	FloatVectorsHolderMap& operator=(const FloatVectorsHolderMap&) noexcept = delete;
	FloatVectorsHolderMap& operator=(FloatVectorsHolderMap&&) noexcept;
	~FloatVectorsHolderMap();

	template <typename It>
	void Add(const NamespaceImpl&, const It& begin, const It& end, const FieldsFilter&);
	bool Empty() const noexcept;

private:
	struct [[nodiscard]] FloatVectorIndexInfo;
	struct [[nodiscard]] NsFloatVectorIndexes {
		const NamespaceImpl* ns;
		size_t indexesCnt;
		std::unique_ptr<std::optional<FloatVectorIndexInfo>[]> indexes;
	};

	template <typename It>
	void updatePayload(const NamespaceImpl& ns, const FloatVectorIndexData& index, It it, const It& end,
					   std::span<ConstFloatVectorView> vectors);
	template <typename It>
	void add(const NamespaceImpl& ns, const FloatVectorIndexInfo& indexInfo, It it, const It& end, std::vector<FloatVectorId>&,
			 std::vector<ConstFloatVectorView>& vectors);

	std::vector<NsFloatVectorIndexes> vectorsByNs_;
};

}  // namespace reindexer
