#pragma once

#include <optional>
#include "core/namespace/float_vectors_indexes.h"
#include "core/type_consts.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "float_vector.h"

namespace reindexer {

class NamespaceImpl;
class ItemRef;
class PayloadType;
class FieldsFilter;

class FloatVectorsHolderVector : private h_vector<FloatVector, 1> {
	using Base = h_vector<FloatVector, 1>;

public:
	using Base::reserve;
	using Base::size;
	using Base::empty;
	using Base::resize;

	void Add(FloatVector&& vect) {
		assertrx_dbg(!vect.IsEmpty());
		if (vect.IsEmpty()) {
			return;
		}
		emplace_back(std::move(vect));
	}
	void Add(ConstFloatVectorView vect) {
		assertrx_dbg(!vect.IsEmpty());
		if (vect.IsEmpty()) {
			return;
		}
		emplace_back(std::move(vect));
	}
	ConstFloatVectorView Back() const noexcept { return ConstFloatVectorView{back()}; }
	ConstFloatVectorView Get(size_t i) const noexcept { return ConstFloatVectorView{operator[](i)}; }
};

class FloatVectorsHolderMap {
	using VectorsById = fast_hash_map<IdType, FloatVectorsHolderVector>;

	struct NsFloatVectorsHolder {
		NsFloatVectorsHolder(FloatVectorsIndexes&& fvIdx) noexcept : fvIndexes{std::move(fvIdx)} {}
		FloatVectorsIndexes fvIndexes;
		VectorsById vectorsById;
	};

public:
	template <typename It>
	void Add(const NamespaceImpl&, It begin, It end, const FieldsFilter&);
	[[nodiscard]] bool Empty() const noexcept {
		if (!vectorsByNs_.has_value()) {
			return true;
		}
		for (const auto& holder : *vectorsByNs_) {
			if (!holder.second.vectorsById.empty()) {
				return false;
			}
		}
		return true;
	}

private:
	void add(const FloatVectorsIndexes&, VectorsById&, ItemRef&, const PayloadType&, const FieldsFilter&);
	void clear(const FloatVectorsIndexes&, ItemRef&, const PayloadType&);

	std::optional<fast_hash_map<const NamespaceImpl*, NsFloatVectorsHolder>> vectorsByNs_;
};

}  // namespace reindexer
