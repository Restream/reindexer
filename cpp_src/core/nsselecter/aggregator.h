#pragma once

#include <climits>
#include "core/payload/payloadiface.h"
#include "core/type_consts.h"
#include "estl/fast_hash_set.h"
#include "vendor/cpp-btree/btree_map.h"

namespace reindexer {

struct AggregationResult;

class Aggregator {
public:
	struct SortingEntry {
		int field;
		bool desc;
		enum { Count = -1 };
	};

	Aggregator(const PayloadType &, const FieldsSet &, AggType aggType, const h_vector<string, 1> &names,
			   const h_vector<SortingEntry, 1> &sort = {}, size_t limit = UINT_MAX, size_t offset = 0, bool compositeIndexFields = false);
	Aggregator();
	Aggregator(Aggregator &&);
	~Aggregator();

	void Aggregate(const PayloadValue &lhs);
	AggregationResult GetResult() const;

	Aggregator(const Aggregator &) = delete;
	Aggregator &operator=(const Aggregator &) = delete;
	Aggregator &operator=(Aggregator &&) = delete;

	AggType Type() const noexcept { return aggType_; }
	const h_vector<string, 1> &Names() const noexcept { return names_; }

protected:
	enum Direction { Desc = -1, Asc = 1 };
	class MultifieldComparator;
	class SinglefieldComparator;
	using MultifieldMap = btree::btree_map<PayloadValue, int, MultifieldComparator>;
	using SinglefieldMap = btree::btree_map<Variant, int, SinglefieldComparator>;

	void aggregate(const Variant &variant);

	PayloadType payloadType_;
	FieldsSet fields_;
	double result_ = 0;
	int hitCount_ = 0;
	AggType aggType_;
	h_vector<string, 1> names_;
	size_t limit_ = UINT_MAX;
	size_t offset_ = 0;

	std::unique_ptr<MultifieldMap> multifieldFacets_;
	std::unique_ptr<SinglefieldMap> singlefieldFacets_;

	class RelaxVariantCompare {
	public:
		RelaxVariantCompare(const PayloadType &type, const FieldsSet &fields) : type_(type), fields_(fields) {}
		bool operator()(const Variant &v1, const Variant &v2) const {
			if (v1.Type() != v2.Type()) return false;
			switch (v1.Type()) {
				case KeyValueInt64:
				case KeyValueDouble:
				case KeyValueString:
				case KeyValueBool:
				case KeyValueInt:
					return v1.Compare(v2) == 0;
				case KeyValueComposite:
					return ConstPayload(type_, static_cast<const PayloadValue &>(v1)).IsEQ(static_cast<const PayloadValue &>(v2), fields_);
				default:
					abort();
			}
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};
	struct DistinctHasher {
		DistinctHasher(const PayloadType &type, const FieldsSet &fields) : type_(type), fields_(fields) {}
		size_t operator()(const Variant &v) const {
			switch (v.Type()) {
				case KeyValueInt64:
				case KeyValueDouble:
				case KeyValueString:
				case KeyValueBool:
				case KeyValueInt:
					return v.Hash();
				case KeyValueComposite:
					return ConstPayload(type_, static_cast<const PayloadValue &>(v)).GetHash(fields_);
				default:
					abort();
			}
			assert(type_);
		}

	private:
		PayloadType type_;
		FieldsSet fields_;
	};

	typedef fast_hash_set<Variant, DistinctHasher, RelaxVariantCompare> HashSetVariantRelax;
	std::unique_ptr<HashSetVariantRelax> distincts_;
	bool compositeIndexFields_;
};

}  // namespace reindexer
