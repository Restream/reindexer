#include "ns.h"
#include <ostream>
#include "estl/overloaded.h"
#include "index.h"
#include "tools/assertrx.h"

struct [[nodiscard]] FieldPathHash {
	size_t operator()(const fuzzing::FieldPath& fp) const noexcept {
		constexpr static std::hash<size_t> hasher;
		size_t ret = fp.size();
		for (const size_t f : fp) {
			ret = ((ret * 127) ^ (ret >> 3)) ^ hasher(f);
		}
		return ret;
	}
};

namespace fuzzing {

static bool availablePkFieldType(FieldType ft) {
	switch (ft) {
		case FieldType::Bool:
		case FieldType::Point:
		case FieldType::Struct:
			return false;
		case FieldType::Int:
		case FieldType::Int64:
		case FieldType::Double:
		case FieldType::String:
		case FieldType::Uuid:
			return true;
		default:
			assertrx(false);
			std::abort();
	}
}

static bool availablePkIndexType(IndexType it) {
	switch (it) {
		case IndexType::Store:
		case IndexType::FastFT:
		case IndexType::FuzzyFT:
		case IndexType::RTree:
			return false;
		case IndexType::Hash:
		case IndexType::Tree:
		case IndexType::Ttl:
			return true;
		default:
			assertrx(false);
			std::abort();
	}
}

Ns::Ns(std::string name, RandomGenerator::ErrFactorType errorFactor)
	: name_{std::move(name)}, rndGen_{errorFactor}, scheme_{name_, rndGen_} {
	std::unordered_set<std::string> usedIndexNames;
	std::unordered_set<FieldPath, FieldPathHash> usedPaths;
	constexpr static size_t kMaxTries = 10;
	const size_t idxCount = rndGen_.IndexesCount();
	const bool withErr = rndGen_.RndErr();
	indexes_.reserve(idxCount);
	std::vector<size_t> scalarIndexes;
	scalarIndexes.reserve(idxCount);
	for (size_t i = 0; i < idxCount; ++i) {
		const bool uniqueName = rndGen_.UniqueName();
		if (rndGen_.CompositeIndex(scalarIndexes.size())) {
			reindexer::IsArray isArray = reindexer::IsArray_False;
			bool containsUuid = false;
			std::string indexName;
			Index::Children children;
			const auto fields = rndGen_.RndFieldsForCompositeIndex(scalarIndexes);
			children.reserve(fields.size());
			for (size_t f : fields) {
				Index::Child fieldData;
				if (f < indexes_.size()) {
					const auto& idx = indexes_[f];
					fieldData = std::get<Index::Child>(idx.Content());
					isArray |= idx.IsArray();
				} else {
					fieldData.fieldPath = rndGen_.RndScalarField(scheme_);
					if (scheme_.IsStruct(fieldData.fieldPath)) {
						fieldData.type = rndGen_.RndFieldType();
					} else {
						fieldData.type = scheme_.GetFieldType(fieldData.fieldPath);
					}
				}
				if (!uniqueName) {
					if (!indexName.empty()) {
						indexName += '+';
					}
					indexName += scheme_.GetJsonPath(fieldData.fieldPath);
				}
				containsUuid |= fieldData.type == FieldType::Uuid;
				children.emplace_back(std::move(fieldData));
			}
			const auto indexType =
				containsUuid ? rndGen_.RndIndexType({FieldType::Struct, FieldType::Uuid}) : rndGen_.RndIndexType({FieldType::Struct});
			if (uniqueName) {
				indexName = rndGen_.IndexName(usedIndexNames);
			} else if (!usedIndexNames.insert(indexName).second) {
				indexName = rndGen_.IndexName(usedIndexNames);
				usedIndexNames.insert(indexName);
			}

			indexes_.emplace_back(std::move(indexName), indexType, rndGen_.RndArrayField(isArray), reindexer::IsSparse_False,
								  std::move(children));
		} else {
			FieldPath fldPath;
			size_t tryCounts = 0;
			do {
				fldPath = rndGen_.RndField(scheme_);
			} while (!withErr && ++tryCounts < kMaxTries && usedPaths.find(fldPath) != usedPaths.end());
			if (tryCounts >= kMaxTries) {
				continue;
			}
			usedPaths.insert(fldPath);
			if (scheme_.IsStruct(fldPath)) {
				if (!rndGen_.RndErr()) {
					continue;
				}
				const auto fldType = rndGen_.RndFieldType();
				indexes_.emplace_back(rndGen_.IndexName(usedIndexNames), rndGen_.RndIndexType({fldType}),
									  reindexer::IsArray(rndGen_.RndBool(0.5)), reindexer::IsSparse(rndGen_.RndBool(0.5)),
									  Index::Child{fldType, std::move(fldPath)});
			} else {
				const auto fldType = scheme_.GetFieldType(fldPath);
				const auto isArray = scheme_.IsArray(fldPath);
				std::string idxName;
				if (uniqueName) {
					idxName = rndGen_.IndexName(usedIndexNames);
				} else {
					idxName = scheme_.GetJsonPath(fldPath);
					if (!usedIndexNames.insert(idxName).second) {
						idxName = rndGen_.IndexName(usedIndexNames);
						usedIndexNames.insert(idxName);
					}
				}
				indexes_.emplace_back(std::move(idxName), rndGen_.RndIndexType({fldType}), rndGen_.RndArrayField(isArray),
									  rndGen_.RndSparseIndex(fldType), Index::Child{fldType, std::move(fldPath)});
			}
			if (const auto& idx = indexes_.back();
				!idx.IsArray() && !idx.IsSparse() &&
				std::get<Index::Child>(idx.Content()).type != FieldType::Point) {  // TODO remove point check after #1352
				scalarIndexes.push_back(indexes_.size() - 1);
			}
		}
	}
	if (rndGen_.RndErr()) {
		// Do not set PK index
		return;
	}
	std::vector<size_t> ii;
	for (size_t i = 0, s = indexes_.size(); i < s; ++i) {
		const auto& idx = indexes_[i];
		if (!idx.IsArray() && !idx.IsSparse() && availablePkIndexType(idx.Type()) &&
			(std::holds_alternative<Index::Children>(idx.Content()) || availablePkFieldType(std::get<Index::Child>(idx.Content()).type))) {
			ii.push_back(i);
		}
	}
	if (ii.empty()) {
		auto path = scheme_.AddRndPkField(rndGen_);
		const auto fldType = scheme_.GetFieldType(path);
		std::string indexName;
		if (rndGen_.UniqueName()) {
			indexName = rndGen_.IndexName(usedIndexNames);
		} else {
			indexName = scheme_.GetJsonPath(path);
			if (!usedIndexNames.insert(indexName).second) {
				indexName = rndGen_.IndexName(usedIndexNames);
				usedIndexNames.insert(indexName);
			}
		}
		indexes_.emplace_back(std::move(indexName), rndGen_.RndPkIndexType({fldType}), reindexer::IsArray_False, reindexer::IsSparse_False,
							  Index::Child{fldType, std::move(path)});
		indexes_.back().SetPk();
	} else {
		indexes_[rndGen_.RndWhich(ii)].SetPk();
	}
}

void Ns::AddIndexToScheme(const Index& index, size_t indexNumber) {
	std::visit(reindexer::overloaded{[&](const Index::Child& c) { scheme_.AddIndex(c.fieldPath, indexNumber, index.IsSparse()); },
									 [&](const Index::Children& c) {
										 for (const auto& child : c) {
											 scheme_.AddIndex(child.fieldPath, indexNumber, index.IsSparse());
										 }
									 }},
			   index.Content());
}

void Ns::Dump(std::ostream& os) const {
	os << "{\n";
	scheme_.Dump(os, 1);
	os << "  indexes: [\n";
	for (const auto& i : indexes_) {
		i.Dump(os, scheme_, 2);
	}
	os << "  ]\n}" << std::endl;
}

}  // namespace fuzzing
