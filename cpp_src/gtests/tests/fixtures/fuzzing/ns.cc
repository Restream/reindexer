#include "ns.h"
#include <ostream>

struct FieldPathHash {
	size_t operator()(const fuzzing::FieldPath& fp) const noexcept {
		constexpr static std::hash<size_t> hasher;
		size_t ret = fp.size();
		for (const size_t f : fp) ret = ((ret * 127) ^ (ret >> 3)) ^ hasher(f);
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

Ns::Ns(std::string name, std::ostream& os, RandomGenerator::ErrFactorType errorFactor)
	: name_{std::move(name)}, rndGen_{os, errorFactor}, scheme_{name_, rndGen_} {
	std::unordered_set<std::string> generatedNames;
	std::unordered_set<FieldPath, FieldPathHash> usedPaths;
	constexpr static size_t kMaxTries = 10;
	const size_t idxCount = rndGen_.IndexesCount();
	const bool withErr = rndGen_.RndErr();
	indexes_.reserve(idxCount);
	for (size_t i = 0; i < idxCount; ++i) {
		const bool uniqueName = rndGen_.UniqueName();
		if (rndGen_.CompositeIndex()) {
			bool fail = false;
			Index index{uniqueName ? rndGen_.IndexName(generatedNames) : std::string{}, Index::Children{}};
			auto& children = std::get<Index::Children>(index.content);
			const size_t size = rndGen_.CompositeIndexSize();
			children.reserve(size);
			for (size_t i = 0; i < size; ++i) {
				auto fldPath = rndGen_.RndScalarField(scheme_);
				FieldType fldType;
				if (scheme_.IsStruct(fldPath)) {
					if (!rndGen_.RndErr()) {
						fail = true;
						break;
					}
					fldType = rndGen_.RndFieldType();
				} else {
					fldType = scheme_.GetFieldType(fldPath);
				}
				if (!uniqueName) {
					if (!index.name.empty()) index.name += '+';
					if (fldPath.empty()) {
						index.name += rndGen_.FieldName(generatedNames);
					} else {
						index.name += scheme_.GetJsonPath(fldPath);
					}
				}
				children.emplace_back(Index::Child{fldType, std::move(fldPath)});
			}
			if (fail) continue;
			index.isArray = rndGen_.RndArrayField(false);
			indexes_.emplace_back(std::move(index));
		} else {
			FieldPath fldPath;
			size_t tryCounts = 0;
			do {
				fldPath = rndGen_.RndField(scheme_);
			} while (!withErr && ++tryCounts < kMaxTries && usedPaths.find(fldPath) != usedPaths.end());
			if (tryCounts >= kMaxTries) continue;
			usedPaths.insert(fldPath);
			if (scheme_.IsStruct(fldPath)) {
				if (!rndGen_.RndErr()) continue;
				const auto fldType = rndGen_.RndFieldType();
				indexes_.emplace_back(Index{rndGen_.IndexName(generatedNames), Index::Child{fldType, std::move(fldPath)}});
			} else {
				const auto fldType = scheme_.GetFieldType(fldPath);
				const bool isArray = scheme_.IsArray(fldPath);
				std::string idxName = uniqueName ? rndGen_.IndexName(generatedNames) : scheme_.GetJsonPath(fldPath);
				indexes_.emplace_back(Index{std::move(idxName), Index::Child{fldType, std::move(fldPath)}});
				indexes_.back().isArray = rndGen_.RndArrayField(isArray);
			}
		}
	}
	std::vector<size_t> ii;
	for (size_t i = 0, s = indexes_.size(); i < s; ++i) {
		const auto& idx = indexes_[i];
		if (!idx.isArray &&
			(std::holds_alternative<Index::Children>(idx.content) || availablePkFieldType(std::get<Index::Child>(idx.content).type))) {
			ii.push_back(i);
		}
	}
	if (ii.empty()) {
		if (!rndGen_.RndErr()) {
			auto path = scheme_.AddRndPkField(rndGen_);
			const auto fldType = scheme_.GetFieldType(path);
			std::string name = rndGen_.UniqueName() ? rndGen_.IndexName(generatedNames) : scheme_.GetJsonPath(path);
			indexes_.emplace_back(Index{std::move(name), Index::Child{fldType, std::move(path)}});
			indexes_.back().isArray = false;
			indexes_.back().isPk = true;
		}
	} else {
		indexes_[rndGen_.RndWhich(ii)].isPk = true;
	}
}

void Ns::AddIndex(Index& index, bool isSparse) {
	if (isSparse) return;
	std::visit(reindexer::overloaded{[&](const Index::Child& c) { scheme_.AddIndex(c.fieldPath, isSparse); },
									 [&](const Index::Children& c) {
										 for (const auto& child : c) {
											 scheme_.AddIndex(child.fieldPath, isSparse);
										 }
									 }},
			   index.content);
}

void Ns::Dump(std::ostream& os) const {
	os << "{\n";
	scheme_.Dump(os, 1);
	os << "  indexes: [\n";
	for (const auto& i : indexes_) i.Dump(os, scheme_, 2);
	os << "  ]\n}" << std::endl;
}

}  // namespace fuzzing
