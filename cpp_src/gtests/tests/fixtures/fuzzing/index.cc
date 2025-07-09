#include "index.h"
#include <iostream>
#include "core/indexdef.h"
#include "estl/overloaded.h"
#include "ns_scheme.h"
#include "random_generator.h"

namespace fuzzing {

reindexer::IndexDef Index::IndexDef(RandomGenerator& rnd, const NsScheme& scheme, const std::vector<Index>& indexes) const {
	const FieldType fldType = std::visit(
		reindexer::overloaded{[](const Child& c) noexcept { return c.type; }, [](const Children&) noexcept { return FieldType::Struct; }},
		content_);
	IndexOpts opts;
	const bool pk = rnd.PkIndex(*isPk_);
	opts.PK(pk);
	opts.Array(*rnd.RndArrayField(isArray_));
	opts.Sparse(*rnd.RndSparseIndex(isSparse_));
	opts.Dense(rnd.DenseIndex());
	opts.RTreeType(static_cast<IndexOpts::RTreeIndexType>(rnd.RndInt(IndexOpts::Linear, IndexOpts::RStar)));

	std::string fieldType = rnd.IndexFieldType(fldType);
	std::string indexType{ToText(rnd.RndIndexType(type_))};
	reindexer::JsonPaths jsonPaths;
	std::visit(reindexer::overloaded{[&](const Child& c) { jsonPaths.push_back(scheme.GetJsonPath(c.fieldPath)); },
									 [&](const Children& c) {
										 jsonPaths.reserve(c.size());
										 for (const auto& child : c) {
											 if (rnd.RndBool(0.5)) {
												 std::vector<size_t> scalarIndexes;
												 scalarIndexes.reserve(indexes.size());
												 for (size_t i = 0, s = indexes.size(); i < s; ++i) {
													 if (const auto* c = std::get_if<Child>(&indexes[i].content_);
														 c && c->fieldPath == child.fieldPath) {
														 scalarIndexes.push_back(i);
													 }
												 }
												 if (!scalarIndexes.empty()) {
													 jsonPaths.push_back(indexes[rnd.RndWhich(scalarIndexes)].name_);
													 continue;
												 }
											 }
											 jsonPaths.push_back(scheme.GetJsonPath(child.fieldPath));
										 }
									 }},
			   content_);
	return {name_, std::move(jsonPaths), std::move(indexType), std::move(fieldType), std::move(opts), rnd.ExpiredIndex()};
}

void Index::Dump(std::ostream& os, const NsScheme& scheme, size_t offset) const {
	for (size_t i = 0; i < offset; ++i) {
		os << "  ";
	}
	os << "{\n";
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "name: " << name_ << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "type: " << type_ << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "pk: " << std::boolalpha << *isPk_ << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "array: " << std::boolalpha << *IsArray() << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "sparse: " << std::boolalpha << *IsSparse() << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	std::visit(reindexer::overloaded{[&](const Child& child) {
										 os << "composite: false\n";
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "field: {\n";
										 for (size_t i = 0; i < offset + 2; ++i) {
											 os << "  ";
										 }
										 os << "type: " << child.type << '\n';
										 for (size_t i = 0; i < offset + 2; ++i) {
											 os << "  ";
										 }
										 os << "json: " << scheme.GetJsonPath(child.fieldPath) << '\n';
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "}\n";
									 },
									 [&](const Children& children) {
										 os << "composite: true\n";
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "fields: [\n";
										 for (const auto& c : children) {
											 for (size_t i = 0; i < offset + 2; ++i) {
												 os << "  ";
											 }
											 os << "{\n";
											 for (size_t i = 0; i <= offset + 2; ++i) {
												 os << "  ";
											 }
											 os << "type: " << c.type << '\n';
											 for (size_t i = 0; i <= offset + 2; ++i) {
												 os << "  ";
											 }
											 os << "json: " << scheme.GetJsonPath(c.fieldPath) << '\n';
											 for (size_t i = 0; i < offset + 2; ++i) {
												 os << "  ";
											 }
											 os << "}\n";
										 }
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "]\n";
									 }},
			   content_);
}

}  // namespace fuzzing
