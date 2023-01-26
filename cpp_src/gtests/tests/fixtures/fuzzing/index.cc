#include "index.h"
#include "core/indexdef.h"

namespace fuzzing {

reindexer::IndexDef Index::IndexDef(RandomGenerator& rnd, const NsScheme& scheme) const {
	IndexOpts opts;
	const bool pk = rnd.PkIndex(isPk);
	opts.PK(pk);
	opts.Array(rnd.RndArrayField(isArray));
	opts.Sparse(rnd.SparseIndex(pk));
	opts.Dense(rnd.DenseIndex());
	opts.RTreeType(static_cast<IndexOpts::RTreeIndexType>(rnd.RndInt(IndexOpts::Linear, IndexOpts::RStar)));

	FieldType fldType = std::visit(
		reindexer::overloaded{[](const Child& c) noexcept { return c.type; }, [](const Children&) noexcept { return FieldType::Struct; }},
		content);
	std::string fieldType = rnd.IndexFieldType(fldType);
	std::string indexType = rnd.RndIndexType(fldType, isPk);
	reindexer::JsonPaths jsonPaths;
	std::visit(reindexer::overloaded{[&](const Child& c) { jsonPaths.push_back(scheme.GetJsonPath(c.fieldPath)); },
									 [&](const Children& c) {
										 for (const auto& child : c) {
											 jsonPaths.push_back(scheme.GetJsonPath(child.fieldPath));
										 }
									 }},
			   content);
	return {name, std::move(jsonPaths), std::move(indexType), std::move(fieldType), std::move(opts), rnd.ExpiredIndex()};
}

void Index::Dump(std::ostream& os, const NsScheme& scheme, size_t offset) const {
	for (size_t i = 0; i < offset; ++i) os << "  ";
	os << "{\n";
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "name: " << name << '\n';
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "pk: " << (isPk ? "true" : "false") << '\n';
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "array: " << (isArray ? "true" : "false") << '\n';
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	std::visit(reindexer::overloaded{[&](const Child& child) {
										 os << "field: {\n";
										 for (size_t i = 0; i < offset + 2; ++i) os << "  ";
										 os << "type: " << child.type << '\n';
										 for (size_t i = 0; i < offset + 2; ++i) os << "  ";
										 os << "json: " << scheme.GetJsonPath(child.fieldPath) << '\n';
										 for (size_t i = 0; i <= offset; ++i) os << "  ";
										 os << "}\n";
									 },
									 [&](const Children& children) {
										 os << "fields: [\n";
										 for (const auto& c : children) {
											 for (size_t i = 0; i < offset + 2; ++i) os << "  ";
											 os << "{\n";
											 for (size_t i = 0; i <= offset + 2; ++i) os << "  ";
											 os << "type: " << c.type << '\n';
											 for (size_t i = 0; i <= offset + 2; ++i) os << "  ";
											 os << "json: " << scheme.GetJsonPath(c.fieldPath) << '\n';
											 for (size_t i = 0; i < offset + 2; ++i) os << "  ";
											 os << "}\n";
										 }
										 for (size_t i = 0; i <= offset; ++i) os << "  ";
										 os << "]\n";
									 }},
			   content);
}

}  // namespace fuzzing
