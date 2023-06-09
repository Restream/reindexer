#include "query_generator.h"
#include "core/query/query.h"

namespace fuzzing {

reindexer::Query QueryGenerator::operator()() {
	if (namespaces_.empty() || rndGen_.RndErr()) {
		std::unordered_set<std::string> generatedNames;
		return reindexer::Query{rndGen_.NsName(generatedNames)};
	}
	const auto& ns = rndGen_.RndWhich(namespaces_);
	reindexer::Query query{ns.GetName()};
	enum By : uint8_t { Index, Field, Empty, END = Empty };
	switch (rndGen_.RndWhich<By, 1, 1, 1>()) {
		case Index:
			if (const auto& indexes = ns.GetIndexes(); !indexes.empty()) {
				const auto& idx = rndGen_.RndWhich(indexes);
				std::visit(reindexer::overloaded{[&](const Index::Child& c) { rndGen_.RndWhere(query, idx.name, {c.type}); },
												 [&](const Index::Children& c) {
													 std::vector<FieldType> types;
													 types.reserve(c.size());
													 for (const auto& child : c) types.push_back(child.type);
													 rndGen_.RndWhere(query, idx.name, types);
												 }},
						   idx.content);
			}
			break;
		case Field: {
			const auto path = rndGen_.RndField(ns.GetScheme());
			const FieldType type = ns.GetScheme().GetFieldType(path);
			if (type != FieldType::Struct) {
				rndGen_.RndWhere(query, ns.GetScheme().GetJsonPath(path), {type});
			}
		} break;
		case Empty:
			break;
		default:
			assertrx(0);
	}
	return query;
}

}  // namespace fuzzing
