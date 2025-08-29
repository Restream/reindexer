#include "query_generator.h"
#include "core/query/query.h"
#include "index.h"
#include "ns.h"

namespace fuzzing {

reindexer::Query QueryGenerator::operator()() {
	if (namespaces_.empty() || rndGen_.RndErr()) {
		return reindexer::Query{rndGen_.GenerateNsName()};
	}
	const auto& ns = rndGen_.RndWhich(namespaces_);
	reindexer::Query query{ns.GetName()};
	enum [[nodiscard]] By : uint8_t { Index, Field, Empty, END = Empty };
	switch (rndGen_.RndWhich<By, 1, 1, 1>()) {
		case Index:
			if (const auto& indexes = ns.GetIndexes(); !indexes.empty()) {
				const auto& idx = rndGen_.RndWhich(indexes);
				std::visit(reindexer::overloaded{[&](const Index::Child& c) { rndGen_.RndWhere(query, idx.Name(), c.type, idx.Type()); },
												 [&](const Index::Children& c) {
													 std::vector<FieldType> types;
													 types.reserve(c.size());
													 for (const auto& child : c) {
														 types.push_back(child.type);
													 }
													 rndGen_.RndWhereComposite(query, idx.Name(), std::move(types), idx.Type());
												 }},
						   idx.Content());
			}
			break;
		case Field: {
			const auto path = rndGen_.RndField(ns.GetScheme());
			const FieldType type = ns.GetScheme().GetFieldType(path);
			if (type == FieldType::Struct) {  // TODO object find
			} else {
				const std::optional<IndexType> indexType =
					ns.GetScheme().IsTtl(path, ns.GetIndexes()) ? IndexType::Ttl : std::optional<IndexType>{};
				rndGen_.RndWhere(query, ns.GetScheme().GetJsonPath(path), type, indexType);
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
