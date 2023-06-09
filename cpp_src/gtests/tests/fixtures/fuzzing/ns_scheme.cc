#include "ns_scheme.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/serializer.h"

namespace fuzzing {

void NsScheme::NewItem(reindexer::WrSerializer& ser, RandomGenerator& rnd) {
	ser.Reset();
	if (rnd.RndErr()) {
		enum Err : uint8_t { Zero, Random, END = Random };
		switch (rnd.RndWhich<Err, 1, 1>()) {
			case Zero:
				return;
			case Random: {
				const size_t len = rnd.RndInt(0, 10000);
				ser.Reserve(len);
				for (size_t i = 0; i < len; ++i) {
					ser << rnd.RndChar();
				}
			} break;
			default:
				assertrx(0);
		}
	}
	reindexer::JsonBuilder builder{ser};
	toJson(builder, std::get<Node::Children>(ns_.content), rnd);
}

void NsScheme::rndValueToJson(reindexer::JsonBuilder& builder, FieldType ft, std::string_view name, RandomGenerator& rnd) {
	switch (ft) {
		case FieldType::Bool:
			builder.Put(name, rnd.RndBool(0.5));
			break;
		case FieldType::Int:
			builder.Put(name, rnd.RndIntValue());
			break;
		case FieldType::Int64:
			builder.Put(name, rnd.RndInt64Value());
			break;
		case FieldType::Double:
			builder.Put(name, rnd.RndDoubleValue());
			break;
		case FieldType::String:
			builder.Put(name, rnd.RndStringValue());
			break;
		case FieldType::Uuid:
			builder.Put(name, rnd.RndStrUuidValue());
			break;
		case FieldType::Point:
			builder.Array(name, {rnd.RndDoubleValue(), rnd.RndDoubleValue()});
			break;
		case FieldType::Struct: {
			bool canBeArray = false, canBeSparse = false;
			Node::Children children;
			fillChildren(children, rnd, 2, canBeArray, canBeSparse);
			auto obj = builder.Object(name);
			toJson(obj, children, rnd);
		} break;
		default:
			assertrx(0);
	}
}

void NsScheme::toJson(reindexer::JsonBuilder& builder, const Node::Children& children, RandomGenerator& rnd) {
	for (const Node& n : children) {
		if (!rnd.NeedThisNode(n.sparse)) continue;
		if (rnd.RndArrayField(n.array)) {
			auto arr = builder.Array(n.name);
			const size_t arrSize = rnd.ArraySize();
			for (size_t i = 0; i < arrSize; ++i) {
				if (rnd.RndErr()) {
					rndValueToJson(arr, rnd.RndFieldType(), {}, rnd);
				} else {
					std::visit(reindexer::overloaded{[&](const Node::Child& c) { rndValueToJson(arr, c.type, {}, rnd); },
													 [&](const Node::Children& c) {
														 auto obj = arr.Object();
														 toJson(obj, c, rnd);
													 }},
							   n.content);
				}
			}
		} else {
			if (rnd.RndErr()) {
				rndValueToJson(builder, rnd.RndFieldType(), n.name, rnd);
			} else {
				std::visit(reindexer::overloaded{[&](const Node::Child& c) { rndValueToJson(builder, c.type, n.name, rnd); },
												 [&](const Node::Children& c) {
													 auto obj = builder.Object(n.name);
													 toJson(obj, c, rnd);
												 }},
						   n.content);
			}
		}
	}
}

void NsScheme::Node::Dump(std::ostream& os, size_t offset) const {
	for (size_t i = 0; i < offset; ++i) os << "  ";
	os << "{\n";
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "name: " << name << '\n';
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "sparse: " << (sparse ? "true" : "false") << '\n';
	for (size_t i = 0; i <= offset; ++i) os << "  ";
	os << "array: " << (array ? "true" : "false") << '\n';
	std::visit(reindexer::overloaded{[&](const Child& child) {
										 for (size_t i = 0; i <= offset; ++i) os << "  ";
										 os << "type: " << child.type << '\n';
									 },
									 [&](const Children& children) {
										 for (size_t i = 0; i <= offset; ++i) os << "  ";
										 os << "fields: [\n";
										 for (const Node& n : children) n.Dump(os, offset + 2);
										 for (size_t i = 0; i <= offset; ++i) os << "  ";
										 os << "]\n";
									 }},
			   content);
	for (size_t i = 0; i < offset; ++i) os << "  ";
	os << "}\n";
}

}  // namespace fuzzing
