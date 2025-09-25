#include "ns_scheme.h"
#include "core/cjson/jsonbuilder.h"
#include "index.h"
#include "random_generator.h"
#include "tools/serializer.h"

namespace fuzzing {

void NsScheme::NewItem(reindexer::WrSerializer& ser, RandomGenerator& rnd, const std::vector<Index>& indexes) {
	ser.Reset();
	if (rnd.RndErr()) {
		enum [[nodiscard]] Err : uint8_t { Zero, Random, END = Random };
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
	toJson(builder, std::get<Node::Children>(ns_.content), rnd, indexes);
}

bool NsScheme::IsStruct(const FieldPath& path) const {
	if (path.empty()) {
		return true;
	}
	const Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	return std::holds_alternative<Node::Children>(ref[path.back()].content);
}

bool NsScheme::IsPoint(const FieldPath& path) const {
	if (path.empty()) {
		return false;
	}
	const Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	return !std::holds_alternative<Node::Children>(ref[path.back()].content) &&
		   std::get<Node::Child>(ref[path.back()].content).type == FieldType::Point;
}

bool NsScheme::isTtl(const std::vector<size_t>& idxNumbers, const std::vector<Index>& indexes) noexcept {
	for (size_t idx : idxNumbers) {
		assertrx(idx < indexes.size());
		if (indexes[idx].Type() == IndexType::Ttl) {
			return true;
		}
	}
	return false;
}

bool NsScheme::IsTtl(const FieldPath& path, const std::vector<Index>& indexes) const {
	if (path.empty()) {
		return false;
	}
	const Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	if (std::holds_alternative<Node::Children>(ref[path.back()].content)) {
		return false;
	}
	return isTtl(std::get<Node::Child>(ref[path.back()].content).indexes, indexes);
}

size_t NsScheme::FieldsCount(const FieldPath& path) const {
	if (path.empty()) {
		return std::get<Node::Children>(ns_.content).size();
	}
	const Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	return std::visit(reindexer::overloaded{[](const Node::Child&) noexcept -> size_t {
												assertrx(false);
												return 0;
											},
											[](const Node::Children& c) noexcept { return c.size(); }},
					  ref[path.back()].content);
}

reindexer::IsArray NsScheme::IsArray(const FieldPath& path) const {
	if (path.empty()) {
		return ns_.array;
	}
	const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
	for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
		assertrx(ptr->size() > path[i]);
		const auto& idx = (*ptr)[path[i]];
		if (idx.array) {
			return reindexer::IsArray_True;
		}
		std::visit(
			reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; }, [](const Node::Child&) noexcept { assertrx(0); }},
			idx.content);
	}
	assertrx(ptr->size() > path.back());
	return (*ptr)[path.back()].array;
}

FieldType NsScheme::GetFieldType(const FieldPath& path) const {
	assertrx(!path.empty());
	const Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	return std::visit(reindexer::overloaded{[](const Node::Child& c) noexcept { return c.type; },
											[](const Node::Children&) noexcept { return FieldType::Struct; }},
					  ref[path.back()].content);
}

void NsScheme::SetFieldType(const FieldPath& path, FieldType ft) {
	assertrx(!path.empty());
	Node::Children& ref = findLastContainer(path);
	assertrx(ref.size() > path.back());
	return std::visit(reindexer::overloaded{[ft](Node::Child& c) noexcept { c.type = ft; }, [](Node::Children&) noexcept { assertrx(0); }},
					  ref[path.back()].content);
}

std::string NsScheme::GetJsonPath(const FieldPath& path) const {
	if (path.empty()) {
		return {};
	}
	std::string res;
	const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
	for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
		assertrx(ptr->size() > path[i]);
		const auto& idx = (*ptr)[path[i]];
		res += idx.name;
		std::visit(
			reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; }, [](const Node::Child&) noexcept { assertrx(0); }},
			idx.content);
		res += '.';
	}
	assertrx(ptr->size() > path.back());
	res += (*ptr)[path.back()].name;
	return res;
}

void NsScheme::AddIndex(const FieldPath& path, size_t index, reindexer::IsSparse isSparse) {
	assertrx(!path.empty());
	if (!isSparse) {
		ns_.sparse = reindexer::IsSparse_False;
	}
	Node::Children* ptr = &std::get<Node::Children>(ns_.content);
	for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
		assertrx(ptr->size() > path[i]);
		if (!isSparse) {
			(*ptr)[path[i]].sparse = reindexer::IsSparse_False;
		}
		std::visit(reindexer::overloaded{[&ptr](Node::Children& c) noexcept { ptr = &c; }, [](Node::Child&) noexcept { assertrx(0); }},
				   (*ptr)[path[i]].content);
	}
	assertrx(ptr->size() > path.back());
	addIndex((*ptr)[path.back()], index, isSparse);
}

FieldPath NsScheme::AddRndPkField(RandomGenerator& rnd) {
	auto& children = std::get<Node::Children>(ns_.content);
	children.emplace_back(Node{rnd.FieldName(generatedNames_), Node::Child{rnd.RndPkIndexFieldType()}});
	children.back().array = reindexer::IsArray_False;
	children.back().sparse = reindexer::IsSparse_False;
	return {children.size() - 1};
}

void NsScheme::addIndex(Node& node, size_t index, reindexer::IsSparse isSparse) {
	if (!isSparse) {
		node.sparse = reindexer::IsSparse_False;
	}
	std::visit(reindexer::overloaded{[index](Node::Child& c) noexcept { c.indexes.push_back(index); },
									 [](Node::Children&) noexcept { assertrx(0); }},
			   node.content);
}

void NsScheme::fillChildren(Node::Children& children, RandomGenerator& rnd, unsigned level, bool& canBeArray, bool& canBeSparse) {
	const size_t fieldsCount = rnd.FieldsCount(level == 0);
	children.reserve(fieldsCount);
	for (size_t i = 0; i < fieldsCount; ++i) {
		auto fName = rnd.FieldName(generatedNames_);
		const auto type = rnd.RndFieldType(level);
		if (type == FieldType::Struct) {
			children.emplace_back(std::move(fName), Node::Children{});
			fillChildren(std::get<Node::Children>(children.back().content), rnd, level + 1, canBeArray, canBeSparse);
			if (canBeArray || rnd.RndErr()) {
				children.back().array = rnd.RndArrayField();
			}
			if (!canBeSparse && !rnd.RndErr()) {
				children.back().sparse = reindexer::IsSparse_False;
			}
		} else {
			children.emplace_back(Node{std::move(fName), Node::Child{type}});
			if (type == FieldType::Point) {
				canBeSparse = false;
				canBeArray = false;
				children.back().sparse = reindexer::IsSparse_False;
			}
			if (canBeArray || rnd.RndErr()) {
				children.back().array = rnd.RndArrayField();
			}
		}
	}
}

const NsScheme::Node::Children& NsScheme::findLastContainer(const FieldPath& path) const {
	const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
	for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
		assertrx(ptr->size() > path[i]);
		std::visit(
			reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; }, [](const Node::Child&) noexcept { assertrx(0); }},
			(*ptr)[path[i]].content);
	}
	return *ptr;
}

NsScheme::Node::Children& NsScheme::findLastContainer(const FieldPath& path) {
	Node::Children* ptr = &std::get<Node::Children>(ns_.content);
	for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
		assertrx(ptr->size() > path[i]);
		std::visit(reindexer::overloaded{[&ptr](Node::Children& c) noexcept { ptr = &c; }, [](Node::Child&) noexcept { assertrx(0); }},
				   (*ptr)[path[i]].content);
	}
	return *ptr;
}

void NsScheme::rndValueToJson(reindexer::JsonBuilder& builder, FieldType ft, std::string_view name, const std::vector<size_t>& idxNumbers,
							  const std::vector<Index>& indexes, RandomGenerator& rnd) {
	switch (ft) {
		case FieldType::Bool:
			builder.Put(name, rnd.RndBool(0.5));
			break;
		case FieldType::Int:
			builder.Put(name, rnd.RndIntValue());
			break;
		case FieldType::Int64:
			if (isTtl(idxNumbers, indexes)) {
				builder.Put(name, rnd.RndTtlValue());
			} else {
				builder.Put(name, rnd.RndInt64Value());
			}
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
			toJson(obj, children, rnd, indexes);
		} break;
		default:
			assertrx(0);
	}
}

void NsScheme::toJson(reindexer::JsonBuilder& builder, const Node::Children& children, RandomGenerator& rnd,
					  const std::vector<Index>& indexes) {
	for (const Node& n : children) {
		if (!rnd.NeedThisNode(n.sparse)) {
			continue;
		}
		if (rnd.RndArrayField(n.array)) {
			auto arr = builder.Array(n.name);
			const size_t arrSize = rnd.ArraySize();
			for (size_t i = 0; i < arrSize; ++i) {
				if (rnd.RndErr()) {
					rndValueToJson(arr, rnd.RndFieldType(), {}, {}, indexes, rnd);
				} else {
					std::visit(
						reindexer::overloaded{[&](const Node::Child& c) { rndValueToJson(arr, c.type, {}, c.indexes, indexes, rnd); },
											  [&](const Node::Children& c) {
												  auto obj = arr.Object();
												  toJson(obj, c, rnd, indexes);
											  }},
						n.content);
				}
			}
		} else {
			if (rnd.RndErr()) {
				rndValueToJson(builder, rnd.RndFieldType(), n.name, {}, indexes, rnd);
			} else {
				std::visit(
					reindexer::overloaded{[&](const Node::Child& c) { rndValueToJson(builder, c.type, n.name, c.indexes, indexes, rnd); },
										  [&](const Node::Children& c) {
											  auto obj = builder.Object(n.name);
											  toJson(obj, c, rnd, indexes);
										  }},
					n.content);
			}
		}
	}
}

void NsScheme::Node::Dump(std::ostream& os, size_t offset) const {
	for (size_t i = 0; i < offset; ++i) {
		os << "  ";
	}
	os << "{\n";
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "name: " << name << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "sparse: " << std::boolalpha << *sparse << '\n';
	for (size_t i = 0; i <= offset; ++i) {
		os << "  ";
	}
	os << "array: " << std::boolalpha << *array << '\n';
	std::visit(reindexer::overloaded{[&](const Child& child) {
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "type: " << child.type << '\n';
									 },
									 [&](const Children& children) {
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "fields: [\n";
										 for (const Node& n : children) {
											 n.Dump(os, offset + 2);
										 }
										 for (size_t i = 0; i <= offset; ++i) {
											 os << "  ";
										 }
										 os << "]\n";
									 }},
			   content);
	for (size_t i = 0; i < offset; ++i) {
		os << "  ";
	}
	os << "}\n";
}

}  // namespace fuzzing
