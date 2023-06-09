#pragma once

#include <string>
#include <variant>
#include <vector>
#include "estl/overloaded.h"
#include "random_generator.h"

namespace reindexer {

class WrSerializer;
class JsonBuilder;

}  // namespace reindexer

namespace fuzzing {

class NsScheme {
	struct Node {
		using Children = std::vector<Node>;
		struct Child {
			FieldType type;
		};

		std::string name;
		std::variant<Child, Children> content;
		bool sparse{true};
		bool array{false};
		void Dump(std::ostream&, size_t offset) const;
	};

public:
	NsScheme(std::string ns, RandomGenerator& rnd) : ns_{std::move(ns), Node::Children{}} {
		bool canBeArray = true, canBeSparse = true;
		fillChildren(std::get<Node::Children>(ns_.content), rnd, 0, canBeArray, canBeSparse);
	}
	size_t FieldsCount(const FieldPath& path) const noexcept {
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
	bool IsStruct(const FieldPath& path) const noexcept {
		if (path.empty()) return true;
		const Node::Children& ref = findLastContainer(path);
		assertrx(ref.size() > path.back());
		return std::holds_alternative<Node::Children>(ref[path.back()].content);
	}
	bool IsPoint(const FieldPath& path) const noexcept {
		if (path.empty()) return false;
		const Node::Children& ref = findLastContainer(path);
		assertrx(ref.size() > path.back());
		return !std::holds_alternative<Node::Children>(ref[path.back()].content) &&
			   std::get<Node::Child>(ref[path.back()].content).type == FieldType::Point;
	}
	bool IsArray(const FieldPath& path) const noexcept {
		if (path.empty()) return ns_.array;
		const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
		for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
			assertrx(ptr->size() > path[i]);
			const auto& idx = (*ptr)[path[i]];
			if (idx.array) return true;
			std::visit(reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; },
											 [](const Node::Child&) noexcept { assert(0); }},
					   idx.content);
		}
		assertrx(ptr->size() > path.back());
		return (*ptr)[path.back()].array;
	}
	FieldType GetFieldType(const FieldPath& path) const noexcept {
		assertrx(!path.empty());
		const Node::Children& ref = findLastContainer(path);
		assertrx(ref.size() > path.back());
		return std::visit(reindexer::overloaded{[](const Node::Child& c) noexcept { return c.type; },
												[](const Node::Children&) noexcept { return FieldType::Struct; }},
						  ref[path.back()].content);
	}
	void SetFieldType(const FieldPath& path, FieldType ft) noexcept {
		assertrx(!path.empty());
		Node::Children& ref = findLastContainer(path);
		assertrx(ref.size() > path.back());
		return std::visit(
			reindexer::overloaded{[ft](Node::Child& c) noexcept { c.type = ft; }, [](Node::Children&) noexcept { assert(0); }},
			ref[path.back()].content);
	}
	std::string GetJsonPath(const FieldPath& path) const noexcept {
		if (path.empty()) return {};
		std::string res;
		const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
		for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
			assertrx(ptr->size() > path[i]);
			const auto& idx = (*ptr)[path[i]];
			res += idx.name;
			std::visit(reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; },
											 [](const Node::Child&) noexcept { assert(0); }},
					   idx.content);
			res += '.';
		}
		assertrx(ptr->size() > path.back());
		res += (*ptr)[path.back()].name;
		return res;
	}
	void AddIndex(const FieldPath& path, bool isSparse) {
		if (path.empty()) return;
		if (!isSparse) ns_.sparse = false;
		Node::Children* ptr = &std::get<Node::Children>(ns_.content);
		for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
			assertrx(ptr->size() > path[i]);
			if (!isSparse) {
				(*ptr)[path[i]].sparse = false;
			}
			std::visit(reindexer::overloaded{[&ptr](Node::Children& c) noexcept { ptr = &c; }, [](Node::Child&) noexcept { assert(0); }},
					   (*ptr)[path[i]].content);
		}
		assertrx(ptr->size() > path.back());
		mark((*ptr)[path.back()], isSparse);
	}
	void NewItem(reindexer::WrSerializer&, RandomGenerator&);
	void Dump(std::ostream& os, size_t offset) const { ns_.Dump(os, offset); }
	FieldPath AddRndPkField(RandomGenerator& rnd) {
		auto& children = std::get<Node::Children>(ns_.content);
		children.emplace_back(Node{rnd.FieldName(generatedNames_), Node::Child{rnd.RndPkIndexFieldType()}});
		children.back().array = false;
		children.back().sparse = false;
		return {children.size() - 1};
	}

private:
	static void mark(Node& node, bool isSparse) {
		if (!isSparse) {
			node.sparse = false;
		}
		std::visit(reindexer::overloaded{[](Node::Child&) noexcept {},
										 [isSparse](Node::Children& c) noexcept {
											 for (Node& n : c) mark(n, isSparse);
										 }},
				   node.content);
	}
	void fillChildren(Node::Children& children, RandomGenerator& rnd, unsigned level, bool& canBeArray, bool& canBeSparse) {
		const size_t fieldsCount = rnd.FieldsCount(level == 0);
		children.reserve(fieldsCount);
		for (size_t i = 0; i < fieldsCount; ++i) {
			auto fName = rnd.FieldName(generatedNames_);
			const auto type = rnd.RndFieldType(level);
			if (type == FieldType::Struct) {
				children.emplace_back(Node{std::move(fName), Node::Children{}});
				fillChildren(std::get<Node::Children>(children.back().content), rnd, level + 1, canBeArray, canBeSparse);
				if (canBeArray || rnd.RndErr()) {
					children.back().array = rnd.RndArrayField();
				}
				if (!canBeSparse && !rnd.RndErr()) {
					children.back().sparse = false;
				}
			} else {
				children.emplace_back(Node{std::move(fName), Node::Child{type}});
				if (type == FieldType::Point) {
					canBeSparse = false;
					canBeArray = false;
					children.back().sparse = false;
				}
				if (canBeArray || rnd.RndErr()) {
					children.back().array = rnd.RndArrayField();
				}
			}
		}
	}
	const Node::Children& findLastContainer(const FieldPath& path) const noexcept {
		const Node::Children* ptr = &std::get<Node::Children>(ns_.content);
		for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
			assertrx(ptr->size() > path[i]);
			std::visit(reindexer::overloaded{[&ptr](const Node::Children& c) noexcept { ptr = &c; },
											 [](const Node::Child&) noexcept { assert(0); }},
					   (*ptr)[path[i]].content);
		}
		return *ptr;
	}
	Node::Children& findLastContainer(const FieldPath& path) noexcept {
		Node::Children* ptr = &std::get<Node::Children>(ns_.content);
		for (size_t i = 0, s = path.size() - 1; i < s; ++i) {
			assertrx(ptr->size() > path[i]);
			std::visit(reindexer::overloaded{[&ptr](Node::Children& c) noexcept { ptr = &c; }, [](Node::Child&) noexcept { assert(0); }},
					   (*ptr)[path[i]].content);
		}
		return *ptr;
	}
	void toJson(reindexer::JsonBuilder&, const Node::Children&, RandomGenerator&);
	void rndValueToJson(reindexer::JsonBuilder&, FieldType, std::string_view name, RandomGenerator&);
	Node ns_;
	std::unordered_set<std::string> generatedNames_;
};

}  // namespace fuzzing
