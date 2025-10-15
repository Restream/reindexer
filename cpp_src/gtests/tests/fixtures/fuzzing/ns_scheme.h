#pragma once

#include <string>
#include <unordered_set>
#include <variant>
#include <vector>
#include "core/enums.h"
#include "types.h"

namespace reindexer {

class WrSerializer;

namespace builders {
class JsonBuilder;
}  // namespace builders
using builders::JsonBuilder;

}  // namespace reindexer

namespace fuzzing {

class RandomGenerator;
class Index;

class [[nodiscard]] NsScheme {
	struct [[nodiscard]] Node {
		using Children = std::vector<Node>;
		struct [[nodiscard]] Child {
			Child(FieldType t) noexcept : type{t} {}
			FieldType type;
			std::vector<size_t> indexes;
		};

		Node(std::string&& _name, Child&& child) : name(std::move(_name)), content(std::move(child)) {}
		Node(std::string&& _name, Children&& children) : name(std::move(_name)), content(std::move(children)) {}

		std::string name;
		std::variant<Child, Children> content;
		reindexer::IsSparse sparse{reindexer::IsSparse_True};
		reindexer::IsArray array{reindexer::IsArray_False};
		void Dump(std::ostream&, size_t offset) const;
	};

public:
	NsScheme(std::string ns, RandomGenerator& rnd) : ns_{std::move(ns), Node::Children{}} {
		bool canBeArray = true, canBeSparse = true;
		fillChildren(std::get<Node::Children>(ns_.content), rnd, 0, canBeArray, canBeSparse);
	}
	size_t FieldsCount(const FieldPath&) const;
	bool IsStruct(const FieldPath&) const;
	bool IsPoint(const FieldPath&) const;
	bool IsTtl(const FieldPath&, const std::vector<Index>&) const;
	reindexer::IsArray IsArray(const FieldPath&) const;
	FieldType GetFieldType(const FieldPath&) const;
	void SetFieldType(const FieldPath&, FieldType);
	std::string GetJsonPath(const FieldPath&) const;
	void AddIndex(const FieldPath&, size_t index, reindexer::IsSparse);
	void NewItem(reindexer::WrSerializer&, RandomGenerator&, const std::vector<Index>&);
	void Dump(std::ostream& os, size_t offset) const { ns_.Dump(os, offset); }
	FieldPath AddRndPkField(RandomGenerator&);

private:
	static void addIndex(Node&, size_t index, reindexer::IsSparse);
	void fillChildren(Node::Children&, RandomGenerator&, unsigned level, bool& canBeArray, bool& canBeSparse);
	const Node::Children& findLastContainer(const FieldPath&) const;
	Node::Children& findLastContainer(const FieldPath&);
	void toJson(reindexer::JsonBuilder&, const Node::Children&, RandomGenerator&, const std::vector<Index>&);
	void rndValueToJson(reindexer::JsonBuilder&, FieldType, std::string_view name, const std::vector<size_t>& idxNumbers,
						const std::vector<Index>&, RandomGenerator&);
	static bool isTtl(const std::vector<size_t>& idxNumbers, const std::vector<Index>&) noexcept;

	Node ns_;
	std::unordered_set<std::string> generatedNames_;
};

}  // namespace fuzzing
