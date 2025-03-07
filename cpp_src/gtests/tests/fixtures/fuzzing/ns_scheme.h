#pragma once

#include <string>
#include <unordered_set>
#include <variant>
#include <vector>
#include "types.h"

namespace reindexer {

class WrSerializer;
class JsonBuilder;

}  // namespace reindexer

namespace fuzzing {

class RandomGenerator;
class Index;

class NsScheme {
	struct Node {
		using Children = std::vector<Node>;
		struct Child {
			Child(FieldType t) noexcept : type{t} {}
			FieldType type;
			std::vector<size_t> indexes;
		};

		Node(std::string&& _name, Child&& child) : name(std::move(_name)), content(std::move(child)) {}
		Node(std::string&& _name, Children&& children) : name(std::move(_name)), content(std::move(children)) {}

		std::string name;
		std::variant<Child, Children> content;
		IsSparseT sparse{IsSparseT::Yes};
		IsArrayT array{IsArrayT::No};
		void Dump(std::ostream&, size_t offset) const;
	};

public:
	NsScheme(std::string ns, RandomGenerator& rnd) : ns_{std::move(ns), Node::Children{}} {
		bool canBeArray = true, canBeSparse = true;
		fillChildren(std::get<Node::Children>(ns_.content), rnd, 0, canBeArray, canBeSparse);
	}
	size_t FieldsCount(const FieldPath&) const noexcept;
	bool IsStruct(const FieldPath&) const noexcept;
	bool IsPoint(const FieldPath&) const noexcept;
	bool IsTtl(const FieldPath&, const std::vector<Index>&) const noexcept;
	IsArrayT IsArray(const FieldPath&) const noexcept;
	FieldType GetFieldType(const FieldPath&) const noexcept;
	void SetFieldType(const FieldPath&, FieldType) noexcept;
	std::string GetJsonPath(const FieldPath&) const noexcept;
	void AddIndex(const FieldPath&, size_t index, IsSparseT);
	void NewItem(reindexer::WrSerializer&, RandomGenerator&, const std::vector<Index>&);
	void Dump(std::ostream& os, size_t offset) const { ns_.Dump(os, offset); }
	FieldPath AddRndPkField(RandomGenerator&);

private:
	static void addIndex(Node&, size_t index, IsSparseT);
	void fillChildren(Node::Children&, RandomGenerator&, unsigned level, bool& canBeArray, bool& canBeSparse);
	const Node::Children& findLastContainer(const FieldPath&) const noexcept;
	Node::Children& findLastContainer(const FieldPath&) noexcept;
	void toJson(reindexer::JsonBuilder&, const Node::Children&, RandomGenerator&, const std::vector<Index>&);
	void rndValueToJson(reindexer::JsonBuilder&, FieldType, std::string_view name, const std::vector<size_t>& idxNumbers,
						const std::vector<Index>&, RandomGenerator&);
	static bool isTtl(const std::vector<size_t>& idxNumbers, const std::vector<Index>&) noexcept;

	Node ns_;
	std::unordered_set<std::string> generatedNames_;
};

}  // namespace fuzzing
