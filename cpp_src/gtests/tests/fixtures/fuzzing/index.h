#pragma once

#include "ns_scheme.h"
#include "random_generator.h"

namespace reindexer {
struct IndexDef;
}  // namespace reindexer

namespace fuzzing {

struct Index {
	reindexer::IndexDef IndexDef(RandomGenerator&, const NsScheme&) const;

	struct Child {
		FieldType type;
		FieldPath fieldPath;
	};
	using Children = std::vector<Child>;

	std::string name;
	std::variant<Child, Children> content;
	bool isPk{false};
	bool isArray{false};
	void Dump(std::ostream&, const NsScheme&, size_t offset) const;
};

}  // namespace fuzzing
