#pragma once

#include <string>
#include <variant>
#include <vector>
#include "core/enums.h"
#include "types.h"

namespace reindexer {
class IndexDef;
}  // namespace reindexer

namespace fuzzing {

class RandomGenerator;
class NsScheme;

class [[nodiscard]] Index {
public:
	struct [[nodiscard]] Child {
		FieldType type;
		FieldPath fieldPath;
	};
	using Children = std::vector<Child>;

	Index(std::string name, IndexType type, reindexer::IsArray isArray, reindexer::IsSparse isSparse, Children content) noexcept
		: name_{std::move(name)}, type_{type}, content_{std::move(content)}, isArray_{isArray}, isSparse_{isSparse} {}
	Index(std::string name, IndexType type, reindexer::IsArray isArray, reindexer::IsSparse isSparse, Child content) noexcept
		: name_{std::move(name)}, type_{type}, content_{std::move(content)}, isArray_{isArray}, isSparse_{isSparse} {}

	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() const&& = delete;
	IndexType Type() const noexcept { return type_; }
	const auto& Content() const& noexcept { return content_; }
	const auto& Content() const&& = delete;
	reindexer::IsPk IsPk() const noexcept { return isPk_; }
	void SetPk() noexcept { isPk_ = reindexer::IsPk_True; }
	reindexer::IsArray IsArray() const noexcept { return isArray_; }
	reindexer::IsSparse IsSparse() const noexcept { return isSparse_; }

	reindexer::IndexDef IndexDef(RandomGenerator&, const NsScheme&, const std::vector<Index>&) const;

	void Dump(std::ostream&, const NsScheme&, size_t offset) const;

private:
	std::string name_;
	IndexType type_;
	std::variant<Child, Children> content_;
	reindexer::IsPk isPk_{reindexer::IsPk_False};
	reindexer::IsArray isArray_{reindexer::IsArray_False};
	reindexer::IsSparse isSparse_{reindexer::IsSparse_False};
};

}  // namespace fuzzing
