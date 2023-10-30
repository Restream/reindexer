#pragma once

#include <string>
#include <variant>
#include <vector>
#include "types.h"

namespace reindexer {
struct IndexDef;
}  // namespace reindexer

namespace fuzzing {

class RandomGenerator;
class NsScheme;

class Index {
public:
	struct Child {
		FieldType type;
		FieldPath fieldPath;
	};
	using Children = std::vector<Child>;

	Index(std::string name, IndexType type, IsArray isArray, IsSparse isSparse, Children content) noexcept
		: name_{std::move(name)}, type_{type}, content_{std::move(content)}, isArray_{isArray}, isSparse_{isSparse} {}
	Index(std::string name, IndexType type, IsArray isArray, IsSparse isSparse, Child content) noexcept
		: name_{std::move(name)}, type_{type}, content_{std::move(content)}, isArray_{isArray}, isSparse_{isSparse} {}

	const std::string& Name() const& noexcept { return name_; }
	const std::string& Name() const&& = delete;
	IndexType Type() const noexcept { return type_; }
	const auto& Content() const& noexcept { return content_; }
	const auto& Content() const&& = delete;
	bool IsPk() const noexcept { return isPk_; }
	void SetPk() noexcept { isPk_ = true; }
	bool IsArray() const noexcept { return isArray_ == IsArray::Yes; }
	auto IsSparse() const noexcept { return isSparse_; }

	reindexer::IndexDef IndexDef(RandomGenerator&, const NsScheme&, const std::vector<Index>&) const;

	void Dump(std::ostream&, const NsScheme&, size_t offset) const;

private:
	std::string name_;
	IndexType type_;
	std::variant<Child, Children> content_;
	bool isPk_{false};
	enum IsArray isArray_ { IsArray::No };
	enum IsSparse isSparse_ { IsSparse::No };
};

}  // namespace fuzzing
