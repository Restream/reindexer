#pragma once

#include <cstdlib>

#include "core/tag_name_index.h"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] IndexedPathNode {
public:
	explicit IndexedPathNode(TagName name) noexcept : name_{name}, type_{Name} {}
	explicit IndexedPathNode(TagIndex index) noexcept : index_{index}, type_{Index} {}
	TagName GetTagName() const noexcept {
		assertrx_dbg(IsTagName());
		return name_;
	}
	TagIndex GetTagIndex() const noexcept {
		assertrx_dbg(IsTagIndex());
		return index_;
	}
	TagIndex& GetTagIndexRef() & noexcept {
		assertrx_dbg(IsTagIndex());
		return index_;
	}
	bool IsTagName() const noexcept { return type_ == Name; }
	bool IsTagNameEmpty() const noexcept { return type_ == Name && name_.IsEmpty(); }
	bool IsTagIndex() const noexcept { return type_ == Index; }
	bool IsTagIndexNotAll() const noexcept { return IsTagIndex() && !index_.IsAll(); }
	bool Match(TagIndex tag) const noexcept { return type_ == Index && index_ == tag; }
	bool Match(TagName tag) const noexcept { return type_ == Name && name_ == tag; }
	bool operator==(const IndexedPathNode& other) const noexcept {
		if (type_ != other.type_) {
			return false;
		}
		switch (type_) {
			case Index:
				return index_ == other.index_;
			case Name:
				return name_ == other.name_;
			default:
				assertrx_dbg(false);
				return false;
		}
	}
	bool operator==(TagName name) const noexcept { return type_ == Name && name_ == name; }
	bool operator==(TagIndex index) const noexcept { return type_ == Index && index_ == index; }

private:
	TagIndex index_{TagIndex::All()};
	TagName name_{TagName::Empty()};
	enum [[nodiscard]] { Index, Name } type_;
};

}  // namespace reindexer
