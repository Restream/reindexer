#pragma once

#include <vector>
#include "estl/h_vector.h"
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/stringstools.h"
#include "vendor/hopscotch/hopscotch_map.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class WrSerializer;

struct FieldProps {
	FieldProps() = default;
	FieldProps(std::string _type, bool _isArray = false, bool _isRequired = false, bool _allowAdditionalProps = false)
		: type(std::move(_type)), isArray(_isArray), isRequired(_isRequired), allowAdditionalProps(_allowAdditionalProps) {}
	FieldProps(FieldProps&&) = default;
	FieldProps& operator=(FieldProps&&) = default;

	bool operator==(const FieldProps& rh) const {
		return type == rh.type && isArray == rh.isArray && isRequired == rh.isRequired && allowAdditionalProps == rh.allowAdditionalProps;
	}

	std::string type;
	bool isArray = false;
	bool isRequired = false;
	bool allowAdditionalProps = false;
};

class Schema;

class PrefixTree {
public:
	using PathT = h_vector<std::string, 10>;

	PrefixTree();

	Error AddPath(FieldProps props, const PathT& splittedPath) noexcept;
	std::vector<std::string> GetSuggestions(string_view path) const;
	std::vector<std::string> GetPaths() const;
	bool HasPath(string_view path, bool allowAdditionalFields) const noexcept;

	struct PrefixTreeNode;
	using map = tsl::hopscotch_map<std::string, std::unique_ptr<PrefixTreeNode>, hash_str, equal_str>;
	struct PrefixTreeNode {
		void GetPaths(std::string&& basePath, std::vector<std::string>& pathsList) const;

		FieldProps props_;
		map children_;
	};

private:
	friend Schema;
	static std::string pathToStr(const PathT&);

	PrefixTreeNode* findNode(string_view path, bool* maybeAdditionalField = nullptr) const noexcept;

	PrefixTreeNode root_;
};

class Schema {
public:
	Schema() = default;
	explicit Schema(string_view json);

	std::vector<string> GetSuggestions(string_view path) const { return paths_.GetSuggestions(path); }
	std::vector<std::string> GetPaths() const noexcept { return paths_.GetPaths(); }
	bool HasPath(string_view path, bool allowAdditionalFields = false) const noexcept {
		return paths_.HasPath(path, allowAdditionalFields);
	}

	Error FromJSON(span<char> json);
	Error FromJSON(string_view json);
	void GetJSON(WrSerializer&) const;

	const PrefixTree::PrefixTreeNode* GetRoot() const { return &paths_.root_; }

private:
	void parseJsonNode(const gason::JsonNode& node, PrefixTree::PathT& splittedPath, bool isRequired);

	PrefixTree paths_;
	std::string originalJson_;
};

}  // namespace reindexer
