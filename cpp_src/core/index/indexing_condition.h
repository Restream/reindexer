#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>
#include "core/cjson/tagspath.h"
#include "core/definitions/collateopts.h"
#include "core/definitions/indexdef.h"
#include "core/enums.h"
#include "core/payload/payloadiface.h"
#include "core/query/queryentry.h"
#include "core/type_consts.h"
#include "estl/expected.h"
#include "estl/h_vector.h"

namespace reindexer {

class Index;
class IndexDef;
class PayloadType;
class TagsMatcher;

namespace ns_indexes {
class Registry;
class TargetState;
class IndexesStorage;
}  // namespace ns_indexes

class [[nodiscard]] IndexingCondition {
public:
	IndexingCondition() = default;

	static Expected<IndexingCondition> Parse(std::string_view sql, const ns_indexes::Registry& registry) noexcept;
	static Expected<IndexingCondition> Parse(std::string_view sql, ns_indexes::TargetState& st) noexcept;

	static void VerifyIndexChange(std::string_view sql, ns_indexes::TargetState& st, const IndexDef& indexDef, const IndexDef* previous);

	static void VerifyQueryEntry(QueryEntry&, const Index*, const PayloadType&);

	[[nodiscard]] bool Empty() const noexcept { return !entries_.Size(); }
	[[nodiscard]] bool Evaluate(const ConstPayload& pl) const;
	[[nodiscard]] bool IsAffectedBy(const IndexedTagsPath& path) const noexcept;
	[[nodiscard]] bool UsesField(std::string_view fieldOrIndexName) const noexcept;

private:
	template <typename State>
	static Expected<IndexingCondition> parse(std::string_view sql, State& state) noexcept;
	void collectReferences(const PayloadType&, const TagsMatcher&);

	QueryEntries entries_;
	// Collation for each query-tree node.
	std::vector<CollateOpts> collateOpts_;
	std::vector<std::string> referencedFieldNames_;
	std::vector<IndexedTagsPath> referencedTagsPaths_;
};

// A snapshot of condition results taken before indexes or payload references are modified.
class [[nodiscard]] IndexingVerdicts {
public:
	IndexingVerdicts(const ns_indexes::IndexesStorage&, const ConstPayload&);
	bool HasCondition(int idx) const noexcept;
	MatchesCondition operator[](int idx) const noexcept;
	std::span<const int> ConditionalIndexes() const noexcept { return positions_; }

private:
	std::span<const int> positions_;
	h_vector<MatchesCondition, 4> matches_;
};

void ApplyIndexingConditionToIndex(Index& index, const IndexDef& indexDef, ns_indexes::TargetState& st);

}  // namespace reindexer
