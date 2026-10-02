#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"
#include "core/queryresults/fields_filter.h"
#include "estl/h_vector.h"
#include "iterators.h"

namespace reindexer {

class FieldsFilter;
class PayloadType;
class Schema;
class TagsMatcher;

namespace client {
class QueryResults;
}

namespace joins {

class Results;

/// Joined item context.
struct [[nodiscard]] JoinedItemContext {
	JoinedItemContext(ItemIterator&& it, const joins::Results* res) : iterator(std::move(it)), results(res) {}
	JoinedItemContext(ItemIterator&& it, const joins::Results* res, const client::QueryResults& qr);
	JoinedItemContext(ItemIterator&& it, const joins::Results* res, const LocalQueryResults& qr);

	struct [[nodiscard]] NamespaceContext {
		NamespaceContext(PayloadType pt_, TagsMatcher tm_, FieldsFilter filter_, std::shared_ptr<const Schema> schema_)
			: pt{std::move(pt_)}, tm{std::move(tm_)}, filter{std::move(filter_)}, schema{std::move(schema_)} {}
		PayloadType pt;
		TagsMatcher tm;
		FieldsFilter filter;
		std::shared_ptr<const Schema> schema;
	};

	ItemIterator iterator;
	const joins::Results* results;
	h_vector<NamespaceContext, 1> contexts;
};

}  // namespace joins
}  // namespace reindexer
