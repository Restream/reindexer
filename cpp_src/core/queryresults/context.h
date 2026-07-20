#pragma once

#include "core/cjson/tagsmatcher.h"
#include "fields_filter.h"

namespace reindexer {

class Schema;

struct [[nodiscard]] QueryResultsContext {
	QueryResultsContext() = default;
	QueryResultsContext(PayloadType type, TagsMatcher tagsMatcher, FieldsFilter fieldsFilter, std::shared_ptr<const Schema> schema,
						lsn_t nsIncarnationTag)
		: type_(std::move(type)),
		  tagsMatcher_(std::move(tagsMatcher)),
		  fieldsFilter_(std::move(fieldsFilter)),
		  schema_(std::move(schema)),
		  nsIncarnationTag_(std::move(nsIncarnationTag)) {}

	PayloadType type_;
	TagsMatcher tagsMatcher_;
	FieldsFilter fieldsFilter_;
	std::shared_ptr<const Schema> schema_;
	lsn_t nsIncarnationTag_;
};

}  // namespace reindexer
