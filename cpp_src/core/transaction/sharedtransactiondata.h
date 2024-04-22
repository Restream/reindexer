#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/payload/fieldsset.h"
#include "core/schema.h"
#include "transaction.h"

namespace reindexer {

class ItemImpl;

class SharedTransactionData {
public:
	SharedTransactionData(std::string _nsName, lsn_t _lsn, Transaction::ClockT::time_point _startTime, const PayloadType &pt,
						  const TagsMatcher &tm, const FieldsSet &pf, std::shared_ptr<const Schema> schema)
		: nsName(std::move(_nsName)),
		  lsn(_lsn),
		  startTime(_startTime),
		  payloadType_(pt),
		  tagsMatcher_(tm),
		  pkFields_(pf),
		  schema_(std::move(schema)) {}
	void UpdateTagsMatcherIfNecessary(ItemImpl &item);
	void SetTagsMatcher(TagsMatcher &&tm);
	bool IsTagsUpdated() const noexcept { return tagsUpdated_; }
	const PayloadType &GetPayloadType() const noexcept { return payloadType_; }
	const TagsMatcher &GetTagsMatcher() const noexcept { return tagsMatcher_; }
	const FieldsSet &GetPKFileds() const noexcept { return pkFields_; }
	std::shared_ptr<const Schema> GetSchema() const noexcept { return schema_; }

	const std::string nsName;
	const lsn_t lsn;
	const Transaction::TimepointT startTime;

private:
	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;
	std::shared_ptr<const Schema> schema_;
	bool tagsUpdated_ = false;
};

}  // namespace reindexer
