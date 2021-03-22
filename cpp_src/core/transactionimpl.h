#pragma once
#include "core/itemimpl.h"
#include "payload/fieldsset.h"
#include "transaction.h"

namespace reindexer {

class TransactionStep {
public:
	TransactionStep(Item &&item, ItemModifyMode modifyMode, lsn_t lsn)
		: itemData_(move(*item.impl_)), modifyMode_(modifyMode), query_(nullptr), lsn_(lsn) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(lsn_t lsn) : modifyMode_(ModeUpdate), query_(nullptr), lsn_(lsn) {}
	TransactionStep(Query &&query, lsn_t lsn) : modifyMode_(ModeUpdate), query_(new Query(std::move(query))), lsn_(lsn) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) noexcept = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = default;

	ItemImplRawData itemData_;
	ItemModifyMode modifyMode_;
	std::unique_ptr<Query> query_;
	const lsn_t lsn_;
};

class TransactionImpl {
public:
	TransactionImpl(const std::string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
					std::shared_ptr<const Schema> schema, lsn_t lsn);

	void Insert(Item &&item, lsn_t lsn);
	void Update(Item &&item, lsn_t lsn);
	void Upsert(Item &&item, lsn_t lsn);
	void Delete(Item &&item, lsn_t lsn);
	void Modify(Item &&item, ItemModifyMode mode, lsn_t lsn);
	void Modify(Query &&item, lsn_t lsn);
	void Nop(lsn_t lsn);

	void UpdateTagsMatcherFromItem(ItemImpl *ritem);
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	lsn_t GetLSN() const noexcept { return lsn_; }

	// const std::string &GetName() { return nsName_; }

	void checkTagsMatcher(Item &item);

	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;
	std::shared_ptr<const Schema> schema_;

	std::vector<TransactionStep> steps_;
	std::string nsName_;
	bool tagsUpdated_;
	std::mutex mtx_;
	Transaction::time_point startTime_;
	const lsn_t lsn_;
};

}  // namespace reindexer
