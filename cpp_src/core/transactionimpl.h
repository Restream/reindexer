#pragma once
#include "core/itemimpl.h"
#include "payload/fieldsset.h"
#include "transaction.h"

namespace reindexer {

class TransactionStep {
public:
	TransactionStep(Item &&item, ItemModifyMode modifyMode) : itemData_(move(*item.impl_)), modifyMode_(modifyMode), query_(nullptr) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(Query &&query) : modifyMode_(ModeUpdate), query_(new Query(std::move(query))) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) noexcept = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = default;

	ItemImplRawData itemData_;
	ItemModifyMode modifyMode_;
	std::unique_ptr<Query> query_;
};

class TransactionImpl {
public:
	TransactionImpl(const std::string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf);

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&item);

	void UpdateTagsMatcherFromItem(ItemImpl *ritem);
	Item NewItem();
	Item GetItem(TransactionStep &&st);

	const std::string &GetName() { return nsName_; }

	void checkTagsMatcher(Item &item);

	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;

	std::vector<TransactionStep> steps_;
	std::string nsName_;
	bool tagsUpdated_;
	std::mutex mtx_;
	Transaction::time_point startTime_;
};

}  // namespace reindexer
