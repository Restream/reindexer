#pragma once
#include "core/itemimpl.h"
#include "payload/fieldsset.h"
#include "transaction.h"

namespace reindexer {

struct TransactionItemStep {
	ItemImplRawData data;
	ItemModifyMode mode;
};

struct TransactionQueryStep {
	std::unique_ptr<Query> query;
};

struct TransactionNopStep {};

struct TransactionMetaStep {
	std::string key;
	std::string value;
};

class TransactionStep {
public:
	enum class Type : uint8_t { Nop, ModifyItem, Query, PutMeta };

	TransactionStep(Item &&item, ItemModifyMode modifyMode, lsn_t lsn)
		: data_(TransactionItemStep{std::move(*item.impl_), modifyMode}), type_(Type::ModifyItem), lsn_(lsn) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(lsn_t lsn) : data_(TransactionNopStep{}), type_(Type::Nop), lsn_(lsn) {}
	TransactionStep(Query &&query, lsn_t lsn)
		: data_(TransactionQueryStep{std::make_unique<Query>(std::move(query))}), type_(Type::Query), lsn_(lsn) {}
	TransactionStep(std::string_view key, std::string_view value, lsn_t lsn)
		: data_(TransactionMetaStep{std::string(key), std::string(value)}), type_(Type::PutMeta), lsn_(lsn) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = delete;

	std::variant<TransactionItemStep, TransactionQueryStep, TransactionNopStep, TransactionMetaStep> data_;
	Type type_;
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
	void PutMeta(std::string_view key, std::string_view value, lsn_t lsn);

	void UpdateTagsMatcherFromItem(ItemImpl *ritem);
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	lsn_t GetLSN() const noexcept { return lsn_; }

	// const std::string &GetName() { return nsName_; }

	void updateTagsMatcherIfNecessary(Item &item);

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
