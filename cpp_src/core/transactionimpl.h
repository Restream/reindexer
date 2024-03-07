#pragma once
#include "core/itemimpl.h"
#include "payload/fieldsset.h"
#include "transaction.h"

namespace reindexer {

struct TransactionItemStep {
	ItemModifyMode mode;
	bool hadTmUpdate;
	ItemImplRawData data;
};

struct TransactionQueryStep {
	std::unique_ptr<Query> query;
};

struct TransactionMetaStep {
	std::string key;
	std::string value;
};

struct TransactionTmStep {
	TagsMatcher tm;
};

class TransactionStep {
public:
	enum class Type : uint8_t { Nop, ModifyItem, Query, PutMeta, SetTM };

	TransactionStep(Item &&item, ItemModifyMode modifyMode)
		: data_(TransactionItemStep{modifyMode, item.IsTagsUpdated(), std::move(*item.impl_)}), type_(Type::ModifyItem) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(TagsMatcher &&tm) : data_(TransactionTmStep{std::move(tm)}), type_(Type::SetTM) {}
	TransactionStep(Query &&query) : data_(TransactionQueryStep{std::make_unique<Query>(std::move(query))}), type_(Type::Query) {}
	TransactionStep(std::string_view key, std::string_view value)
		: data_(TransactionMetaStep{std::string(key), std::string(value)}), type_(Type::PutMeta) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = delete;

	std::variant<TransactionItemStep, TransactionQueryStep, TransactionMetaStep, TransactionTmStep> data_;
	Type type_;
};

class TransactionImpl {
public:
	TransactionImpl(const std::string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
					std::shared_ptr<const Schema> schema)
		: payloadType_(pt),
		  tagsMatcher_(tm),
		  pkFields_(pf),
		  schema_(std::move(schema)),
		  nsName_(nsName),
		  tagsUpdated_(false),
		  hasDeleteItemSteps_(false),
		  startTime_(system_clock_w::now()) {
		tagsMatcher_.clearUpdated();
	}

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&item);
	void PutMeta(std::string_view key, std::string_view value);
	void SetTagsMatcher(TagsMatcher &&tm);

	Item NewItem();
	Item GetItem(TransactionStep &&st);
	void ValidatePK(const FieldsSet &pkFields);

	const std::string &GetName() { return nsName_; }

	void checkTagsMatcher(Item &item);

	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;
	std::shared_ptr<const Schema> schema_;

	std::vector<TransactionStep> steps_;
	std::string nsName_;
	bool tagsUpdated_;
	bool hasDeleteItemSteps_;
	std::mutex mtx_;
	Transaction::time_point startTime_;
};

}  // namespace reindexer
