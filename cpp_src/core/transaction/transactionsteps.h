#pragma once

#include "core/item.h"
#include "core/itemimpl.h"
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

struct TransactionNopStep {};

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

	TransactionStep(Item &&item, ItemModifyMode modifyMode, lsn_t lsn)
		: data_(TransactionItemStep{modifyMode, item.IsTagsUpdated(), std::move(*item.impl_)}), type_(Type::ModifyItem), lsn_(lsn) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(lsn_t lsn) : data_(TransactionNopStep{}), type_(Type::Nop), lsn_(lsn) {}
	TransactionStep(TagsMatcher tm, lsn_t lsn) : data_(TransactionTmStep{std::move(tm)}), type_(Type::SetTM), lsn_(lsn) {}
	TransactionStep(Query &&query, lsn_t lsn)
		: data_(TransactionQueryStep{std::make_unique<Query>(std::move(query))}), type_(Type::Query), lsn_(lsn) {}
	TransactionStep(std::string_view key, std::string_view value, lsn_t lsn)
		: data_(TransactionMetaStep{std::string(key), std::string(value)}), type_(Type::PutMeta), lsn_(lsn) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = delete;

	std::variant<TransactionItemStep, TransactionQueryStep, TransactionNopStep, TransactionMetaStep, TransactionTmStep> data_;
	Type type_;
	const lsn_t lsn_;
};

class TransactionSteps {
public:
	void Insert(Item &&item, lsn_t lsn) { steps_.emplace_back(TransactionStep{move(item), ModeInsert, lsn}); }
	void Update(Item &&item, lsn_t lsn) { steps_.emplace_back(TransactionStep{move(item), ModeUpdate, lsn}); }
	void Upsert(Item &&item, lsn_t lsn) { steps_.emplace_back(TransactionStep{move(item), ModeUpsert, lsn}); }
	void Delete(Item &&item, lsn_t lsn) { steps_.emplace_back(TransactionStep{move(item), ModeDelete, lsn}); }
	void Modify(Item &&item, ItemModifyMode mode, lsn_t lsn) { steps_.emplace_back(TransactionStep{move(item), mode, lsn}); }
	void Modify(Query &&query, lsn_t lsn) { steps_.emplace_back(TransactionStep(std::move(query), lsn)); }
	void Nop(lsn_t lsn) { steps_.emplace_back(TransactionStep{lsn}); }
	void PutMeta(std::string_view key, std::string_view value, lsn_t lsn) { steps_.emplace_back(TransactionStep{key, value, lsn}); }
	void SetTagsMatcher(TagsMatcher &&tm, lsn_t lsn) { steps_.emplace_back(TransactionStep{tm, lsn}); }

	std::vector<TransactionStep> steps_;
};

}  // namespace reindexer
