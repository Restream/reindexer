#pragma once

#include "core/item.h"
#include "core/itemimpl.h"
#include "core/query/query.h"
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

	TransactionStep(Item&& item, ItemModifyMode modifyMode, lsn_t lsn)
		: data_(TransactionItemStep{modifyMode, item.IsTagsUpdated(), std::move(*item.impl_)}), type_(Type::ModifyItem), lsn_(lsn) {
		delete item.impl_;
		item.impl_ = nullptr;
	}
	TransactionStep(lsn_t lsn) : data_(TransactionNopStep{}), type_(Type::Nop), lsn_(lsn) {}
	TransactionStep(TagsMatcher&& tm, lsn_t lsn) : data_(TransactionTmStep{std::move(tm)}), type_(Type::SetTM), lsn_(lsn) {}
	TransactionStep(Query&& query, lsn_t lsn)
		: data_(TransactionQueryStep{std::make_unique<Query>(std::move(query))}), type_(Type::Query), lsn_(lsn) {}
	TransactionStep(std::string_view key, std::string_view value, lsn_t lsn)
		: data_(TransactionMetaStep{std::string(key), std::string(value)}), type_(Type::PutMeta), lsn_(lsn) {}

	TransactionStep(const TransactionStep&) = delete;
	TransactionStep& operator=(const TransactionStep&) = delete;
	TransactionStep(TransactionStep&& /*rhs*/) = default;
	TransactionStep& operator=(TransactionStep&& /*rhs*/) = delete;

	std::variant<TransactionItemStep, TransactionQueryStep, TransactionNopStep, TransactionMetaStep, TransactionTmStep> data_;
	Type type_;
	const lsn_t lsn_;
};

class TransactionSteps {
public:
	void Insert(Item&& item, lsn_t lsn) {
		++expectedInsertionsCount_;
		steps.emplace_back(std::move(item), ModeInsert, lsn);
	}
	void Update(Item&& item, lsn_t lsn) { steps.emplace_back(std::move(item), ModeUpdate, lsn); }
	void Upsert(Item&& item, lsn_t lsn) {
		++expectedInsertionsCount_;
		steps.emplace_back(std::move(item), ModeUpsert, lsn);
	}
	void Delete(Item&& item, lsn_t lsn) {
		++deletionsCount_;
		steps.emplace_back(std::move(item), ModeDelete, lsn);
	}
	void Modify(Item&& item, ItemModifyMode mode, lsn_t lsn);
	void Modify(Query&& query, lsn_t lsn);
	void Nop(lsn_t lsn) { steps.emplace_back(lsn); }
	void PutMeta(std::string_view key, std::string_view value, lsn_t lsn) { steps.emplace_back(key, value, lsn); }
	void SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) { steps.emplace_back(std::move(tm), lsn); }
	[[nodiscard]] size_t CalculateNewCapacity(size_t currentSize) const noexcept;
	[[nodiscard]] bool HasDeleteItemSteps() const noexcept { return deletionsCount_ > 0; }
	[[nodiscard]] unsigned DeletionsCount() const noexcept { return deletionsCount_; }
	[[nodiscard]] unsigned ExpectedInsertionsCount() const noexcept { return expectedInsertionsCount_; }
	[[nodiscard]] unsigned UpdateQueriesCount() const noexcept { return updateQueriesCount_; }

	std::vector<TransactionStep> steps;

private:
	unsigned updateQueriesCount_{0};
	unsigned deletionsCount_{0};
	unsigned expectedInsertionsCount_{0};
};

}  // namespace reindexer
