#pragma once

#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

class TransactionImpl;
class TransactionStep;
class PayloadType;
class TagsMatcher;
class FieldsSet;

class Transaction {
public:
	using time_point = system_clock_w::time_point;

	Transaction(const std::string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
				std::shared_ptr<const Schema> schema);
	Transaction(const Error &err);
	~Transaction();
	Transaction() = default;
	Transaction(Transaction &&) noexcept;
	Transaction &operator=(Transaction &&) noexcept;

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&query);
	void PutMeta(std::string_view key, std::string_view value);
	void MergeTagsMatcher(TagsMatcher &&tm);
	bool IsFree() { return impl_ == nullptr; }
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	Error Status() { return status_; }

	const std::string &GetName() const;

	friend class ReindexerImpl;

	std::vector<TransactionStep> &GetSteps();
	const std::vector<TransactionStep> &GetSteps() const;
	bool IsTagsUpdated() const;
	time_point GetStartTime() const;
	void ValidatePK(const FieldsSet &pkFields);

protected:
	std::unique_ptr<TransactionImpl> impl_;
	Error status_;
};

}  // namespace reindexer
