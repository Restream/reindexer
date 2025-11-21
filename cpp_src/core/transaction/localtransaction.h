#pragma once

#include "sharedtransactiondata.h"
#include "tools/errors.h"
#include "transactionsteps.h"

namespace reindexer {

class [[nodiscard]] LocalTransaction {
public:
	LocalTransaction(NamespaceName nsName, const PayloadType& pt, const TagsMatcher& tm, const FieldsSet& pf,
					 std::shared_ptr<const Schema> schema, lsn_t lsn)
		: data_(std::make_unique<SharedTransactionData>(std::move(nsName), lsn, Transaction::ClockT::now(), pt, tm, pf, std::move(schema))),
		  tx_(std::make_unique<TransactionSteps>()) {}
	LocalTransaction(Error err) : err_(std::move(err)) {}

	Item GetItem(TransactionStep&& st) {
		assertrx(tx_ && data_);
		auto& data = std::get<TransactionItemStep>(st.data_);
		auto item = Item(
			new ItemImpl(data_->GetPayloadType(), data_->GetTagsMatcher(), data_->GetPKFileds(), data_->GetSchema(), std::move(data.data)));
		data.hadTmUpdate ? item.impl_->tagsMatcher().setUpdated() : item.impl_->tagsMatcher().clearUpdated();
		return item;
	}
	std::vector<TransactionStep>& GetSteps() noexcept {
		assertrx(tx_);
		return tx_->steps;
	}
	const std::vector<TransactionStep>& GetSteps() const noexcept {
		assertrx(tx_);
		return tx_->steps;
	}
	Transaction::TimepointT GetStartTime() const noexcept {
		assertrx(data_);
		return data_->startTime;
	}
	lsn_t GetLSN() const noexcept {
		assertrx(data_);
		return data_->lsn;
	}
	std::string_view GetNsName() const noexcept {
		assertrx(data_);
		return data_->nsName;
	}
	Error Status() const noexcept { return err_; }
	void ValidatePK(const FieldsSet& pkFields) {
		assertrx(data_);
		if (tx_ && tx_->HasDeleteItemSteps() && pkFields != data_->GetPKFileds()) [[unlikely]] {
			throw Error(errNotValid,
						"Transaction has Delete-calls and it's PK metadata is outdated (probably PK has been changed during the "
						"transaction creation)");
		}
	}
	size_t CalculateNewCapacity(size_t currentSize) const noexcept { return tx_ ? tx_->CalculateNewCapacity(currentSize) : currentSize; }
	unsigned DeletionsCount() const noexcept { return tx_ ? tx_->DeletionsCount() : 0; }
	unsigned ExpectedInsertionsCount() const noexcept { return tx_ ? tx_->ExpectedInsertionsCount() : 0; }
	unsigned UpdateQueriesCount() const noexcept { return tx_ ? tx_->UpdateQueriesCount() : 0; }
	unsigned DeleteQueriesCount() const noexcept { return tx_ ? tx_->DeleteQueriesCount() : 0; }

private:
	LocalTransaction(std::unique_ptr<SharedTransactionData>&& d, std::unique_ptr<TransactionSteps>&& tx, Error&& e)
		: data_(std::move(d)), tx_(std::move(tx)), err_(std::move(e)) {}

	std::unique_ptr<SharedTransactionData> data_;
	std::unique_ptr<TransactionSteps> tx_;
	Error err_;

	friend class TransactionImpl;
};

}  // namespace reindexer
