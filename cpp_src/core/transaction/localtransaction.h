#pragma once

#include "sharedtransactiondata.h"
#include "tools/errors.h"
#include "transactionsteps.h"

namespace reindexer {

class LocalTransaction {
public:
	LocalTransaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf,
					 std::shared_ptr<const Schema> schema, lsn_t lsn)
		: data_(std::make_unique<SharedTransactionData>(nsName, lsn, Transaction::ClockT::now(), pt, tm, pf, std::move(schema))),
		  tx_(std::make_unique<TransactionSteps>()) {}
	LocalTransaction(Error err) : err_(std::move(err)) {}

	Item GetItem(TransactionStep &&st) {
		assertrx(tx_ && data_);
		auto &data = std::get<TransactionItemStep>(st.data_);
		auto item = Item(
			new ItemImpl(data_->GetPayloadType(), data_->GetTagsMatcher(), data_->GetPKFileds(), data_->GetSchema(), std::move(data.data)));
		data.hadTmUpdate ? item.impl_->tagsMatcher().setUpdated() : item.impl_->tagsMatcher().clearUpdated();
		return item;
	}
	std::vector<TransactionStep> &GetSteps() noexcept {
		assertrx(tx_);
		return tx_->steps_;
	}
	const std::vector<TransactionStep> &GetSteps() const noexcept {
		assertrx(tx_);
		return tx_->steps_;
	}
	Transaction::TimepointT GetStartTime() const noexcept {
		assertrx(data_);
		return data_->startTime;
	}
	lsn_t GetLSN() const noexcept {
		assertrx(data_);
		return data_->lsn;
	}
	const std::string &GetNsName() const noexcept {
		assertrx(data_);
		return data_->nsName;
	}
	Error Status() const noexcept { return err_; }

private:
	LocalTransaction(std::unique_ptr<SharedTransactionData> &&d, std::unique_ptr<TransactionSteps> &&tx, Error &&e)
		: data_(std::move(d)), tx_(std::move(tx)), err_(std::move(e)) {}

	std::unique_ptr<SharedTransactionData> data_;
	std::unique_ptr<TransactionSteps> tx_;
	Error err_;

	friend class TransactionImpl;
};

}  // namespace reindexer
