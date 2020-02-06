#pragma once
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

class TransactionImpl;
class TransactionStep;
class PayloadType;
class TagsMatcher;
class FieldsSet;
class Namespace;

class Transaction {
public:
	class Serializer {
	public:
		Serializer(const Transaction &);
		void SerializeNextStep();
		void Serialize(ItemImpl &, ItemModifyMode);
		void IgnoreNextStep() {
			assert(!finalized_);
			++currentStep_;
		}
		string_view Slice();

		Serializer(const Serializer &) = delete;
		Serializer &operator=(const Serializer &) = delete;

	private:
		WrSerializer ser_;
		size_t stepsCountPos_;
		const vector<TransactionStep> &steps_;
		size_t currentStep_ = 0;
		uint32_t writtenStepsCount_ = 0;
		bool finalized_ = false;
	};
	Transaction(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf);
	Transaction(const Error &err);
	~Transaction();
	Transaction() = default;
	Transaction(Transaction &&);
	Transaction &operator=(Transaction &&);

	void Insert(Item &&item);
	void Update(Item &&item);
	void Upsert(Item &&item);
	void Delete(Item &&item);
	void Modify(Item &&item, ItemModifyMode mode);
	void Modify(Query &&query);
	bool IsFree() { return impl_ == nullptr; }
	Item NewItem();
	Item GetItem(TransactionStep &&st);
	Error Status() { return status_; }
	Error Deserialize(string_view, int64_t lsn);

	const std::string &GetName();

	friend class ReindexerImpl;

	vector<TransactionStep> &GetSteps();
	const vector<TransactionStep> &GetSteps() const;
	bool IsTagsUpdated() const;

protected:
	std::unique_ptr<TransactionImpl> impl_;
	Error status_;
};

}  // namespace reindexer
