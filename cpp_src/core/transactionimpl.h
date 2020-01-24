#pragma once
#include "payload/fieldsset.h"
#include "transaction.h"

namespace reindexer {

class TransactionStep {
public:
	TransactionStep(Item &&item, ItemModifyMode modifyMode) : item_(move(item)), modifyMode_(modifyMode), query_(nullptr) {}
	TransactionStep(Query &&query) : modifyMode_(ModeUpdate), query_(new Query(std::move(query))) {}

	TransactionStep(const TransactionStep &) = delete;
	TransactionStep &operator=(const TransactionStep &) = delete;
	TransactionStep(TransactionStep && /*rhs*/) noexcept = default;
	TransactionStep &operator=(TransactionStep && /*rhs*/) = default;
	void Serialize(WrSerializer &) const;
	static TransactionStep Deserialize(string_view, int64_t lsn, const PayloadType &, const TagsMatcher &, const FieldsSet &);
	static void ConvertCJSONtoJSON(string_view cjson, JsonBuilder &, std::function<string(string_view)> cjsonViewer);

	Item item_;
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

	const std::string &GetName() { return nsName_; }
	Error Deserialize(string_view, int64_t lsn);
	static void ConvertCJSONtoJSON(string_view cjson, JsonBuilder &, std::function<string(string_view)> cjsonViewer);

	void checkTagsMatcher(Item &item);

	PayloadType payloadType_;
	TagsMatcher tagsMatcher_;
	FieldsSet pkFields_;

	std::vector<TransactionStep> steps_;
	std::string nsName_;
};

}  // namespace reindexer
