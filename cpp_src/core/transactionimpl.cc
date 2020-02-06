#include "transactionimpl.h"
#include "cjson/jsonbuilder.h"
#include "item.h"
#include "itemimpl.h"
#include "namespace.h"

namespace reindexer {

using std::string;

void TransactionImpl::checkTagsMatcher(Item &item) {
	if (item.IsTagsUpdated()) {
		ItemImpl *ritem = item.impl_;
		UpdateTagsMatcherFromItem(ritem);
		tagsUpdated_ = true;
	}
}

Item TransactionImpl::NewItem() { return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_)); }
Item TransactionImpl::GetItem(TransactionStep &&st) {
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_, std::move(st.itemData_)));
}

TransactionImpl::TransactionImpl(const string &nsName, const PayloadType &pt, const TagsMatcher &tm, const FieldsSet &pf)
	: payloadType_(pt), tagsMatcher_(tm), pkFields_(pf), nsName_(nsName), tagsUpdated_(false) {}

void TransactionImpl::UpdateTagsMatcherFromItem(ItemImpl *ritem) {
	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		string jsonSliceBuf(ritem->GetJSON());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) throw err;

		if (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
	if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void TransactionImpl::Insert(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeInsert});
}
void TransactionImpl::Update(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpdate});
}
void TransactionImpl::Upsert(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeUpsert});
}
void TransactionImpl::Delete(Item &&item) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), ModeDelete});
}
void TransactionImpl::Modify(Item &&item, ItemModifyMode mode) {
	checkTagsMatcher(item);
	steps_.emplace_back(TransactionStep{move(item), mode});
}

void TransactionImpl::Modify(Query &&query) { steps_.emplace_back(TransactionStep(std::move(query))); }

Transaction::Serializer::Serializer(const Transaction &tx)
	: steps_(tx.GetSteps()), currentStep_(0), writtenStepsCount_(0), finalized_(false) {
	tx.impl_->tagsMatcher_.serialize(ser_);
	stepsCountPos_ = ser_.Len();
	ser_.PutUInt32(0);
}

void Transaction::Serializer::SerializeNextStep() {
	assert(!finalized_);
	assert(currentStep_ < steps_.size());
	const auto sliceHelper = ser_.StartSlice();
	steps_[currentStep_++].Serialize(ser_);
	++writtenStepsCount_;
}

enum { ItemTransactionStep, QueryTransactionStep };

static void serializeItemStep(WrSerializer &ser, ItemImpl &item, ItemModifyMode modifyMode) {
	ser.PutVarUint(ItemTransactionStep);
	ser.PutVarUint(modifyMode);
	item.GetCJSON(ser);
}

void Transaction::Serializer::Serialize(ItemImpl &item, ItemModifyMode modifyMode) {
	assert(!finalized_);
	const auto sliceHelper = ser_.StartSlice();
	serializeItemStep(ser_, item, modifyMode);
	++writtenStepsCount_;
}

string_view Transaction::Serializer::Slice() {
	if (!finalized_) {
		assert(currentStep_ == steps_.size());
		memcpy(&ser_.Buf()[stepsCountPos_], &writtenStepsCount_, sizeof(writtenStepsCount_));
		finalized_ = true;
	}
	return ser_.Slice();
}

Error TransactionImpl::Deserialize(string_view data, int64_t lsn) try {
	Serializer ser(data);
	tagsMatcher_.deserialize(ser);
	steps_.clear();
	const uint32_t stepsCount = ser.GetUInt32();
	steps_.reserve(stepsCount);
	for (size_t i = 0; i < stepsCount; ++i) {
		steps_.push_back(TransactionStep::Deserialize(ser.GetSlice(), lsn, payloadType_, tagsMatcher_, pkFields_));
	}
	return {};
} catch (const Error &err) {
	return err;
}

void TransactionImpl::ConvertCJSONtoJSON(string_view cjson, JsonBuilder &jb, std::function<string(string_view)> cjsonViewer) {
	Serializer ser(cjson);
	TagsMatcher tagsMatcher;
	tagsMatcher.deserialize(ser);
	const uint32_t stepsCount = ser.GetUInt32();
	auto stepsArrayBuilder = jb.Array("steps");
	for (size_t i = 0; i < stepsCount; ++i) {
		auto stepBuilder = stepsArrayBuilder.Object();
		TransactionStep::ConvertCJSONtoJSON(ser.GetSlice(), stepBuilder, cjsonViewer);
	}
}

void TransactionStep::Serialize(WrSerializer &ser) const {
	if (query_) {
		ser.PutVarUint(QueryTransactionStep);
		const auto sliceHelper = ser.StartSlice();
		query_->GetSQL(ser);
	} else {
		// serializeItemStep(ser, *item_.impl_, modifyMode_);
	}
}

TransactionStep TransactionStep::Deserialize(string_view data, int64_t lsn, const PayloadType &pt, const TagsMatcher &tm,
											 const FieldsSet &fs) {
	Serializer ser(data);
	if (ser.GetVarUint() == QueryTransactionStep) {
		Query query;
		query.FromSQL(ser.GetSlice());
		return {std::move(query)};
	} else {
		const ItemModifyMode modifyMode = static_cast<ItemModifyMode>(ser.GetVarUint());
		std::unique_ptr<ItemImpl> itemImpl{new ItemImpl{pt, tm, fs}};
		itemImpl->tagsMatcher() = tm;
		Item item{itemImpl.release()};
		item.setLSN(lsn);
		const Error err = item.FromCJSON(data.substr(ser.Pos()));
		if (!err.ok()) throw err;
		return {std::move(item), modifyMode};
	}
}

static const char *toStr(ItemModifyMode mode) {
	switch (mode) {
		case ModeUpdate:
			return "Update";
		case ModeInsert:
			return "Insert";
		case ModeUpsert:
			return "Upsert";
		case ModeDelete:
			return "Delete";
		default:
			return "<Unknown>";
	}
}

void TransactionStep::ConvertCJSONtoJSON(string_view cjson, JsonBuilder &jb, std::function<string(string_view)> cjsonViewer) {
	Serializer ser(cjson);
	if (ser.GetVarUint() == QueryTransactionStep) {
		jb.Put("query", ser.GetSlice());
	} else {
		const ItemModifyMode modifyMode = static_cast<ItemModifyMode>(ser.GetVarUint());
		jb.Put("mode", toStr(modifyMode));
		jb.Raw("item", cjsonViewer(cjson.substr(ser.Pos())));
	}
}

}  // namespace reindexer
