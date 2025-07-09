#include "sharedtransactiondata.h"
#include "core/item.h"
#include "core/itemimpl.h"

namespace reindexer {

void SharedTransactionData::UpdateTagsMatcherIfNecessary(ItemImpl& ritem) {
	if (!ritem.tagsMatcher().isUpdated()) {
		return;
	}
	if (ritem.Type().get() != payloadType_.get() || (!tagsMatcher_.try_merge(ritem.tagsMatcher()))) {
		std::string jsonSliceBuf(ritem.GetJSON());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem.Value().GetLSN());
		ritem = std::move(tmpItem);

		auto err = ritem.FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) {
			throw err;
		}

		if (ritem.tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem.tagsMatcher())) {
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		}
		ritem.tagsMatcher() = tagsMatcher_;
		ritem.tagsMatcher().setUpdated();
	} else {
		ritem.tagsMatcher() = tagsMatcher_;
		ritem.tagsMatcher().setUpdated();
	}
	tagsUpdated_ = true;
}

void SharedTransactionData::SetTagsMatcher(TagsMatcher&& tm) {
	if (tm.stateToken() != tagsMatcher_.stateToken()) {
		throw Error(errParams, "Tx tm statetoken missmatch: {:#08x} vs {:#08x}", tagsMatcher_.stateToken(), tm.stateToken());
	}
	tagsMatcher_ = std::move(tm);
	tagsMatcher_.UpdatePayloadType(payloadType_, sparseIndexes_, NeedChangeTmVersion::No);
	tagsUpdated_ = true;
}

}  // namespace reindexer
