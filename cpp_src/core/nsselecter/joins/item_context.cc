#include "item_context.h"
#include "core/cjson/tagsmatcher.h"
#include "core/queryresults/queryresults.h"

namespace reindexer::joins {

JoinedItemContext::JoinedItemContext(ItemIterator&& it, const joins::Results* res, const client::QueryResults& qr)
	: JoinedItemContext(std::move(it), res) {
	contexts.reserve(qr.GetNamespacesCount());
	for (size_t nsid = 0; nsid < qr.GetNamespacesCount(); ++nsid) {
		contexts.emplace_back(qr.GetPayloadType(nsid), qr.GetTagsMatcher(nsid), FieldsFilter{}, std::shared_ptr<const Schema>{});
	}
}

JoinedItemContext::JoinedItemContext(ItemIterator&& it, const joins::Results* res, const LocalQueryResults& qr)
	: JoinedItemContext(std::move(it), res) {
	contexts.reserve(qr.getMergedNSCount());
	for (int nsid = 0; nsid < qr.getMergedNSCount(); ++nsid) {
		contexts.emplace_back(qr.getPayloadType(nsid), qr.getTagsMatcher(nsid), qr.getFieldsFilter(nsid), qr.getSchema(nsid));
	}
}

}  // namespace reindexer::joins
