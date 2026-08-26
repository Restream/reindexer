#include "item_context.h"
#include "core/cjson/tagsmatcher.h"
#include "core/queryresults/queryresults.h"

namespace reindexer::joins {

JoinedItemContext::JoinedItemContext(ItemIterator&& it, const joins::Results* res, const client::QueryResults& qr)
	: JoinedItemContext(std::move(it), res) {
	contexts.reserve(qr.GetNamespacesCount());
	for (size_t nsid = 0; nsid < qr.GetNamespacesCount(); ++nsid) {
		contexts.emplace_back(qr.GetPayloadType(int(nsid)), qr.GetTagsMatcher(int(nsid)), FieldsFilter{}, std::shared_ptr<const Schema>{});
	}
}

JoinedItemContext::JoinedItemContext(ItemIterator&& it, const joins::Results* res, const LocalQueryResults& qr)
	: JoinedItemContext(std::move(it), res) {
	contexts.reserve(qr.getNamespacesCount());
	for (size_t nsid = 0; nsid < qr.getNamespacesCount(); ++nsid) {
		contexts.emplace_back(qr.getPayloadType(int(nsid)), qr.getTagsMatcher(int(nsid)), qr.getFieldsFilter(int(nsid)),
							  qr.getSchema(int(nsid)));
	}
}

}  // namespace reindexer::joins
