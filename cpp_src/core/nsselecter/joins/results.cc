#include "results.h"

#include "core/id_type.h"
#include "core/queryresults/localqueryresults.h"

namespace reindexer::joins {

void NamespaceResults::Insert(IdType rowid, int joinedNsId, uint16_t fieldIdx, LocalQueryResults&& qr) {
	assertrx_throw(fieldIdx < joinedFieldsCount_);
	ItemsOffsets& offsets = offsets_[rowid];
	if (offsets.size() <= size_t(joinedFieldsCount_)) {
		offsets.resize(joinedFieldsCount_);
	}
	for (auto& it : qr) {
		it.GetItemRef().SetNsid(static_cast<uint16_t>(joinedNsId));
	}
	offsets[fieldIdx] = ItemOffset(fieldIdx, items_.Size(), qr.Count());
	items_.Insert(items_.end(), std::move(qr.Items()).mbegin(), std::move(qr.Items()).mend());
}

}  // namespace reindexer::joins
