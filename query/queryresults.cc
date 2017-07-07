#include "query/queryresults.h"
#include "tools/logger.h"

namespace reindexer {
void QueryResults::Dump() const {
	string buf;
	for (auto &r : *this) {
		if (&r != &*(*this).begin()) buf += ",";
		buf += to_string(r.id);
		auto it = joined_.find(r.id);
		if (it != joined_.end()) {
			buf += "[";
			for (auto &ra : it->second) {
				if (&ra != &*it->second.begin()) buf += ";";
				for (auto &rr : ra) {
					if (&rr != &*ra.begin()) buf += ",";
					buf += to_string(rr.id);
				}
			}
			buf += "]";
		}
	}
	logPrintf(LogInfo, "Query returned: %s", buf.c_str());
}

void QueryResults::GetJSON(int idx, WrSerializer &ser) const {
	KeyRefs kref;
	ConstPayload pl(*type_, &at(idx).data);
	pl.Get(0, kref);

	// reserve place for size
	int saveLen = ser.Len();
	ser.PutInt(0);

	p_string tuple = (p_string)kref[0];
	Serializer rdser(tuple.data(), tuple.size());
	int fieldsoutcnt[maxIndexes];
	for (int i = 0; i < pl.NumFields(); ++i) fieldsoutcnt[i] = 0;
	ItemImpl::MergeJson(&pl, tagsMatcher_, ser, rdser, true, fieldsoutcnt);

	// put real json size
	int realSize = ser.Len() - saveLen - sizeof(int);
	memcpy(ser.Buf() + saveLen, &realSize, sizeof(int));
}

}  // namespace reindexer