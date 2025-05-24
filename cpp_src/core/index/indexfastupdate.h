#include "core/type_consts.h"
#include "estl/fast_hash_set.h"

namespace reindexer {

class NamespaceImpl;
class IndexDef;

struct IndexFastUpdate {
	static bool Try(NamespaceImpl& ns, const IndexDef& from, const IndexDef& to);
	static bool RelaxedEqual(const IndexDef& from, const IndexDef& to);

private:
	static bool isLegalTypeTransform(IndexType from, IndexType to) noexcept;
	static const std::vector<fast_hash_set<IndexType>> kTransforms;
};

}  // namespace reindexer
