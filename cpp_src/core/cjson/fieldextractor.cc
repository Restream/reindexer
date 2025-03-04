#include "fieldextractor.h"
#include "core/queryresults/fields_filter.h"

namespace reindexer {

const IndexedPathNode& FieldsExtractor::getArrayPathNode() const {
	if (filter_) {
		const auto* regularFields = filter_->TryRegularFields();
		if (regularFields && regularFields->getTagsPathsLength() > 0) {
			size_t lastItemIndex = regularFields->getTagsPathsLength() - 1;
			if (regularFields->isTagsPathIndexed(lastItemIndex)) {
				const IndexedTagsPath& path = regularFields->getIndexedTagsPath(lastItemIndex);
				assertrx(path.size() > 0);
				if (path.back().IsArrayNode()) {
					return path.back();
				}
			}
		}
	}
	static const IndexedPathNode commonNode{IndexedPathNode::AllItems};
	return commonNode;
}

}  // namespace reindexer
