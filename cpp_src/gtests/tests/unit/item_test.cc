#include "item_move_semantics_api.h"

using reindexer::Item;
using reindexer::Slice;
using std::unique_ptr;
using std::vector;

TEST_F(ItemMoveSemanticsApi, MoveSemanticsOperator) {
	prepareItems();
	verifyAndUpsertItems();
	verifyJsonsOfUpsertedItems();
}
