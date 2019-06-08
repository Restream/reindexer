#include "item_move_semantics_api.h"

TEST_F(ItemMoveSemanticsApi, MoveSemanticsOperator) {
	prepareItems();
	verifyAndUpsertItems();
	verifyJsonsOfUpsertedItems();
}
