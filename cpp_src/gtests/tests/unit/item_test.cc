#include "item_move_semantics_api.h"

using reindexer::Item;
using reindexer::Slice;
using std::unique_ptr;
using std::vector;

TEST_F(ItemMoveSemanticsApi, MoveSemanticsOperator) {
	CreateNamespace(default_namespace);

	Error err =
		reindexer->AddIndex(default_namespace, {idFieldName.c_str(), idFieldName.c_str(), "", "int", IndexOpts(false, true, false)});

	prepareJsons();
	prepareItemsFromJsons();

	upsertItems();

	checkJsonsOfWrittenItems();
}
