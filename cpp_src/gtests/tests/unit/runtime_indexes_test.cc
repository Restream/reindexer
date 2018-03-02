#include "runtime_indexes_api.h"

TEST_F(RuntimeIndexesApi, RuntimeIndexesAddTest) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
	}

	FillNamespace(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
	}

	FillNamespace(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesDropTest) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
	}

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
	}

	FillNamespace(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
	}

	FillNamespace(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesDropTest2) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i, true);
		AddDataForRuntimeStringIndex(i);
	}

	FillNamespace(101, 200);
	FillNamespace(201, 300);

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
	}

	FillNamespace(301, 400);
}

TEST_F(RuntimeIndexesApi, RuntimePKIndexesTest) {
	FillNamespace(0, 100);

	AddRuntimeStringIndex(1, true);
	AddDataForRuntimeStringIndex(1);

	FillNamespace(101, 200);

	DropRuntimeStringIndex(1);

	FillNamespace(201, 300);

	AddRuntimeStringIndex(1, true);
	DropRuntimeStringIndex(1);

	FillNamespace(301, 400);
}

TEST_F(RuntimeIndexesApi, RuntimeCompositeIndexesTest) {
	FillNamespace(0, 100);
	AddRuntimeCompositeIndex();
	FillNamespace(101, 200);
	DropRuntimeCompositeIndex();
	FillNamespace(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimePKCompositeIndexesTest) {
	FillNamespace(0, 100);
	AddRuntimeCompositeIndex(true);
	FillNamespace(301, 400);
	DropRuntimeCompositeIndex(true);
	FillNamespace(401, 500);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesRemoveAndSelect) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
	}

	AddRuntimeCompositeIndex();

	DropRuntimeStringIndex(1);

	CheckSelectValidity(Query(default_namespace));
	CheckSelectValidity(Query(default_namespace).Where(getRuntimeStringIndexName(2).c_str(), CondGt, RandString()));
	// CheckSelectValidity(Query(default_namespace).Where(getRuntimeCompositeIndexName(false).c_str(), CondGt, RandString()));
}
