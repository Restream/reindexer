#include "runtime_indexes_api.h"

TEST_F(RuntimeIndexesApi, RuntimeIndexesAddTest) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
	}

	FillNamespace(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
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
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
	}

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
	}

	FillNamespace(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
	}

	FillNamespace(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesDropTest2) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
	}

	FillNamespace(101, 200);
	FillNamespace(201, 300);

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
	}

	FillNamespace(301, 400);
}

TEST_F(RuntimeIndexesApi, RuntimePKIndexesTest) {
	FillNamespace(0, 100);

	AddRuntimeStringIndex(1);
	AddDataForRuntimeStringIndex(1);

	FillNamespace(101, 200);

	DropRuntimeStringIndex(1);

	FillNamespace(201, 300);

	AddRuntimeStringIndex(1);
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
	AddRuntimeCompositeIndex();
	FillNamespace(301, 400);
	DropRuntimeCompositeIndex();
	FillNamespace(401, 500);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesRemoveAndSelect) {
	FillNamespace(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
	}

	AddRuntimeCompositeIndex();
	FillNamespace(0, 100);

	CheckSelectValidity(
		Query(default_namespace)
			.WhereComposite(getRuntimeCompositeIndexName(false).c_str(), CondEq, {{Variant(rand()), Variant(RandString())}}));

	DropRuntimeStringIndex(1);
	DropRuntimeQPointIndex(1);
	DropRuntimeLPointIndex(1);

	CheckSelectValidity(Query(default_namespace));
	CheckSelectValidity(Query(default_namespace).Where(getRuntimeStringIndexName(2).c_str(), CondGt, RandString()));

	CheckSelectValidity(
		Query(default_namespace)
			.WhereComposite(getRuntimeCompositeIndexName(false).c_str(), CondGt, {{Variant(rand()), Variant(RandString())}}));

	CheckSelectValidity(Query(default_namespace)
							.DWithin(getRuntimeQPointIndexName(2), RandPoint(), RandDouble(0.0, 1.0, 1000))
							.Or()
							.DWithin(getRuntimeLPointIndexName(2), RandPoint(), RandDouble(0.0, 1.0, 1000)));
}
