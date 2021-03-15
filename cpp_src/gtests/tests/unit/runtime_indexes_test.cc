#include "runtime_indexes_api.h"

TEST_F(RuntimeIndexesApi, RuntimeIndexesAddTest) {
	FillNamespaces(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
	}

	FillNamespaces(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
	}

	FillNamespaces(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesDropTest) {
	FillNamespaces(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
	}

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
		DropRuntimeGPointIndex(i);
		DropRuntimeSPointIndex(i);
	}

	FillNamespaces(101, 200);

	for (int i = 5; i < 10; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
		DropRuntimeGPointIndex(i);
		DropRuntimeSPointIndex(i);
	}

	FillNamespaces(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesDropTest2) {
	FillNamespaces(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeIntArrayIndex(i);
		AddDataForRuntimeIntIndex(i);
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
	}

	FillNamespaces(101, 200);
	FillNamespaces(201, 300);

	for (int i = 4; i >= 0; --i) {
		DropRuntimeIntArrayIndex(i);
		DropRuntimeStringIndex(i);
		DropRuntimeQPointIndex(i);
		DropRuntimeLPointIndex(i);
		DropRuntimeGPointIndex(i);
		DropRuntimeSPointIndex(i);
	}

	FillNamespaces(301, 400);
}

TEST_F(RuntimeIndexesApi, RuntimePKIndexesTest) {
	FillNamespaces(0, 100);

	AddRuntimeStringIndex(1);
	AddDataForRuntimeStringIndex(1);

	FillNamespaces(101, 200);

	DropRuntimeStringIndex(1);

	FillNamespaces(201, 300);

	AddRuntimeStringIndex(1);
	DropRuntimeStringIndex(1);

	FillNamespaces(301, 400);
}

TEST_F(RuntimeIndexesApi, RuntimeCompositeIndexesTest) {
	FillNamespaces(0, 100);
	AddRuntimeCompositeIndex();
	FillNamespaces(101, 200);
	DropRuntimeCompositeIndex();
	FillNamespaces(201, 300);
}

TEST_F(RuntimeIndexesApi, RuntimePKCompositeIndexesTest) {
	FillNamespaces(0, 100);
	AddRuntimeCompositeIndex();
	FillNamespaces(301, 400);
	DropRuntimeCompositeIndex();
	FillNamespaces(401, 500);
}

TEST_F(RuntimeIndexesApi, RuntimeIndexesRemoveAndSelect) {
	FillNamespaces(0, 100);

	for (int i = 0; i < 5; ++i) {
		AddRuntimeStringIndex(i);
		AddDataForRuntimeStringIndex(i);
		AddRuntimeQPointIndex(i);
		AddDataForRuntimeQPointIndex(i);
		AddRuntimeLPointIndex(i);
		AddDataForRuntimeLPointIndex(i);
		AddRuntimeGPointIndex(i);
		AddDataForRuntimeGPointIndex(i);
		AddRuntimeSPointIndex(i);
		AddDataForRuntimeSPointIndex(i);
	}

	AddRuntimeCompositeIndex();
	FillNamespaces(0, 100);

	CheckSelectValidity(
		Query(default_namespace)
			.WhereComposite(getRuntimeCompositeIndexName(false).c_str(), CondEq, {{Variant(rand()), Variant(RandString())}}));

	DropRuntimeStringIndex(1);
	DropRuntimeQPointIndex(1);
	DropRuntimeLPointIndex(1);
	DropRuntimeGPointIndex(1);
	DropRuntimeSPointIndex(1);

	CheckSelectValidity(Query(default_namespace));
	CheckSelectValidity(Query(default_namespace).Where(getRuntimeStringIndexName(2).c_str(), CondGt, RandString()));

	CheckSelectValidity(
		Query(default_namespace)
			.WhereComposite(getRuntimeCompositeIndexName(false).c_str(), CondGt, {{Variant(rand()), Variant(RandString())}}));

	CheckSelectValidity(Query(geom_namespace)
							.DWithin(getRuntimeQPointIndexName(2), randPoint(10), randBinDouble(0, 1))
							.Or()
							.DWithin(getRuntimeLPointIndexName(2), randPoint(10), randBinDouble(0, 1))
							.DWithin(getRuntimeGPointIndexName(2), randPoint(10), randBinDouble(0, 1))
							.Or()
							.DWithin(getRuntimeSPointIndexName(2), randPoint(10), randBinDouble(0, 1)));
}
