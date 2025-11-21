#include "core/index/rtree/rtree.h"
#include "core/cjson/jsonbuilder.h"
#include "core/index/rtree/greenesplitter.h"
#include "core/index/rtree/linearsplitter.h"
#include "core/index/rtree/quadraticsplitter.h"
#include "core/index/rtree/rstarsplitter.h"
#include "gtest/gtest.h"
#include "gtests/tools.h"
#include "reindexer_api.h"
#include "tools/float_comparison.h"

namespace {

static constexpr long long kRange = 1000ll;

template <typename T>
struct Compare;

template <>
struct [[nodiscard]] Compare<reindexer::Point> {
	bool operator()(reindexer::Point lhs, reindexer::Point rhs) const noexcept {
		if (reindexer::fp::EqualWithinULPs(lhs.X(), rhs.X())) {
			return lhs.Y() < rhs.Y();
		}
		return lhs.X() < rhs.X();
	}
};

template <typename T>
struct [[nodiscard]] Compare<reindexer::RMapValue<T, size_t>> {
	bool operator()(const reindexer::RMapValue<T, size_t>& lhs, const reindexer::RMapValue<T, size_t>& rhs) const noexcept {
		return lhs.second < rhs.second;
	}
};

template <typename RTree>
class [[nodiscard]] SearchVisitor : public RTree::Visitor {
public:
	bool operator()(const typename RTree::value_type& v) override {
		const auto it = data_.find(v);
		if (it == data_.end()) {
			++wrong_;
		} else {
			data_.erase(it);
		}
		return false;
	}
	size_t Size() const noexcept { return data_.size(); }
	void Add(const typename RTree::value_type& r) { data_.insert(r); }
	size_t Wrong() const noexcept { return wrong_; }

private:
	size_t wrong_ = 0;
	std::multiset<typename RTree::value_type, Compare<typename RTree::value_type>> data_;
};

template <typename RTree>
class [[nodiscard]] DeleteVisitor : public RTree::Visitor {
public:
	DeleteVisitor(const reindexer::Rectangle& r) : rect_{r} {}
	bool operator()(const typename RTree::value_type& v) override { return rect_.Contain(RTree::traits::GetPoint(v)); }

private:
	const reindexer::Rectangle rect_;
};

}  // namespace

// Checks of inserting of points to RectangleTree and verifies of its structure after each insertion
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestInsert() {
	reindexer::RectangleTree<reindexer::Point, Splitter, 16, 8> tree;
	ASSERT_TRUE(tree.Check());

	size_t insertedCount = 0;
	for (size_t i = 0; i < 10000; ++i) {
		const auto p = randPoint(kRange);
		const auto insertRes{tree.insert(reindexer::Point{p})};
		if (insertRes.second) {
			++insertedCount;
		}
		ASSERT_TRUE(*insertRes.first == p);
		ASSERT_TRUE(tree.Check());
		ASSERT_EQ(tree.size(), insertedCount);
	}
}

TEST(RTree, QuadraticInsert) { TestInsert<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearInsert) { TestInsert<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneInsert) { TestInsert<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarInsert) { TestInsert<reindexer::RStarSplitter>(); }

// Checks that iterators could iterate over whole RectangleTree after multiple modifications of the tree
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestIterators() {
	reindexer::RectangleTree<reindexer::Point, Splitter, 16, 8> tree;
	ASSERT_TRUE(tree.Check());
	ASSERT_TRUE(tree.begin() == tree.end());
	ASSERT_FALSE(tree.begin() != tree.end());
	ASSERT_TRUE(tree.cbegin() == tree.cend());
	ASSERT_FALSE(tree.cbegin() != tree.cend());

	size_t dublicates = 0;
	for (size_t i = 0; i < 10000 + dublicates; ++i) {
		const auto res = tree.insert(randPoint(kRange));
		if (!res.second) {
			++dublicates;
		}
		ASSERT_TRUE(tree.Check());
		auto it = tree.begin(), end = tree.end();
		auto cit = tree.cbegin(), cend = tree.cend();
		for (size_t j = 0; j <= i - dublicates; ++j) {
			ASSERT_FALSE(it == end);
			ASSERT_TRUE(it != end);
			ASSERT_FALSE(cit == cend);
			ASSERT_TRUE(cit != cend);
			++it;
			++cit;
		}
		ASSERT_TRUE(it == end);
		ASSERT_FALSE(it != end);
		ASSERT_TRUE(cit == cend);
		ASSERT_FALSE(cit != cend);
	}
}

TEST(RTree, QuadraticIterators) { TestIterators<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearIterators) { TestIterators<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneIterators) { TestIterators<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarIterators) { TestIterators<reindexer::RStarSplitter>(); }

// Verifies of searching of points in RectangleTree by DWithin
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestSearch() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter, 16, 8>;
	constexpr size_t kCount = 100000;

	RTree tree;
	std::vector<reindexer::Point> data;
	size_t dublicates = 0;
	for (size_t i = 0; i < kCount + dublicates; ++i) {
		const auto res = tree.insert(randPoint(kRange));
		if (res.second) {
			data.push_back(*res.first);
		} else {
			++dublicates;
		}
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	for (size_t i = 0; i < 1000; ++i) {
		SearchVisitor<RTree> DWithinVisitor;
		const reindexer::Point point{randPoint(kRange)};
		const double distance = randBin<double>(0, 100);
		for (const auto& r : data) {
			if (reindexer::DWithin(point, r, distance)) {
				DWithinVisitor.Add(r);
			}
		}

		tree.DWithin(point, distance, DWithinVisitor);
		ASSERT_EQ(DWithinVisitor.Size(), 0);
		ASSERT_EQ(DWithinVisitor.Wrong(), 0);
	}
}

TEST(RTree, QuadraticSearch) { TestSearch<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearSearch) { TestSearch<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneSearch) { TestSearch<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarSearch) { TestSearch<reindexer::RStarSplitter>(); }

// Checks of deleting of points from RectangleTree and verifies of its structure after each deletion
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestDelete() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter, 16, 8>;
	constexpr size_t kCount = 10000;

	RTree tree;
	for (size_t i = 0; i < kCount;) {
		i += tree.insert(randPoint(kRange)).second;
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	size_t deletedCount = 0;
	for (size_t i = 0; i < 1000; ++i) {
		DeleteVisitor<RTree> visitor{{randPoint(kRange), randPoint(kRange)}};
		if (tree.DeleteOneIf(visitor)) {
			++deletedCount;
		}
		ASSERT_TRUE(tree.Check());
		ASSERT_EQ(tree.size(), kCount - deletedCount);
	}
}

TEST(RTree, QuadraticDelete) { TestDelete<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearDelete) { TestDelete<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneDelete) { TestDelete<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarDelete) { TestDelete<reindexer::RStarSplitter>(); }

// Checks of deleting of points iterators point to from RectangleTree and verifies of its structure after each deletion
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestErase() {
	using RTree = reindexer::RectangleTree<reindexer::Point, Splitter, 16, 8>;
	constexpr size_t kCount = 10000;

	RTree tree;
	for (size_t i = 0; i < kCount;) {
		i += tree.insert(randPoint(kRange)).second;
	}
	ASSERT_TRUE(tree.Check());
	ASSERT_EQ(tree.size(), kCount);

	for (size_t i = 0; i < 1000; ++i) {
		auto it = tree.begin();
		for (size_t j = 0, k = rand() % (kCount - i); j < k; ++j) {
			++it;
		}
		tree.erase(it);
		ASSERT_TRUE(tree.Check()) << i;
		ASSERT_EQ(tree.size(), kCount - i - 1);
	}
}

TEST(RTree, QuadraticErase) { TestErase<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearErase) { TestErase<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneErase) { TestErase<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarErase) { TestErase<reindexer::RStarSplitter>(); }

// Checks of inserting, deleting search of points in RectangleTree and verifies of its structure after each its modidfication
template <template <typename, typename, typename, typename, size_t, size_t> class Splitter>
static void TestMap() {
	using Map = reindexer::RTreeMap<size_t, Splitter, 16, 8>;
	constexpr size_t kCount = 10000;

	Map map;
	std::vector<typename Map::value_type> data;
	size_t dublicates = 0;
	for (size_t i = 0; i < kCount + dublicates; ++i) {
		const auto res = map.insert({randPoint(kRange), i});
		if (res.second) {
			data.emplace_back(res.first->first, i);
		} else {
			++dublicates;
		}
	}
	ASSERT_TRUE(map.Check());

	for (size_t i = 0; i < 1000; ++i) {
		SearchVisitor<Map> visitor;
		const reindexer::Point point{randPoint(kRange)};
		const double distance = randBin<double>(0, 100);
		for (const auto& r : data) {
			if (reindexer::DWithin(point, r.first, distance)) {
				visitor.Add(r);
			}
		}
		map.DWithin(point, distance, visitor);
		ASSERT_EQ(visitor.Size(), 0);
		ASSERT_EQ(visitor.Wrong(), 0);
	}

	size_t deletedCount = 0;
	for (size_t i = 0; i < 1000; ++i) {
		DeleteVisitor<Map> visitor{{randPoint(kRange), randPoint(kRange)}};
		ASSERT_TRUE(map.Check());
		if (map.DeleteOneIf(visitor)) {
			++deletedCount;
		}
		ASSERT_EQ(map.size(), kCount - deletedCount);
	}
}

TEST(RTree, QuadraticMap) { TestMap<reindexer::QuadraticSplitter>(); }
TEST(RTree, LinearMap) { TestMap<reindexer::LinearSplitter>(); }
TEST(RTree, GreeneMap) { TestMap<reindexer::GreeneSplitter>(); }
TEST(RTree, RStarMap) { TestMap<reindexer::RStarSplitter>(); }

// Make sure RTree indexes work with null values correctly
TEST_F(ReindexerApi, EmptyRTreeSparseValues) {
	// Create namespace and add 2 RTree indexes (of type Sparse)
	rt.OpenNamespace(default_namespace);
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"point1", "rtree", "point", IndexOpts().Sparse(true).RTreeType(IndexOpts::Linear)});
	rt.AddIndex(default_namespace, {"point2", "rtree", "point", IndexOpts().Sparse(true).RTreeType(IndexOpts::Linear)});

	// Fill namespace with null values for RTree fields
	reindexer::WrSerializer wrser;
	for (int i = 0; i < 100; ++i) {
		wrser.Reset();
		reindexer::JsonBuilder jsonBuilder(wrser, reindexer::ObjType::TypeObject);
		jsonBuilder.Put("id", reindexer::Variant(i));
		jsonBuilder.Null("point1");
		jsonBuilder.Null("point2");
		jsonBuilder.End();

		rt.UpsertJSON(default_namespace, wrser.Slice());
	}

	// Make sure we can select data normally and access newly added RTree null fields
	{
		QueryResults qr = rt.Select(Query(default_namespace).Where("id", CondEq, 13));
		ASSERT_TRUE(qr.Count() == 1);
		Item item = qr.begin().GetItem(false);
		Variant idVal = item["id"];
		ASSERT_TRUE(idVal.As<int>() == 13);
		Variant idPoint1 = item["point1"];
		ASSERT_TRUE(idPoint1.IsNullValue());
		Variant idPoint2 = item["point2"];
		ASSERT_TRUE(idPoint2.IsNullValue());
	}

	// Make sure removal of items with null RTree items works as expected
	{
		QueryResults qr;
		rt.Delete(Query(default_namespace).Where("id", CondEq, 13), qr);
		ASSERT_TRUE(qr.Count() == 1);
	}
}
