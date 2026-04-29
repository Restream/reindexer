#pragma once

#include <gtest/gtest.h>
#include <unordered_set>
#include "core/item.h"
#include "core/keyvalue/float_vector.h"
#include "core/reindexer.h"
#include "reindexertestapi.h"

class [[nodiscard]] FloatVector : public ::testing::TestWithParam<reindexer::VectorMetric> {
protected:
	enum class [[nodiscard]] HasIndex : bool { Yes = true, No = false };
	enum [[nodiscard]] IsArray : bool { Array = true, Scalar = false };

	void SetUp() override;

	template <IsArray>
	void TestHnswIndex();
	template <IsArray>
	void TestVecBruteforceIndex();
	template <IsArray>
	void TestIvfIndex();
	template <IsArray>
	void TestDeleteIndex();
	template <IsArray>
	void TestUpdateIndex(reindexer::VectorMetric metric);
	template <IsArray>
	void TestHnswIndexMTRace();
	template <IsArray>
	void HnswIndexItemSet();
	template <IsArray>
	void TestHnswIndexUpdateQuery();
	template <IsArray>
	void TestKeeperQRIterateAfterDropIndex();
	template <IsArray, IndexType>
	void TestQueries();
	template <IsArray>
	void TestSelectFilters();

	template <size_t Dim>
	reindexer::FloatVector rndFloatVector();
	std::pair<reindexer::FloatVector, std::string> rndFloatVectorForSerializers(bool withSpace);
	void deleteSomeItems(std::string_view nsName, int maxElements, std::unordered_set<int>& emptyVectors,
						 std::unordered_map<int, size_t>& vectorsCount);
	template <size_t Dimension, IsArray>
	reindexer::Item newItem(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
							std::unordered_map<int, size_t>& vectorsCount);
	reindexer::Item itemForDelete(std::string_view nsName, int id);
	template <size_t Dimension, IsArray>
	reindexer::Item newItemDirect(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
								  std::unordered_map<int, size_t>& vectorsCount);
	template <size_t Dimension, IsArray>
	reindexer::Item newItemFromJson(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
									std::unordered_map<int, size_t>& vectorsCount);
	template <size_t Dimension>
	static void newVectorFromJson(reindexer::JsonBuilder&, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors,
								  std::unordered_map<int, size_t>& vectorsCount);
	template <size_t Dimension, IsArray>
	reindexer::Item newItem(std::string_view nsName, size_t fieldsCount,
							std::vector<std::vector<std::vector<reindexer::FloatVector>>>& items);
	template <size_t Dimension, IsArray>
	reindexer::Item newItem(std::string_view nsName, size_t fieldsCount, int id,
							std::vector<std::vector<std::vector<reindexer::FloatVector>>>& items);
	void rebuildCentroids();
	template <size_t Dimension, IsArray>
	void upsertItems(std::string_view nsName, std::string_view fieldName, int startId, int endId, std::unordered_set<int>& emptyVectors,
					 std::unordered_map<int, size_t>& vectorsCount, HasIndex = HasIndex::Yes);
	template <size_t Dimension, IsArray, typename SearchParamGetterT>
	void runMultithreadQueries(size_t threads, size_t queriesPerThread, std::string_view nsName, std::string_view fieldName,
							   const SearchParamGetterT& getKNNParam);
	template <size_t Dimension, IsArray>
	std::vector<reindexer::FloatVector> rndFVField();

	void validateIndexValueInItem(std::string_view ns, std::string_view field, std::string_view json, std::span<const float> expected);
	void validateIndexValueInItem(std::string_view ns, std::string_view field, std::string_view json,
								  std::span<const reindexer::ConstFloatVectorView> expected);
	void validateIndexValueInQueryResults(std::string_view field, const reindexer::QueryResults& qr, std::span<const float> expected);
	template <typename ParamT, size_t kDims>
	void checkEmptyIndexSelection(std::string_view ns, std::string_view vectorIndex);

	ReindexerTestApi<reindexer::Reindexer> rt;
};
