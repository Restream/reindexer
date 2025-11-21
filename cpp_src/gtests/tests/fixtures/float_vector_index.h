#pragma once

#include <gtest/gtest.h>
#include "core/item.h"
#include "core/reindexer.h"
#include "reindexertestapi.h"

class [[nodiscard]] FloatVector : public ::testing::TestWithParam<reindexer::VectorMetric> {
protected:
	enum class [[nodiscard]] HasIndex : bool { Yes = true, No = false };

	void SetUp() override;

	template <size_t Dim>
	reindexer::FloatVector rndFloatVector();
	std::pair<reindexer::FloatVector, std::string> rndFloatVectorForSerializers(bool withSpace);
	size_t deleteSomeItems(std::string_view nsName, int maxElements, std::unordered_set<int>& emptyVectors);
	template <size_t Dimension>
	reindexer::Item newItem(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors);
	reindexer::Item itemForDelete(std::string_view nsName, int id);
	template <size_t Dimension>
	reindexer::Item newItemDirect(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors);
	template <size_t Dimension>
	reindexer::Item newItemFromJson(std::string_view nsName, std::string_view fieldName, int id, std::unordered_set<int>& emptyVectors);
	template <size_t Dimension>
	reindexer::Item newItem(std::string_view nsName, size_t fieldsCount, std::vector<std::vector<reindexer::FloatVector>>& items);
	template <size_t Dimension>
	reindexer::Item newItem(std::string_view nsName, size_t fieldsCount, int id, std::vector<std::vector<reindexer::FloatVector>>& items);
	void rebuildCentroids();
	template <size_t Dimension>
	void upsertItems(std::string_view nsName, std::string_view fieldName, int startId, int endId, std::unordered_set<int>& emptyVectors,
					 HasIndex = HasIndex::Yes);
	template <size_t Dimension, typename SearchParamGetterT>
	void runMultithreadQueries(size_t threads, size_t queriesPerThread, std::string_view nsName, std::string_view fieldName,
							   const SearchParamGetterT& getKNNParam);

	void validateIndexValueInItem(std::string_view ns, std::string_view field, std::string_view json, std::span<const float> expected);
	void validateIndexValueInQueryResults(std::string_view field, const reindexer::QueryResults& qr, std::span<const float> expected);
	template <typename ParamT, size_t kDims>
	void checkEmptyIndexSelection(std::string_view ns, std::string_view vectorIndex);

	ReindexerTestApi<reindexer::Reindexer> rt;
};
