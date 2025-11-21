#pragma once

#include "reindexer_api.h"

class NestedArraysApi : public ReindexerApi {
public:
	void SetUp() override;

protected:
	std::string newItemJson(int id);
	void checkData() const;
	void insertItems(int count, auto newItemJsonFn);
	void addItemForUpdate(std::optional<std::string> intValue, std::optional<std::string> strValue);
	void testSetInt(const std::string& updateExpr, std::string expectedValue);
	void testSetStr(const std::string& updateExpr, std::string expectedValue);
	void set(int id, const std::string& field, const std::string& updateExpr);
	void drop(int id, const std::string& field, const std::string& arrayIndex = "");
	std::string makeUpdateItemJson(int id, const std::optional<std::string>& intValue, const std::optional<std::string>& strValue);
	void testDropInt(const std::string& arrayIndex = "", std::optional<std::string> expectedValue = std::nullopt);
	void testDropStr(const std::string& arrayIndex = "", std::optional<std::string> expectedValue = std::nullopt);

	std::vector<std::string> jsons_;
	std::string nsName_ = "nested_arrays_cxx_test";
	std::string idIdxName_ = "id";
	std::string heteroArrayFldName_ = "hetero_array";
	std::string objectArrayFldName_ = "object_array";
	std::string nestedArrayIdxName_ = "object_array";
	std::string nestedArraySparseIdxName_ = "object_array_sparse";
	std::string intArrayIdxName_ = "int_array";
	std::string intSparseArrayIdxName_ = "int_array_sparse";
	std::string strArrayIdxName_ = "str_array";
	std::string strSparseArrayIdxName_ = "str_array_sparse";
	std::string intArrayFld_ = "int_array_field";
	std::string strArrayFld_ = "str_array_field";
	std::optional<std::string> lastIntValue_;
	std::optional<std::string> lastStrValue_;
};
