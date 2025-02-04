#pragma once
#include <gtest/gtest.h>
#include <memory>
#include "client/reindexer.h"
#include "core/namespace/namespacestat.h"
#include "core/reindexer.h"
#include "tools/stringstools.h"

struct IndexDeclaration {
	std::string_view indexName;
	std::string_view fieldType;
	std::string_view indexType;
	IndexOpts indexOpts;
	int64_t expireAfter;
};

struct ReplicationTestState {
	reindexer::lsn_t lsn;
	reindexer::lsn_t nsVersion;
	reindexer::lsn_t ownLsn;
	uint64_t dataHash = 0;
	size_t dataCount = 0;
	std::optional<int> tmVersion;
	std::optional<int> tmStatetoken;
	uint64_t updateUnixNano = 0;
	reindexer::ClusterizationStatus::Role role = reindexer::ClusterizationStatus::Role::None;
};

template <typename DB>
class ReindexerTestApi {
public:
	using ItemType = typename DB::ItemT;
	using QueryResultsType = typename DB::QueryResultsT;

	ReindexerTestApi();
	ReindexerTestApi(const typename DB::ConfigT& cfg);

	template <typename FieldsT>
	static void DefineNamespaceDataset(DB& rx, std::string_view ns, const FieldsT& fields) {
		auto err = reindexer::Error();
		for (const auto& field : fields) {
			if (field.indexType != "composite") {
				err = rx.AddIndex(ns, {std::string{field.indexName},
									   {std::string{field.indexName}},
									   std::string{field.fieldType},
									   std::string{field.indexType},
									   field.indexOpts});
			} else {
				std::string indexName{field.indexName};
				std::string idxContents{field.indexName};
				auto eqPos = indexName.find_first_of('=');
				if (eqPos != std::string::npos) {
					idxContents = indexName.substr(0, eqPos);
					indexName = indexName.substr(eqPos + 1);
				}
				reindexer::JsonPaths jsonPaths;
				jsonPaths = reindexer::split(idxContents, "+", true, jsonPaths);

				err = rx.AddIndex(ns, {indexName, jsonPaths, std::string{field.fieldType}, std::string{field.indexType}, field.indexOpts,
									   field.expireAfter});
			}
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
	void DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}
	void DefineNamespaceDataset(std::string_view ns, const std::vector<IndexDeclaration>& fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}

	ItemType NewItem(std::string_view ns);
	void OpenNamespace(std::string_view ns, const StorageOpts& storage = StorageOpts());
	void AddIndex(std::string_view ns, const reindexer::IndexDef& idef);
	void UpdateIndex(std::string_view ns, const reindexer::IndexDef& idef);
	void DropIndex(std::string_view ns, std::string_view name);
	void Upsert(std::string_view ns, ItemType& item);
	void UpsertJSON(std::string_view ns, std::string_view json);
	void Update(const reindexer::Query& q, QueryResultsType& qr);
	size_t Update(const reindexer::Query& q);
	QueryResultsType UpdateQR(const reindexer::Query& q);
	void Select(const reindexer::Query& q, QueryResultsType& qr);
	QueryResultsType Select(const reindexer::Query& q);
	void Delete(std::string_view ns, ItemType& item);
	size_t Delete(const reindexer::Query& q);
	void Delete(const reindexer::Query& q, QueryResultsType& qr);
	ReplicationTestState GetReplicationState(std::string_view ns);
	reindexer::Error DumpIndex(std::ostream& os, std::string_view ns, std::string_view index);
	void PrintQueryResults(const std::string& ns, const QueryResultsType& res);
	std::string RandString(unsigned minLen = 4, unsigned maxRandLen = 4);
	std::string RandLikePattern();
	std::string RuRandString();
	std::vector<int> RandIntVector(size_t size, int start, int range);
	void SetVerbose(bool v) noexcept { verbose = v; }
	std::shared_ptr<DB> reindexer;

	static std::vector<std::string> GetSerializedQrItems(reindexer::QueryResults& qr);

private:
	const std::string letters = "abcdefghijklmnopqrstuvwxyz";
	const std::wstring ru_letters = L"абвгдеёжзийклмнопрстуфхцчшщъыьэюя";
	bool verbose = false;
};

extern template class ReindexerTestApi<reindexer::Reindexer>;
extern template class ReindexerTestApi<reindexer::client::Reindexer>;
