#pragma once
#include <gtest/gtest.h>
#include <memory>
#include "client/reindexer.h"
#include "core/namespace/namespacestat.h"
#include "core/reindexer.h"

struct [[nodiscard]] IndexDeclaration {
	std::string_view indexName;
	std::string_view fieldType;
	std::string_view indexType;
	IndexOpts indexOpts;
	int64_t expireAfter;
};

struct [[nodiscard]] ReplicationTestState {
	reindexer::lsn_t lsn;
	reindexer::lsn_t nsVersion;
	reindexer::lsn_t ownLsn;
	uint64_t dataHash = 0;
	size_t dataCount = 0;
	std::optional<int> tmVersion;
	std::optional<int> tmStatetoken;
	uint64_t updateUnixNano = 0;
	reindexer::ClusterOperationStatus::Role role = reindexer::ClusterOperationStatus::Role::None;
};

template <typename DB>
class [[nodiscard]] ReindexerTestApi {
public:
	using ItemType = typename DB::ItemT;
	using QueryResultsType = typename DB::QueryResultsT;
	using TransactionType = typename DB::TransactionT;
	using ConnectOptsType = typename DB::ConnectOptsT;

	ReindexerTestApi();
	ReindexerTestApi(const typename DB::ConfigT& cfg);

	static void DefineNamespaceDataset(DB& rx, std::string_view ns, std::span<const IndexDeclaration> fields);
	void DefineNamespaceDataset(std::string_view ns, std::span<const IndexDeclaration> fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}
	void DefineNamespaceDataset(std::string_view ns, std::initializer_list<const IndexDeclaration> fields) {
		DefineNamespaceDataset(*reindexer, ns, fields);
	}

	void Connect(const std::string& dsn, const ConnectOptsType& opts = ConnectOptsType());
	ItemType NewItem(std::string_view ns);
	void OpenNamespace(std::string_view ns, const StorageOpts& storage = StorageOpts().Enabled().CreateIfMissing());
	void CloseNamespace(std::string_view ns);
	void DropNamespace(std::string_view ns);
	void AddNamespace(const reindexer::NamespaceDef&);
	void TruncateNamespace(std::string_view ns);
	void AddIndex(std::string_view ns, const reindexer::IndexDef& idef);
	void UpdateIndex(std::string_view ns, const reindexer::IndexDef& idef);
	void DropIndex(std::string_view ns, std::string_view name);
	void Insert(std::string_view ns, ItemType& item);
	void Insert(std::string_view ns, ItemType& item, QueryResultsType&);
	void Upsert(std::string_view ns, ItemType& item);
	void Upsert(std::string_view ns, ItemType& item, QueryResultsType&);
	void Update(std::string_view ns, ItemType& item);
	void Update(std::string_view ns, ItemType& item, QueryResultsType&);
	void UpsertJSON(std::string_view ns, std::string_view json);
	void InsertJSON(std::string_view ns, std::string_view json);
	void Update(const reindexer::Query& q, QueryResultsType& qr);
	size_t Update(const reindexer::Query& q);
	QueryResultsType UpdateQR(const reindexer::Query& q);
	void Select(const reindexer::Query& q, QueryResultsType& qr) const;
	QueryResultsType Select(const reindexer::Query& q) const;
	QueryResultsType ExecSQL(std::string_view sql) const;
	void Delete(std::string_view ns, ItemType& item);
	void Delete(std::string_view ns, ItemType& item, QueryResultsType&);
	size_t Delete(const reindexer::Query& q);
	void Delete(const reindexer::Query& q, QueryResultsType& qr);
	std::vector<reindexer::NamespaceDef> EnumNamespaces(reindexer::EnumNamespacesOpts opts);
	void RenameNamespace(std::string_view srcNsName, const std::string& dstNsName);
	TransactionType NewTransaction(std::string_view ns);
	QueryResultsType CommitTransaction(TransactionType& tx);
	ReplicationTestState GetReplicationState(std::string_view ns);
	void SetSchema(std::string_view ns, std::string_view schema);
	std::string GetSchema(std::string_view ns, int format);

	reindexer::Error DumpIndex(std::ostream& os, std::string_view ns, std::string_view index);
	void PrintQueryResults(const std::string& ns, const QueryResultsType& res);
	static std::string RandString(unsigned minLen = 4, unsigned maxRandLen = 4);
	std::string RandLikePattern();
	std::string RuRandString();
	std::vector<int> RandIntVector(size_t size, int start, int range);
	std::vector<std::string> RandStrVector(size_t size);
	void SetVerbose(bool v) noexcept { verbose_ = v; }
	static void EnablePerfStats(DB& rx);
	void AwaitIndexOptimization(std::string_view nsName);

	std::shared_ptr<DB> reindexer;

	static std::vector<std::string> GetSerializedQrItems(reindexer::QueryResults& qr);

private:
	constexpr static std::string_view kLetters = "abcdefghijklmnopqrstuvwxyz";
	constexpr static std::wstring_view kRuLetters = L"абвгдеёжзийклмнопрстуфхцчшщъыьэюя";
	bool verbose_ = false;
};

template <>
void ReindexerTestApi<reindexer::Reindexer>::AwaitIndexOptimization(std::string_view nsName);

extern template class ReindexerTestApi<reindexer::Reindexer>;
extern template class ReindexerTestApi<reindexer::client::Reindexer>;
