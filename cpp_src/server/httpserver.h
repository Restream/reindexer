#pragma once

#include <memory>
#include "config.h"
#include "core/reindexer.h"
#include "dbmanager.h"
#include "estl/fast_hash_map.h"
#include "loggerwrapper.h"
#include "net/http/router.h"
#include "net/listener.h"
#include "pprof/pprof.h"

namespace reindexer_server {

class Prometheus;
struct IStatsWatcher;

using namespace reindexer::net;

struct [[nodiscard]] HTTPClientData final : public http::ClientData {
	AuthContext auth;
};

class [[nodiscard]] HTTPServer final {
public:
	HTTPServer(DBManager& dbMgr, LoggerWrapper& logger, const ServerConfig& serverConfig, Prometheus* prometheusI = nullptr,
			   IStatsWatcher* statsWatcherI = nullptr);

	void Start(const std::string& addr, ev::dynamic_loop& loop);
	void Stop() { listener_->Stop(); }

	int NotFoundHandler(http::Context& ctx);
	int DocHandler(http::Context& ctx);
	int Check(http::Context& ctx);
	int PostQuery(http::Context& ctx);
	int DeleteQuery(http::Context& ctx);
	int UpdateQuery(http::Context& ctx);
	int GetSQLQuery(http::Context& ctx);
	int PostSQLQuery(http::Context& ctx);
	int GetSQLSuggest(http::Context& ctx);
	int GetDatabases(http::Context& ctx);
	int PostDatabase(http::Context& ctx);
	int DeleteDatabase(http::Context& ctx);
	int GetNamespaces(http::Context& ctx);
	int GetNamespace(http::Context& ctx);
	int PostNamespace(http::Context& ctx);
	int DeleteNamespace(http::Context& ctx);
	int TruncateNamespace(http::Context& ctx);
	int RenameNamespace(http::Context& ctx);
	int GetItems(http::Context& ctx);
	int PostItems(http::Context& ctx);
	int PatchItems(http::Context& ctx);
	int PutItems(http::Context& ctx);
	int DeleteItems(http::Context& ctx);
	int GetIndexes(http::Context& ctx);
	int PostIndex(http::Context& ctx);
	int PutIndex(http::Context& ctx);
	int PutSchema(http::Context& ctx);
	int GetSchema(http::Context& ctx);
	int GetProtobufSchema(http::Context& ctx);
	int GetMetaList(http::Context& ctx);
	int GetMetaByKey(http::Context& ctx);
	int PutMetaByKey(http::Context& ctx);
	int DeleteMetaByKey(http::Context& ctx);
	int DeleteIndex(http::Context& ctx);
	int CheckAuth(http::Context& ctx);
	int BeginTx(http::Context& ctx);
	int CommitTx(http::Context& ctx);
	int RollbackTx(http::Context& ctx);
	int PostItemsTx(http::Context& ctx);
	int PutItemsTx(http::Context& ctx);
	int PatchItemsTx(http::Context& ctx);
	int DeleteItemsTx(http::Context& ctx);
	int GetSQLQueryTx(http::Context& ctx);
	int DeleteQueryTx(http::Context& ctx);
	int PostMemReset(http::Context& ctx);
	int GetMemInfo(http::Context& ctx);
	void Logger(http::Context& ctx);
	void OnResponse(http::Context& ctx);
	int GetRole(http::Context& ctx);
	int GetDefaultConfigs(http::Context& ctx);

private:
	enum class DataFormat { JSON, MsgPack, Protobuf, CSVFile };

	struct IQRSerializingOption {
		virtual ~IQRSerializingOption() = default;

		virtual std::optional<size_t> TotalCount() const noexcept = 0;
		virtual std::optional<size_t> QueryTotalCount() const noexcept = 0;
		virtual unsigned ExternalLimit() const noexcept = 0;
		virtual unsigned ExternalOffset() const noexcept = 0;
	};
	class TxCommitOption;
	class TwoLevelLimitOffsetOption;
	class ReqularQueryResultsOption;
	class ItemsQueryResultsOption;

	Error modifyItem(Reindexer& db, std::string_view nsName, Item& item, ItemModifyMode mode);
	Error modifyItem(Reindexer& db, std::string_view nsName, Item& item, QueryResults&, ItemModifyMode mode);
	int modifyItems(http::Context& ctx, ItemModifyMode mode);
	int modifyItemsTx(http::Context& ctx, ItemModifyMode mode);
	int modifyItemsProtobuf(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int modifyItemsMsgPack(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int modifyItemsJSON(http::Context& ctx, std::string_view nsName, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int modifyItemsTxProtobuf(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int modifyItemsTxMsgPack(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int modifyItemsTxJSON(http::Context& ctx, Transaction& tx, std::vector<std::string>&& precepts, ItemModifyMode mode);
	int queryResults(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption);
	int queryResultsMsgPack(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption, bool withColumns,
							int width = 0);
	int queryResultsProtobuf(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption, bool withColumns,
							 int width = 0);
	int queryResultsJSON(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption, bool withColumns,
						 int width = 0);
	int queryResultsCSV(http::Context& ctx, reindexer::QueryResults& res, const IQRSerializingOption& qrOption);
	template <typename Builder>
	void queryResultParams(Builder& builder, reindexer::QueryResults& res, std::vector<std::string>&& jsonData,
						   const IQRSerializingOption& qrOption, bool withColumns, int width);
	int statusOK(http::Context& ctx, chunk&& chunk);
	int status(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	int jsonStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	int msgpackStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	int protobufStatus(http::Context& ctx, const http::HttpStatus& status = http::HttpStatus());
	unsigned prepareLimit(std::string_view limitParam, int limitDefault = kDefaultLimit);
	unsigned prepareOffset(std::string_view offsetParam, int offsetDefault = kDefaultOffset);
	int modifyQueryTxImpl(http::Context& ctx, const std::string& dbName, std::string_view txId, Query& q);

	template <UserRole role>
	Reindexer getDB(http::Context& ctx, std::string* dbNameOut = nullptr);
	std::string getNameFromJson(std::string_view json);
	constexpr static std::string_view statsSourceName() { return std::string_view{"http"}; }

	std::shared_ptr<Transaction> getTx(const std::string& dbName, std::string_view txId);
	std::string addTx(std::string dbName, Transaction&& tx);
	void removeTx(const std::string& dbName, std::string_view txId);
	void removeExpiredTx();
	void deadlineTimerCb(ev::periodic&, int) { removeExpiredTx(); }

	Error execSqlQueryByType(std::string_view sqlQuery, reindexer::QueryResults& res, http::Context& ctx);
	bool isParameterSetOn(std::string_view val) const noexcept;
	int getAuth(http::Context& ctx, AuthContext& auth, const std::string& dbName) const;

	DataFormat dataFormatFromStr(std::string_view str);
	DataFormat getDataFormat(const http::Context& ctx);
	[[noreturn]] void throwUnsupportedOpFormat(const http::Context& ctx);

	DBManager& dbMgr_;
	Pprof pprof_;
	const ServerConfig& serverConfig_;
	Prometheus* prometheus_;
	IStatsWatcher* statsWatcher_;
	const std::string webRoot_;

	http::Router router_;
	std::unique_ptr<IListener> listener_;

	LoggerWrapper logger_;

	system_clock_w::time_point startTs_;

	using TxDeadlineClock = steady_clock_w;
	struct [[nodiscard]] TxInfo {
		std::shared_ptr<Transaction> tx;
		TxDeadlineClock::time_point txDeadline;
		std::string dbName;
	};
	fast_hash_map<std::string, TxInfo, nocase_hash_str, nocase_equal_str, nocase_less_str> txMap_;
	reindexer::mutex txMtx_;
	ev::timer deadlineChecker_;

	constexpr static int kDefaultLimit = std::numeric_limits<int>::max();
	constexpr static int kDefaultOffset = 0;

	constexpr static int32_t kMaxConcurrentCsvDownloads = 2;
	std::atomic<int32_t> currentCsvDownloads_ = {0};
};

}  // namespace reindexer_server
