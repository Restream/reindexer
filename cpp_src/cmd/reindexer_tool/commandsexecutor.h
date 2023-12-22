#pragma once

#include <condition_variable>
#include <unordered_map>
#include "core/namespacedef.h"
#include "core/rdxcontext.h"
#include "coroutine/channel.h"
#include "dumpoptions.h"
#include "iotools.h"
#include "net/ev/ev.h"
#include "vendor/urlparser/urlparser.h"

namespace reindexer_tool {

struct IExecutorsCommand;

class CancelContext : public reindexer::IRdxCancelContext {
public:
	CancelContext() : cancelType_(reindexer::CancelType::None) {}
	CancelContext(const CancelContext& ctx) = delete;
	CancelContext& operator=(const CancelContext& ctx) = delete;

	bool IsCancelable() const noexcept override final { return true; }
	reindexer::CancelType GetCancelType() const noexcept override final { return cancelType_.load(std::memory_order_acquire); }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override { return std::nullopt; }

	bool IsCancelled() const { return cancelType_.load(std::memory_order_acquire) == reindexer::CancelType::Explicit; }
	void Cancel() noexcept { cancelType_.store(reindexer::CancelType::Explicit, std::memory_order_release); }
	void Reset() noexcept { cancelType_.store(reindexer::CancelType::None, std::memory_order_release); }

private:
	std::atomic<reindexer::CancelType> cancelType_ = {reindexer::CancelType::None};
};

template <typename DBInterface>
class CommandsExecutor {
public:
	struct Status {
		bool running = false;
		Error err;
	};

	template <typename... Args>
	CommandsExecutor(const std::string& outFileName, int numThreads, Args... args)
		: db_(std::move(args)...), output_(outFileName), numThreads_(numThreads) {}
	CommandsExecutor(const CommandsExecutor&) = delete;
	CommandsExecutor(CommandsExecutor&&) = delete;
	CommandsExecutor& operator=(const CommandsExecutor&) = delete;
	CommandsExecutor& operator=(CommandsExecutor&&) = delete;
	~CommandsExecutor() { stop(true); }
	template <typename... Args>
	Error Run(const std::string& dsn, const Args&... args);
	Error GetSuggestions(const std::string& input, std::vector<std::string>& suggestions);
	Error Stop();
	Error Process(const std::string& command);
	Error FromFile(std::istream& in);
	Status GetStatus();
	Error SetDumpMode(const std::string& mode);

protected:
	void setStatus(Status&& status);
	bool isHavingReplicationConfig();
	bool isHavingReplicationConfig(reindexer::WrSerializer& wser, std::string_view type);
	Error fromFileImpl(std::istream& in);
	Error execCommand(IExecutorsCommand& cmd);
	template <typename... Args>
	Error runImpl(const std::string& dsn, Args&&... args);
	template <typename T = DBInterface>
	typename std::enable_if<std::is_default_constructible<T>::value, T>::type createDB() {
		return T();
	}
	template <typename T = DBInterface>
	typename std::enable_if<!std::is_default_constructible<T>::value, T>::type createDB(typename T::ConfigT config) {
		return T(loop_, config);
	}
	std::string getCurrentDsn(bool withPath = false) const;
	Error queryResultsToJson(std::ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery, bool fstream);
	Error getAvailableDatabases(std::vector<std::string>&);

	void addCommandsSuggestions(std::string const& input, std::vector<std::string>& suggestions);
	void checkForNsNameMatch(std::string_view str, std::vector<std::string>& suggestions);
	void checkForCommandNameMatch(std::string_view str, std::initializer_list<std::string_view> cmds,
								  std::vector<std::string>& suggestions);

	Error processImpl(const std::string& command) noexcept;
	void stop(bool terminate);
	Error getSuggestions(const std::string& input, std::vector<std::string>& suggestions);
	Error commandSelect(const std::string& command) noexcept;
	Error commandUpsert(const std::string& command);
	Error commandUpdateSQL(const std::string& command) noexcept;
	Error commandDelete(const std::string& command);
	Error commandDeleteSQL(const std::string& command) noexcept;
	Error commandDump(const std::string& command);
	Error commandNamespaces(const std::string& command);
	Error commandMeta(const std::string& command);
	Error commandHelp(const std::string& command);
	Error commandQuit(const std::string& command);
	Error commandSet(const std::string& command);
	Error commandBench(const std::string& command);
	Error commandProcessDatabases(const std::string& command);

	Error seedBenchItems();
	std::function<void(std::chrono::system_clock::time_point)> getBenchWorkerFn(std::atomic<int>& count, std::atomic<int>& errCount);

	DBInterface db() { return db_.WithContext(&cancelCtx_); }
	DBInterface parametrizedDb() {
		return (!fromFile_ || dumpMode_ == DumpOptions::Mode::ShardedOnly) ? db() : db().WithShardId(ShardingKeyType::ProxyOff, false);
	}
	Error filterNamespacesByDumpMode(std::vector<reindexer::NamespaceDef>& defs, DumpOptions::Mode mode);
	Error getMergedSerialMeta(DBInterface& db, std::string_view nsName, const std::string& key, std::string& result);

	struct commandDefinition {
		std::string command;
		std::string description;
		Error (CommandsExecutor::*handler)(const std::string& command);
		std::string help;
	};
	// clang-format off
	std::vector <commandDefinition> cmds_ = {
		{"select",		"Query to database",&CommandsExecutor::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		SELECT * FROM media_items where name = 'Thor'
		)help"},
		{"delete",		"Delete documents from database",&CommandsExecutor::commandDeleteSQL,R"help(
	Syntax:
		See SQL Delete statement
	Example:
		DELETE FROM media_items where name = 'Thor'
		)help"},
		{"update",		"Update documents in database",&CommandsExecutor::commandUpdateSQL,R"help(
	Syntax:
		See SQL Update statement
	Example:
		UPDATE media_items SET year='2011' where name = 'Thor'
		)help"},
		{"explain",		"Explain query execution plan",&CommandsExecutor::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		EXPLAIN SELECT * FROM media_items where name = 'Thor'
		)help"},
		{"\\upsert",	"Upsert new item to namespace",&CommandsExecutor::commandUpsert,R"help(
	Syntax:
		\upsert <namespace> <document>
	Example:
		\upsert books {"id":5,"name":"xx"}
		)help"},
		{"\\delete",	"Delete item from namespace",&CommandsExecutor::commandDelete,R"help(
	Syntax:
		\delete <namespace> <document>
	Example:
		\delete books {"id":5}
		)help"},
		{"\\dump",		"Dump namespaces",&CommandsExecutor::commandDump,R"help(
	Syntax:
		\dump [namespace1 [namespace2]...]
		)help"},
		{"\\namespaces","Manipulate namespaces",&CommandsExecutor::commandNamespaces,R"help(
	Syntax:
		\namespaces add <name> <definition>
		Add new namespace

		\namespaces list
		List available namespaces

		\namespaces drop <namespace>
		Drop namespace

		\namespaces truncate <namespace>
		Truncate namespace

		\namespaces rename <oldName> <newName>
		Rename namespace
		)help"},
		{"\\meta",		"Manipulate meta",&CommandsExecutor::commandMeta,R"help(
	Syntax:
		\meta put <namespace> <key> <value>
		Put metadata key value

		\meta list
		List all metadata in name
		)help"},
		{"\\set",		"Set configuration variables values",&CommandsExecutor::commandSet,R"help(
	Syntax:
		\set <variable> <value>
		Variable can be one of the following:
			-'output'
				possible values:
					- 'json' Unformatted JSON
					- 'pretty' Pretty printed JSON
					- 'table' Table view
			-'with_shard_id'
				possible values:
					- 'on'  Add '#shard_id' field to items from sharded namespaces
					- 'off'
		)help"},
		{"\\bench",		"Run benchmark",&CommandsExecutor::commandBench,R"help(
	Syntax:
		\bench <time>
		)help"},
		{"\\quit",		"Exit from tool",&CommandsExecutor::commandQuit,""},
		{"\\help",		"Show help",&CommandsExecutor::commandHelp,""},
		{"\\databases", "Works with available databases",&CommandsExecutor::commandProcessDatabases, R"help(
	Syntax:
		 \databases list
		 Shows the list of available databases.

         \databases use <db>
         Switches to one of the existing databases.

         \databases create <db>
         Creates new database.
         )help"}
    };
	// clang-format on

	class URI {
	public:
		bool parse(const std::string& dsn) { return uri_.parse(dsn); }
		const std::string& scheme() const { return uri_.scheme(); }
		const std::string& path() const { return uri_.path(); }
		const std::string& username() const { return uri_.username(); }
		const std::string& password() const { return uri_.password(); }
		const std::string& hostname() const { return uri_.hostname(); }
		const std::string& port() const { return uri_.port(); }
		std::string db() const {
			if (uri_.scheme() == "ucproto") {
				return getDBFromUCproto();
			}
			return uri_.db();
		}
		std::string getDBFromUCproto() const;

	private:
		httpparser::UrlParser uri_;
	};

	reindexer::net::ev::dynamic_loop loop_;
	CancelContext cancelCtx_;
	DBInterface db_;
	Output output_;
	int numThreads_;
	std::unordered_map<std::string, std::string> variables_;
	URI uri_;
	reindexer::net::ev::async cmdAsync_;
	std::mutex mtx_;
	std::condition_variable condVar_;
	Status status_;
	IExecutorsCommand* curCmd_ = nullptr;
	reindexer::coroutine::channel<bool> stopCh_;
	std::thread executorThr_;
	bool fromFile_ = {false};
	bool targetHasReplicationConfig_ = {false};
	std::atomic<DumpOptions::Mode> dumpMode_ = {DumpOptions::Mode::FullNode};
};

}  // namespace reindexer_tool
