#pragma once
#include <unordered_map>
#include "core/namespacedef.h"
#include "core/rdxcontext.h"
#include "dumpoptions.h"
#include "iotools.h"
#include "vendor/urlparser/urlparser.h"

#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

namespace reindexer_tool {

#if REINDEX_WITH_REPLXX
typedef std::function<replxx::Replxx::completions_t(const std::string&, int, void*)> old_v_callback_t;
typedef std::function<replxx::Replxx::completions_t(const std::string& input, int& contextLen)> new_v_callback_t;
#endif	// REINDEX_WITH_REPLXX

class DumpFileIndex;

template <typename DBInterface>
class [[nodiscard]] CommandsProcessor {
public:
	template <typename... Args>
	CommandsProcessor(const std::string& outFileName, const std::string& inFileName, unsigned numThreads, unsigned transactionSize,
					  Args... args)
		: inFileName_(inFileName),
		  output_(outFileName),
		  db_(std::move(args)...),
		  numThreads_(numThreads),
		  transactionSize_(transactionSize) {}

	CommandsProcessor(const CommandsProcessor&) = delete;
	CommandsProcessor(CommandsProcessor&&) = delete;
	CommandsProcessor& operator=(const CommandsProcessor&) = delete;
	CommandsProcessor& operator=(CommandsProcessor&&) = delete;

	template <typename ConnectOpts>
	Error Connect(const std::string& dsn, const ConnectOpts& connectOpts) noexcept;
	Error Run(const std::string& command, const std::string& dumpMode) noexcept;

private:
	Error setDumpMode(const std::string& mode) noexcept;
	Error getSuggestions(const std::string& input, std::vector<std::string>& suggestions) noexcept;
	Error loadVariables() noexcept;
	reindexer::DSN getCurrentDsn(bool withPath = false) const;
	Error queryResultsToJson(std::ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery, bool fstream) noexcept;

	Error commandSelect(std::string_view) noexcept;
	Error commandUpsert(std::string_view) noexcept;
	Error commandUpdateSQL(std::string_view) noexcept;
	Error commandDelete(std::string_view) noexcept;
	Error commandDeleteSQL(std::string_view) noexcept;
	Error commandDump(std::string_view) noexcept;
	Error commandNamespaces(std::string_view) noexcept;
	Error commandMeta(std::string_view) noexcept;
	Error commandHelp(std::string_view) noexcept;
	Error commandVersion(std::string_view) noexcept;
	Error commandQuit(std::string_view) noexcept;
	Error commandSet(std::string_view) noexcept;
	Error commandBench(std::string_view) noexcept;
	Error commandProcessDatabases(std::string_view) noexcept;

	Error parseCommand(const std::string& command, std::string& nsName, std::string& cmdBody, bool& needSkip) noexcept;
	Error parallelUpsertCommands(const std::vector<std::pair<std::string, uint64_t>>& commands) noexcept;

	Error seedBenchItems() noexcept;
	void bench(unsigned int numThreads, int benchTime);

	bool isHavingReplicationConfig();
	bool isHavingReplicationConfig(reindexer::WrSerializer& wser, std::string_view type);

	Error interactive() noexcept;
	Error fromFile(std::istream& in) noexcept;
	Error fromDumpFile(std::ifstream& in, DumpFileIndex& dumpFileIdx) noexcept;

	Error getAvailableDatabases(std::vector<std::string>&) noexcept;

	void addCommandsSuggestions(const std::string& input, std::vector<std::string>& suggestions);
	void checkForNsNameMatch(std::string_view str, std::vector<std::string>& suggestions);
	void checkForCommandNameMatch(std::string_view str, std::initializer_list<std::string_view> cmds,
								  std::vector<std::string>& suggestions);

#if REINDEX_WITH_REPLXX
	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(const new_v_callback_t&));
	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(const old_v_callback_t&, void*));
#endif	// REINDEX_WITH_REPLXX

	Error process(const std::string& command) noexcept;
	Error filterNamespacesByDumpMode(std::vector<reindexer::NamespaceDef>& defs, DumpOptions::Mode mode) noexcept;
	Error getMergedSerialMeta(DBInterface& db, std::string_view nsName, const std::string& key, std::string& result) noexcept;

	struct CommandDefinition;
	static const std::initializer_list<CommandDefinition> cmds_;

	class [[nodiscard]] CancelContext : public reindexer::IRdxCancelContext {
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

	class [[nodiscard]] URI : public httpparser::UrlParser {
	public:
		std::string db() const {
			if (scheme() == "ucproto") {
				std::vector<std::string_view> pathParts;
				reindexer::split(std::string_view(path()), ":", true, pathParts);
				return pathParts.size() >= 2 ? std::string(pathParts.back()) : std::string();
			}
			return httpparser::UrlParser::db();
		}
	};

	DBInterface db() { return db_.WithContext(&cancelCtx_); }
	DBInterface parametrizedDb() {
		return (!fromFile_ || dumpMode_ == DumpOptions::Mode::ShardedOnly) ? db() : db().WithShardId(ShardingKeyType::ProxyOff, false);
	}
	URI uri_;
	std::string inFileName_;
	Output output_;
	std::unordered_map<std::string, std::string> variables_;
	CancelContext cancelCtx_;
	DBInterface db_;
	unsigned numThreads_ = 1;
	unsigned transactionSize_ = 0;
	bool fromFile_ = {false};
	bool quitCmdAccepted_ = {false};

	bool targetHasReplicationConfig_ = {false};
	std::atomic<DumpOptions::Mode> dumpMode_ = {DumpOptions::Mode::FullNode};
};

}  // namespace reindexer_tool
