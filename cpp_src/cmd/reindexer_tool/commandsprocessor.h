#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "core/cancelcontextpool.h"
#include "iotools.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "vendor/urlparser/urlparser.h"

#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

namespace reindexer_tool {

typedef std::function<replxx::Replxx::completions_t(std::string const&, int, void*)> old_v_callback_t;
typedef std::function<replxx::Replxx::completions_t(std::string const& input, int& contextLen)> new_v_callback_t;

using std::vector;
using std::string;
using std::unordered_map;
using reindexer::Error;

template <typename DBInterface>
class CommandsProcessor : public reindexer::IUpdatesObserver {
public:
	template <typename... Args>
	CommandsProcessor(const string& outFileName, const string& inFileName, const string& command, int connPoolSize, int numThreads,
					  Args... args)
		: dbNoCtx_(args...),
		  db_(dbNoCtx_.WithContext(&cancelCtx_)),
		  output_(outFileName),
		  outFileName_(outFileName),
		  inFileName_(inFileName),
		  command_(command),
		  connPoolSize_(connPoolSize),
		  numThreads_(numThreads) {
		cancelCtx_.Reset();
	}
	CommandsProcessor(const CommandsProcessor&) = delete;
	CommandsProcessor(const CommandsProcessor&&) = delete;
	CommandsProcessor& operator=(const CommandsProcessor&) = delete;
	CommandsProcessor& operator=(const CommandsProcessor&&) = delete;
	~CommandsProcessor();
	Error Connect(const string& dsn);
	bool Run();

protected:
	bool Interactive();
	bool FromFile();
	string getCurrentDsn() const;
	Error queryResultsToJson(ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery);
	Error getAvailableDatabases(vector<string>&);
	Error stop();
	void onSigInt(int);

	void addCommandsSuggestions(std::string const& input, std::vector<string>& suggestions);
	void checkForNsNameMatch(string_view str, std::vector<string>& suggestions);
	void checkForCommandNameMatch(string_view str, std::initializer_list<string_view> cmds, std::vector<string>& suggestions);

	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(new_v_callback_t const&));
	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(old_v_callback_t const&, void*));

	Error Process(string command);
	Error commandSelect(const string& command);
	Error commandUpsert(const string& command);
	Error commandUpdateSQL(const string& command);
	Error commandDelete(const string& command);
	Error commandDeleteSQL(const string& command);
	Error commandDump(const string& command);
	Error commandNamespaces(const string& command);
	Error commandMeta(const string& command);
	Error commandHelp(const string& command);
	Error commandQuit(const string& command);
	Error commandSet(const string& command);
	Error commandBench(const string& command);
	Error commandSubscribe(const string& command);
	Error commandProcessDatabases(const string& command);

	void OnWALUpdate(int64_t lsn, string_view nsName, const reindexer::WALRecord& wrec) override final;
	void OnConnectionState(const Error& err) override;

	struct commandDefinition {
		string command;
		string description;
		Error (CommandsProcessor::*handler)(const string& command);
		string help;
	};
	// clang-format off
	std::vector <commandDefinition> cmds_ = {
        {"select",		"Query to database",&CommandsProcessor::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		SELECT * FROM media_items where name = 'Thor'
		)help"},
        {"delete",		"Delete documents from database",&CommandsProcessor::commandDeleteSQL,R"help(
	Syntax:
		See SQL Delete statement
	Example:
		DELETE FROM media_items where name = 'Thor'
		)help"},
        {"update",		"Update documents in database",&CommandsProcessor::commandUpdateSQL,R"help(
	Syntax:
		See SQL Update statement
	Example:
		UPDATE media_items SET year='2011' where name = 'Thor'
		)help"},
        {"explain",		"Explain query execution plan",&CommandsProcessor::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		EXPLAIN SELECT * FROM media_items where name = 'Thor'
		)help"},
        {"\\upsert",	"Upsert new item to namespace",&CommandsProcessor::commandUpsert,R"help(
	Syntax:
		\upsert <namespace> <document>
	Example:
		\upsert books {"id":5,"name":"xx"}
		)help"},
        {"\\delete",	"Delete item from namespace",&CommandsProcessor::commandDelete,R"help(
	Syntax:
		\delete <namespace> <document>
	Example:
		\delete books {"id":5}
		)help"},
        {"\\dump",		"Dump namespaces",&CommandsProcessor::commandDump,R"help(
	Syntax:
		\dump [namespace1 [namespace2]...]
		)help"},
        {"\\namespaces","Manipulate namespaces",&CommandsProcessor::commandNamespaces,R"help(
	Syntax:
		\namespaces add <name> <definition>
		Add new namespace

		\namespaces list 
		List available namespaces

		\namespaces drop <namespace>
		Drop namespace
		)help"},
        {"\\meta",		"Manipulate meta",&CommandsProcessor::commandMeta,R"help(
	Syntax:
		\meta put <namespace> <key> <value>
		Put metadata key value

		\meta list
		List all metadata in name
		)help"},
        {"\\set",		"Set configuration variables values",&CommandsProcessor::commandSet,R"help(
	Syntax:
		\set output <format>
		Format can be one of the following:
		- 'json' Unformatted JSON
		- 'pretty' Pretty printed JSON
        - 'table' Table view
		)help"},
        {"\\bench",		"Run benchmark",&CommandsProcessor::commandBench,R"help(
	Syntax:
		\bench <time>
		)help"},
        {"\\subscribe",	"Subscribe to upstream updates",&CommandsProcessor::commandSubscribe,R"help(
	Syntax:
		\subscribe <on|off>
		)help"},
        {"\\quit",		"Exit from tool",&CommandsProcessor::commandQuit,""},
        {"\\help",		"Show help",&CommandsProcessor::commandHelp,""},
        {"\\databases", "Works with available databases",&CommandsProcessor::commandProcessDatabases, R"help(
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

	DBInterface dbNoCtx_;
	reindexer::CancelContextImpl cancelCtx_;
	DBInterface db_;
	Output output_;
	string outFileName_;
	string inFileName_;
	string command_;
	int connPoolSize_;
	int numThreads_;
	bool terminate_ = false;
	unordered_map<string, string> variables_;
	httpparser::UrlParser uri_;
};

}  // namespace reindexer_tool
