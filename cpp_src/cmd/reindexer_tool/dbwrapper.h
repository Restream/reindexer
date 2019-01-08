#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "iotools.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"

namespace reindexer_tool {

using std::vector;
using std::string;
using std::unordered_map;
using reindexer::Error;

template <typename _DB>
class DBWrapper : public reindexer::IUpdatesObserver {
public:
	template <typename... Args>
	DBWrapper(const string& outFileName, const string& inFileName, const string& command, int connPoolSize, int numThreads, Args... args)
		: db_(args...),
		  output_(outFileName),
		  fileName_(inFileName),
		  command_(command),
		  connPoolSize_(connPoolSize),
		  numThreads_(numThreads) {}
	~DBWrapper();
	Error Connect(const string& dsn);
	bool Run();

protected:
	bool Interactive();
	bool FromFile();
	Error queryResultsToJson(ostream& o, const typename _DB::QueryResultsT& r, bool isWALQuery);

	Error ProcessCommand(string command);
	Error commandSelect(const string& command);
	Error commandUpsert(const string& command);
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

	void OnWALUpdate(int64_t lsn, string_view nsName, const reindexer::WALRecord& wrec) override final;
	void OnConnectionState(const Error& err) override;

	struct commandDefinition {
		string command;
		string description;
		Error (DBWrapper::*handler)(const string& command);
		string help;
	};
	// clang-format off
	std::vector <commandDefinition> cmds_ = {
		{"select",		"Query to database",&DBWrapper::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		SELECT * FROM media_items where name = 'Thor'
		)help"},
		{"delete",		"Delete documents from database",&DBWrapper::commandDeleteSQL,R"help(
	Syntax:
		See SQL Delete statement
	Example:
		DELETE FROM media_items where name = 'Thor'
		)help"},
		{"explain",		"Excplain query execution plan",&DBWrapper::commandSelect,R"help(
	Syntax:
		See SQL Select statement
	Example:
		EXPLAIN SELECT * FROM media_items where name = 'Thor'
		)help"},
		{"\\upsert",	"Upsert new item to namespace",&DBWrapper::commandUpsert,R"help( 
	Syntax:
		\upsert <namespace> <document>
	Example:
		\upsert books {"id":5,"name":"xx"}
		)help"},
		{"\\delete",	"Delete item from namespace",&DBWrapper::commandDelete,R"help(
	Syntax:
		\delete <namespace> <document>
	Example:
		\delete books {"id":5}
		)help"},
		{"\\dump",		"Dump namespaces",&DBWrapper::commandDump,R"help(
	Syntax:
		\dump [namespace1 [namespace2]...]
		)help"},
		{"\\namespaces","Manipulate namespaces",&DBWrapper::commandNamespaces,R"help(
	Syntax:
		\namespaces add <name> <definition>
		Add new namespace

		\namespaces list 
		List available namespaces

		\namespaces drop <namespace>
		Drop namespace
		)help"},
		{"\\meta",		"Manipulate meta",&DBWrapper::commandMeta,R"help(
	Syntax:
		\meta put <namespace> <key> <value>
		Put metadata key value

		\meta list
		List all metadata in name
		)help"},
		{"\\set",		"Set configuration variables values",&DBWrapper::commandSet,R"help(
	Syntax:
		\set output <format>
		Format can be one of the following:
		- 'json' Unformatted JSON
		- 'pretty' Pretty printed JSON
		)help"},
		{"\\bench",		"Run benchmark",&DBWrapper::commandBench,R"help(
	Syntax:
		\bench <time>
		)help"},
		{"\\subscribe",	"Subscribe to upstream updates",&DBWrapper::commandSubscribe,R"help(
	Syntax:
		\subscribe <on|off>
		)help"},
		{"\\quit",		"Exit from tool",&DBWrapper::commandQuit,""},
		{"\\help",		"Show help",&DBWrapper::commandHelp,""}
	};
	// clang-format on

	_DB db_;
	Output output_;
	string fileName_;
	string command_;
	int connPoolSize_;
	int numThreads_;
	bool terminate_ = false;
	unordered_map<string, string> variables_;
};

}  // namespace reindexer_tool
