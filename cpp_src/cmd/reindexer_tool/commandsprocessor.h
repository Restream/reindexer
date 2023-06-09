#pragma once

#include "commandsexecutor.h"

#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

namespace reindexer_tool {

#if REINDEX_WITH_REPLXX
typedef std::function<replxx::Replxx::completions_t(std::string const&, int, void*)> old_v_callback_t;
typedef std::function<replxx::Replxx::completions_t(std::string const& input, int& contextLen)> new_v_callback_t;
#endif	// REINDEX_WITH_REPLXX

template <typename DBInterface>
class CommandsProcessor {
public:
	template <typename... Args>
	CommandsProcessor(const std::string& outFileName, const std::string& inFileName, int numThreads, Args... args)
		: inFileName_(inFileName), executor_(outFileName, numThreads, std::move(args)...) {}
	CommandsProcessor(const CommandsProcessor&) = delete;
	CommandsProcessor(CommandsProcessor&&) = delete;
	CommandsProcessor& operator=(const CommandsProcessor&) = delete;
	CommandsProcessor& operator=(CommandsProcessor&&) = delete;
	~CommandsProcessor();
	template <typename... Args>
	Error Connect(const std::string& dsn, const Args&... args);
	bool Run(const std::string& command, const std::string& dumpMode);

protected:
	bool interactive();
	bool fromFile(std::istream& in);
	Error stop();

#if REINDEX_WITH_REPLXX
	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(new_v_callback_t const&));
	template <typename T>
	void setCompletionCallback(T& rx, void (T::*set_completion_callback)(old_v_callback_t const&, void*));
#endif	// REINDEX_WITH_REPLXX

	Error process(const std::string& command);

	std::string inFileName_;
	CommandsExecutor<DBInterface> executor_;
};

}  // namespace reindexer_tool
