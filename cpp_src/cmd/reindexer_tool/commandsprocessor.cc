#include "commandsprocessor.h"
#include <iomanip>
#include <iostream>
#include "client/cororeindexer.h"
#include "core/reindexer.h"
#include "tableviewscroller.h"
#include "tools/fsops.h"

namespace reindexer_tool {

template <typename DBInterface>
template <typename... Args>
Error CommandsProcessor<DBInterface>::Connect(const std::string& dsn, const Args&... args) {
	return executor_.Run(dsn, args...);
}

template <typename DBInterface>
CommandsProcessor<DBInterface>::~CommandsProcessor() {
	stop();
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::process(const std::string& command) {
	return executor_.Process(command);
}

#if REINDEX_WITH_REPLXX
template <typename DBInterface>
template <typename T>
void CommandsProcessor<DBInterface>::setCompletionCallback(T& rx, void (T::*set_completion_callback)(new_v_callback_t const&)) {
	(rx.*set_completion_callback)([this](std::string const& input, int) -> replxx::Replxx::completions_t {
		std::vector<std::string> completions;
		executor_.GetSuggestions(input, completions);
		replxx::Replxx::completions_t result;
		for (const std::string& suggestion : completions) result.emplace_back(suggestion);
		return result;
	});
}

template <typename DBInterface>
template <typename T>
void CommandsProcessor<DBInterface>::setCompletionCallback(T& rx, void (T::*set_completion_callback)(old_v_callback_t const&, void*)) {
	(rx.*set_completion_callback)(
		[this](std::string const& input, int, void*) -> replxx::Replxx::completions_t {
			std::vector<std::string> completions;
			executor_.GetSuggestions(input, completions);
			return completions;
		},
		nullptr);
}
#endif	// REINDEX_WITH_REPLXX

template <typename T>
class HasSetMaxLineSize {
private:
	typedef char YesType[1], NoType[2];

	template <typename C>
	static YesType& test(decltype(&C::set_max_line_size));
	template <typename C>
	static NoType& test(...);

public:
	enum { value = sizeof(test<T>(0)) == sizeof(YesType) };
};

template <class T>
void setMaxLineSize(T* rx, int arg, typename std::enable_if<HasSetMaxLineSize<T>::value>::type* = 0) {
	return rx->set_max_line_size(arg);
}

void setMaxLineSize(...) {}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::interactive() {
	bool wasError = false;
#if REINDEX_WITH_REPLXX
	replxx::Replxx rx;
	std::string history_file = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), ".reindexer_history.txt");

	setMaxLineSize(&rx, 0x10000);
	rx.history_load(history_file);
	rx.set_max_history_size(1000);
	rx.set_max_hint_rows(8);
	setCompletionCallback(rx, &replxx::Replxx::set_completion_callback);

	std::string prompt = "\x1b[1;32mReindexer\x1b[0m> ";

	// main repl loop
	while (executor_.GetStatus().running) {
		char const* input = nullptr;
		do {
			input = rx.input(prompt);
		} while (!input && errno == EAGAIN);

		if (input == nullptr) break;

		if (!*input) continue;

		Error err = process(input);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}

		rx.history_add(input);
	}
	rx.history_save(history_file);
#else
	std::string prompt = "Reindexer> ";
	// main repl loop
	while (executor_.GetStatus().running) {
		std::string command;
		std::cout << prompt;
		if (!std::getline(std::cin, command)) break;
		Error err = process(command);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}
	}
#endif
	return !wasError;
}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::fromFile(std::istream& infile) {
	return executor_.FromFile(infile).ok();
}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::Run(const std::string& command) {
	if (!command.empty()) {
		auto err = process(command);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			return false;
		}
		return true;
	}

	if (!inFileName_.empty()) {
		std::ifstream infile(inFileName_);
		if (!infile) {
			std::cerr << "ERROR: Can't open " << inFileName_ << std::endl;
			return false;
		}
		return fromFile(infile);
	} else if (reindexer::isStdinRedirected()) {
		return fromFile(std::cin);
	} else {
		return interactive();
	}
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::stop() {
	if (executor_.GetStatus().running) {
		return executor_.Stop();
	}
	return Error();
}

template class CommandsProcessor<reindexer::client::CoroReindexer>;
template class CommandsProcessor<reindexer::Reindexer>;
template Error CommandsProcessor<reindexer::Reindexer>::Connect(const std::string& dsn, const ConnectOpts& opts);
template Error CommandsProcessor<reindexer::client::CoroReindexer>::Connect(const std::string& dsn,
																			const reindexer::client::ConnectOpts& opt);

}  // namespace reindexer_tool
