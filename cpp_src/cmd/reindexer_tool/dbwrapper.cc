#include "dbwrapper.h"
#include <iomanip>
#include <thread>
#include "client/reindexer.h"
#include "core/reindexer.h"
#if REINDEX_WITH_REPLXX
#include "replxx.hxx"
#endif

#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

namespace reindexer_tool {

using reindexer::iequals;
using reindexer::WrSerializer;

const string kVariableOutput = "output";
const string kOutputModeJson = "json";
const string kOutputModePretty = "pretty";
const string kOutputModePrettyCollapsed = "collapsed";
const string kOutputModeTable = "table";

template <typename _DB>
Error DBWrapper<_DB>::Connect(const string& dsn) {
	variables_[kVariableOutput] = kOutputModePrettyCollapsed;
	return db_.Connect(dsn);
}

template <typename _DB>
Error DBWrapper<_DB>::ProcessCommand(string command) {
	LineParser parser(command);
	auto token = parser.NextToken();

	if (!token.length() || token.substr(0, 2) == "--") return errOK;

	for (auto& c : cmds_) {
		if (reindexer::iequals(token, c.command)) return (this->*(c.handler))(command);
	}
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandSelect(const string& command) {
	typename _DB::QueryResultsT results;
	auto err = db_.Select(command, results);

	if (err.ok()) {
		if (results.Count()) {
			output_() << "[" << std::endl;
			err = queryResultsToJson(output_(), results);
			output_() << "]" << std::endl;
		}

		string explain = results.GetExplainResults();
		if (!explain.empty()) {
			output_() << "Explain: " << std::endl;
			if (variables_[kVariableOutput] == kOutputModePretty) {
				WrSerializer ser;
				prettyPrintJSON(explain, ser);
				output_() << ser.Slice() << std::endl;
			} else {
				output_() << explain << std::endl;
			}
		}
		output_() << "Returned " << results.Count() << " rows";
		if (results.TotalCount()) output_() << ", total count " << results.TotalCount();
		output_() << std::endl;

		auto& aggResults = results.GetAggregationResults();
		if (aggResults.size()) {
			output_() << "Aggregations: " << std::endl;
			for (auto& agg : aggResults) {
				int maxW = agg.name.length();
				for (auto& row : agg.facets) {
					maxW = std::max(maxW, int(row.value.length()));
				}
				maxW += 3;
				output_() << std::left << std::setw(maxW) << agg.name << "| count" << std::endl;
				output_() << std::left << std::setw(maxW + 8) << std::setfill('-') << "" << std::endl << std::setfill(' ');
				for (auto& row : agg.facets) {
					output_() << std::left << std::setw(maxW) << row.value << "| " << row.count << std::endl;
				}
			}
		}
	}
	return err;
}

template <typename _DB>
Error DBWrapper<_DB>::commandUpsert(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = unescapeName(parser.NextToken());

	auto item = db_.NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(const_cast<char*>(parser.CurPtr()));
	if (!err.ok()) return err;

	return db_.Upsert(nsName, item);
}

template <typename _DB>
Error DBWrapper<_DB>::commandDelete(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = unescapeName(parser.NextToken());

	auto item = db_.NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(const_cast<char*>(parser.CurPtr()));
	if (!err.ok()) return err;

	return db_.Delete(nsName, item);
}

template <typename _DB>
Error DBWrapper<_DB>::commandDump(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	vector<reindexer::NamespaceDef> allNsDefs, doNsDefs;

	auto err = db_.EnumNamespaces(allNsDefs, false);
	if (err) return err;

	if (!parser.End()) {
		// build list of namespaces for dumped
		while (!parser.End()) {
			auto ns = parser.NextToken();
			auto nsDef =
				std::find_if(allNsDefs.begin(), allNsDefs.end(), [&ns](const reindexer::NamespaceDef& nsDef) { return ns == nsDef.name; });
			if (nsDef != allNsDefs.end()) {
				doNsDefs.push_back(std::move(*nsDef));
				allNsDefs.erase(nsDef);
			} else {
				std::cerr << "Namespace '" << ns << "' - skipped. (not found in storage)" << std::endl;
			}
		}
	} else {
		doNsDefs = std::move(allNsDefs);
	}

	auto& file = output_();

	file << "-- Reindexer DB backup file" << std::endl;
	file << "-- VERSION 1.0" << std::endl;

	reindexer::WrSerializer wrser;
	for (auto& nsDef : doNsDefs) {
		// skip system namespaces
		if (nsDef.name.length() > 0 && nsDef.name[0] == '#') continue;

		file << "-- Dumping namespace '" << nsDef.name << "' ..." << std::endl;

		wrser.Reset();
		nsDef.GetJSON(wrser);
		file << "\\NAMESPACES ADD " << escapeName(nsDef.name) << " " << wrser.Slice() << std::endl;

		vector<string> meta;
		err = db_.EnumMeta(nsDef.name, meta);
		if (err) return err;

		for (auto& mkey : meta) {
			string mdata;
			err = db_.GetMeta(nsDef.name, mkey, mdata);
			if (err) return err;

			file << "\\META PUT " << escapeName(nsDef.name) << " " << escapeName(mkey) << " " << escapeName(mdata) << std::endl;
		}

		typename _DB::QueryResultsT itemResults;
		err = db_.Select(reindexer::Query(nsDef.name), itemResults);

		if (!err.ok()) return err;

		for (auto it : itemResults) {
			wrser.Reset();
			if (!it.Status().ok()) return it.Status();

			it.GetJSON(wrser, false);
			file << "\\UPSERT " << escapeName(nsDef.name) << " " << wrser.Slice() << "\n";
		}
	}

	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandNamespaces(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "add")) {
		auto nsName = unescapeName(parser.NextToken());

		reindexer::NamespaceDef def("");
		reindexer::Error err = def.FromJSON(const_cast<char*>(parser.CurPtr()));
		if (!err.ok()) {
			return Error(errParseJson, "Namespace structure is not valid - %s", err.what().c_str());
		}

		def.storage.DropOnFileFormatError(true);
		def.storage.CreateIfMissing(true);

		return db_.AddNamespace(def);
	} else if (iequals(subCommand, "list")) {
		vector<reindexer::NamespaceDef> allNsDefs;

		auto err = db_.EnumNamespaces(allNsDefs, true);
		for (auto& ns : allNsDefs) {
			std::cout << ns.name << std::endl;
		}
		return err;

	} else if (iequals(subCommand, "drop")) {
		auto nsName = unescapeName(parser.NextToken());
		return db_.DropNamespace(nsName);
	}
	return Error(errParams, "Unknown sub command '%s' of namespaces command", subCommand.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandMeta(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "put")) {
		string nsName = unescapeName(parser.NextToken());
		string metaKey = unescapeName(parser.NextToken());
		string metaData = unescapeName(parser.NextToken());
		return db_.PutMeta(nsName, metaKey, metaData);
	} else if (iequals(subCommand, "list")) {
		auto nsName = unescapeName(parser.NextToken());
		vector<std::string> allMeta;
		auto err = db_.EnumMeta(nsName, allMeta);
		for (auto& metaKey : allMeta) {
			string metaData;
			db_.GetMeta(nsName, metaKey, metaData);
			std::cout << metaKey << " = " << metaData << std::endl;
		}
		return err;
	}
	return Error(errParams, "Unknown sub command '%s' of meta command", subCommand.ToString().c_str());
}

template <typename _DB>
Error DBWrapper<_DB>::commandHelp(const string& command) {
	LineParser parser(command);
	parser.NextToken();
	string_view subCommand = parser.NextToken();

	if (!subCommand.length()) {
		std::cout << "Available commands:\n\n";
		for (auto cmd : cmds_) {
			std::cout << "  " << std::left << std::setw(20) << cmd.command << "- " << cmd.description << std::endl;
		}
	} else {
		auto it = std::find_if(cmds_.begin(), cmds_.end(),
							   [&subCommand](const commandDefinition& def) { return iequals(def.command, subCommand); });

		if (it == cmds_.end()) {
			return Error(errParams, "Unknown command '%s' to help. To list of available command type '\\help'",
						 subCommand.ToString().c_str());
		}
		std::cout << it->command << " - " << it->description << ":" << std::endl << it->help << std::endl;
	}

	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandQuit(const string&) {
	terminate_ = true;
	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::commandSet(const string& command) {
	LineParser parser(command);
	parser.NextToken();

	string_view variableName = parser.NextToken();
	string_view variableValue = parser.NextToken();

	variables_[variableName.ToString()] = variableValue.ToString();
	return errOK;
}

template <typename _DB>
Error DBWrapper<_DB>::queryResultsToJson(ostream& o, const typename _DB::QueryResultsT& r) {
	WrSerializer ser;
	size_t i = 0;
	for (auto it : r) {
		ser.Reset();
		Error err = it.GetJSON(ser, false);
		if (!err.ok()) return err;

		if (variables_[kVariableOutput] == kOutputModePretty) {
			string json = ser.Slice().ToString();
			ser.Reset();
			prettyPrintJSON(json, ser);
		}
		o << ser.Slice() << (++i == r.Count() ? "\n" : ",\n");
	}
	return errOK;
}

template <typename _DB>
bool DBWrapper<_DB>::Interactive() {
	bool wasError = false;
#if REINDEX_WITH_REPLXX
	replxx::Replxx rx;
	std::string history_file = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), ".reindexer_history.txt");

	rx.history_load(history_file);
	rx.set_max_history_size(1000);
	rx.set_max_line_size(16384);
	rx.set_max_hint_rows(8);
	std::string prompt = "\x1b[1;32mReindexer\x1b[0m> ";

	// main repl loop
	while (!terminate_) {
		char const* input = nullptr;
		do {
			input = rx.input(prompt);
		} while (!input && errno == EAGAIN);

		if (input == nullptr) break;

		if (!*input) continue;

		Error err = ProcessCommand(input);
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
	while (!terminate_) {
		std::string command;
		std::cout << prompt;
		if (!std::getline(std::cin, command)) break;
		Error err = ProcessCommand(command);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}
	}
#endif
	return !wasError;
}

template <typename _DB>
bool DBWrapper<_DB>::FromFile() {
	bool wasError = false;
	std::ifstream infile(fileName_);
	if (!infile) {
		std::cerr << "ERROR: Can't open " << fileName_ << std::endl;
		return false;
	}

	std::string line;
	while (std::getline(infile, line)) {
		Error err = ProcessCommand(line);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			wasError = true;
		}
	}
	return !wasError;
}

template <typename _DB>
bool DBWrapper<_DB>::Run() {
	if (!command_.empty()) {
		Error err = ProcessCommand(command_);
		if (!err.ok()) {
			std::cerr << "ERROR: " << err.what() << std::endl;
			return false;
		}
		return true;
	}
	if (!fileName_.empty()) {
		return FromFile();
	} else {
		return Interactive();
	}
}

template class DBWrapper<reindexer::client::Reindexer>;
template class DBWrapper<reindexer::Reindexer>;

}  // namespace reindexer_tool
