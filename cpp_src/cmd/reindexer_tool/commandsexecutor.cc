#include "commandsexecutor.h"
#include <iomanip>
#include "client/cororeindexer.h"
#include "core/cjson/jsonbuilder.h"
#include "core/reindexer.h"
#include "coroutine/waitgroup.h"
#include "executorscommand.h"
#include "tableviewscroller.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"

namespace reindexer_tool {

using reindexer::iequals;
using reindexer::WrSerializer;
using reindexer::NamespaceDef;
using reindexer::JsonBuilder;
using reindexer::Query;

const std::string kConfigFile = "rxtool_settings.txt";

const std::string kVariableOutput = "output";
const std::string kOutputModeJson = "json";
const std::string kOutputModeTable = "table";
const std::string kOutputModePretty = "pretty";
const std::string kOutputModePrettyCollapsed = "collapsed";
const std::string kBenchNamespace = "rxtool_bench";
const std::string kBenchIndex = "id";

constexpr int kSingleThreadCoroCount = 200;
constexpr int kBenchItemsCount = 10000;
constexpr int kBenchDefaultTime = 5;
constexpr size_t k24KStack = 24 * 1024;
constexpr size_t k8KStack = 8 * 1024;

template <>
template <typename... Args>
Error CommandsExecutor<reindexer::Reindexer>::Run(const std::string& dsn, const Args&... args) {
	return runImpl(dsn, args...);
}

template <>
template <typename... Args>
Error CommandsExecutor<reindexer::client::CoroReindexer>::Run(const std::string& dsn, const Args&... args) {
	return runImpl(dsn, std::ref(loop_), args...);
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::GetSuggestions(const std::string& input, std::vector<std::string>& suggestions) {
	OutParamCommand<std::vector<std::string>> cmd(
		[this, &input](std::vector<std::string>& suggestions) {
			getSuggestions(input, suggestions);
			return errOK;
		},
		suggestions);
	execCommand(cmd);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::Stop() {
	GenericCommand cmd([this] { return stop(true); });
	auto err = execCommand(cmd);
	if (err.ok() && executorThr_.joinable()) {
		executorThr_.join();
	}
	return err;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::Process(const std::string& command) {
	GenericCommand cmd([this, &command] { return processImpl(command); });
	return execCommand(cmd);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::FromFile(std::istream& in) {
	GenericCommand cmd([this, &in] { return fromFileImpl(in); });
	fromFile_ = true;
	Error status = execCommand(cmd);
	fromFile_ = false;
	return status;
}

template <typename DBInterface>
typename CommandsExecutor<DBInterface>::Status CommandsExecutor<DBInterface>::GetStatus() {
	std::lock_guard<std::mutex> lck(mtx_);
	return status_;
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::setStatus(CommandsExecutor::Status&& status) {
	std::lock_guard<std::mutex> lck(mtx_);
	status_ = std::move(status);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::fromFileImpl(std::istream& in) {
	using reindexer::coroutine::wait_group;
	using reindexer::coroutine::wait_group_guard;

	struct LineData {
		std::string str;
		int64_t lineNum = 1;
	};

	Error lastErr;
	reindexer::coroutine::channel<LineData> cmdCh(500);

	auto handleResultFn = [this, &lastErr](const Error& err, int64_t lineNum) {
		try {
			if (!err.ok()) {
				if (err.code() == errCanceled || !db().Status().ok()) {
					if (lastErr.ok()) {
						lastErr = err;
						std::cerr << "LINE: " << lineNum << " ERROR: " << err.what() << std::endl;
					}
					return false;
				}
				lastErr = err;
				std::cerr << "LINE: " << lineNum << " ERROR: " << err.what() << std::endl;
			}
		} catch (...) {
			std::cout << "exc";
		}

		return true;
	};
	auto workerFn = [this, &cmdCh](const std::function<bool(const Error&, int64_t)>& handleResult, wait_group& wg) {
		wait_group_guard wgg(wg);
		for (;;) {
			auto cmdp = cmdCh.pop();
			if (cmdp.second) {
				auto err = processImpl(cmdp.first.str);
				if (!handleResult(err, cmdp.first.lineNum)) {
					if (cmdCh.opened()) {
						cmdCh.close();
					}
					return;
				}
			} else {
				return;
			}
		}
	};

	wait_group wg;
	wg.add(kSingleThreadCoroCount);
	for (size_t i = 0; i < kSingleThreadCoroCount; ++i) {
		loop_.spawn(std::bind(workerFn, handleResultFn, std::ref(wg)));
	}

	LineData line;
	while (GetStatus().running && std::getline(in, line.str)) {
		if (reindexer::checkIfStartsWith("\\upsert ", line.str) || reindexer::checkIfStartsWith("\\delete ", line.str)) {
			try {
				cmdCh.push(line);
			} catch (std::exception&) {
				break;
			}
		} else {
			auto err = processImpl(line.str);
			if (!handleResultFn(err, line.lineNum)) {
				break;
			}
		}
		line.lineNum++;
	}
	cmdCh.close();
	wg.wait();

	return lastErr;
}

template <typename DBInterface>
reindexer::Error CommandsExecutor<DBInterface>::execCommand(IExecutorsCommand& cmd) {
	std::unique_lock<std::mutex> lck_(mtx_);
	curCmd_ = &cmd;
	cmdAsync_.send();
	condVar_.wait(lck_, [&cmd] { return cmd.IsExecuted(); });
	auto status = cmd.Status();
	lck_.unlock();
	if (!GetStatus().running && status.ok() && executorThr_.joinable()) {
		executorThr_.join();
	}
	return cmd.Status();
}

template <typename DBInterface>
template <typename... Args>
Error CommandsExecutor<DBInterface>::runImpl(const std::string& dsn, Args&&... args) {
	using reindexer::net::ev::sig;
	assertrx(!executorThr_.joinable());

	auto fn = [this](const std::string& dsn, Args&&... args) {
		sig sint;
		sint.set(loop_);
		sint.set([this](sig&) { cancelCtx_.Cancel(); });
		sint.start(SIGINT);

		cmdAsync_.set(loop_);
		cmdAsync_.set([this](reindexer::net::ev::async&) {
			loop_.spawn([this] {
				std::unique_lock<std::mutex> lck(mtx_);
				if (curCmd_) {
					auto cmd = curCmd_;
					curCmd_ = nullptr;
					lck.unlock();
					loop_.spawn([this, cmd] {
						cmd->Execute();
						std::unique_lock<std::mutex> lck(mtx_);
						condVar_.notify_all();
					});
				}
			});
		});
		cmdAsync_.start();

		auto fn = [this](const std::string& dsn, Args&&... args) {
			std::string outputMode;
			Error err;
			if (reindexer::fs::ReadFile(reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile), outputMode) > 0) {
				try {
					gason::JsonParser jsonParser;
					gason::JsonNode value = jsonParser.Parse(reindexer::giftStr(outputMode));
					for (auto node : value) {
						WrSerializer ser;
						reindexer::jsonValueToString(node.value, ser, 0, 0, false);
						variables_[kVariableOutput] = std::string(ser.Slice());
					}
				} catch (const gason::Exception& e) {
					err = Error(errParseJson, "Unable to parse output mode: %s", e.what());
				}
			}
			if (err.ok() && variables_.empty()) {
				variables_[kVariableOutput] = kOutputModeJson;
			}
			if (err.ok() && !uri_.parse(dsn)) {
				err = Error(errNotValid, "Cannot connect to DB: Not a valid uri");
			}
			if (err.ok()) err = db().Connect(dsn, std::forward<Args>(args)...);
			if (err.ok()) {
				loop_.spawn(
					[this] {
						// This coroutine should prevent loop from stopping for core::Reindexer
						stopCh_.pop();
					},
					k8KStack);
			}
			std::lock_guard<std::mutex> lck(mtx_);
			status_.running = err.ok();
			status_.err = std::move(err);
		};
		loop_.spawn(std::bind(fn, std::cref(dsn), std::forward<Args>(args)...));

		loop_.run();
	};

	setStatus(Status());
	executorThr_ = std::thread(std::bind(fn, std::cref(dsn), std::forward<Args>(args)...));
	auto status = GetStatus();
	while (!status.running && status.err.ok()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		status = GetStatus();
	}
	if (!status.err.ok()) {
		executorThr_.join();
		return status.err;
	}

	auto err = output_.Status();
	if (!err.ok()) {
		std::cerr << "Output error: " << err.what() << std::endl;
	}
	return err;
}

template <typename DBInterface>
std::string CommandsExecutor<DBInterface>::getCurrentDsn(bool withPath) const {
	std::string dsn(uri_.scheme() + "://");
	if (!uri_.password().empty() && !uri_.username().empty()) {
		dsn += uri_.username() + ":" + uri_.password() + "@";
	}
	dsn += uri_.hostname() + ":" + uri_.port() + (withPath ? uri_.path() : "/");
	return dsn;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::queryResultsToJson(std::ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery,
														bool fstream) {
	if (cancelCtx_.IsCancelled()) return errOK;
	WrSerializer ser;
	size_t i = 0;
	bool scrollable = !fstream && !reindexer::isStdoutRedirected();
	reindexer::TerminalSize terminalSize;
	if (scrollable) {
		terminalSize = reindexer::getTerminalSize();
		scrollable = (int(r.Count()) > terminalSize.height);
	}
	bool prettyPrint = variables_[kVariableOutput] == kOutputModePretty;
	for (auto it : r) {
		if (auto err = it.Status(); !err.ok()) return err;
		if (cancelCtx_.IsCancelled()) break;
		if (isWALQuery) {
			ser << '#';
			{
				JsonBuilder lsnObj(ser);
				const reindexer::lsn_t lsn(it.GetLSN());
				lsn.GetJSON(lsnObj);
			}
			ser << ' ';
		}
		if (it.IsRaw()) {
			reindexer::WALRecord rec(it.GetRaw());
			try {
				rec.Dump(ser, [this, &r](std::string_view cjson) {
					auto item = db().NewItem(r.GetNamespaces()[0]);
					item.FromCJSONImpl(cjson);
					return std::string(item.GetJSON());
				});
			} catch (Error& err) {
				return std::move(err);
			}
		} else {
			if (isWALQuery) ser << "WalItemUpdate ";

			if (prettyPrint) {
				WrSerializer json;
				Error err = it.GetJSON(json, false);
				if (!err.ok()) return err;
				prettyPrintJSON(reindexer::giftStr(json.Slice()), ser);
			} else {
				Error err = it.GetJSON(ser, false);
				if (!err.ok()) return err;
			}
		}
		if ((++i != r.Count()) && !isWALQuery) ser << ',';
		ser << '\n';
		if ((ser.Len() > 0x100000) || prettyPrint || scrollable) {
			if (scrollable && (i % (terminalSize.height - 1) == 0)) {
				WaitEnterToContinue(o, terminalSize.width, [this]() -> bool { return cancelCtx_.IsCancelled(); });
			}
			o << ser.Slice();
			ser.Reset();
		}
	}
	if (!cancelCtx_.IsCancelled()) {
		o << ser.Slice();
	}
	return errOK;
}

template <>
Error CommandsExecutor<reindexer::client::CoroReindexer>::getAvailableDatabases(std::vector<std::string>& dbList) {
	return db().EnumDatabases(dbList);
}

template <>
Error CommandsExecutor<reindexer::Reindexer>::getAvailableDatabases(std::vector<std::string>&) {
	return Error();
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::addCommandsSuggestions(std::string const& cmd, std::vector<std::string>& suggestions) {
	LineParser parser(cmd);
	std::string_view token = parser.NextToken();

	if ((token == "\\upsert") || (token == "\\delete")) {
		token = parser.NextToken();
		if (parser.End()) {
			checkForNsNameMatch(token, suggestions);
		}
	} else if ((token == "\\dump") && !parser.End()) {
		while (!parser.End()) {
			checkForNsNameMatch(parser.NextToken(), suggestions);
		}
	} else if (token == "\\namespaces") {
		token = parser.NextToken();
		if (token == "drop") {
			checkForNsNameMatch(parser.NextToken(), suggestions);
		} else {
			checkForCommandNameMatch(token, {"add", "list", "drop"}, suggestions);
		}
	} else if (token == "\\meta") {
		checkForCommandNameMatch(parser.NextToken(), {"put", "list"}, suggestions);
	} else if (token == "\\set") {
		token = parser.NextToken();
		if (token == "output") {
			checkForCommandNameMatch(parser.NextToken(), {"json", "pretty", "table"}, suggestions);
		} else {
			checkForCommandNameMatch(token, {"output"}, suggestions);
		}
	} else if (token == "\\subscribe") {
		token = parser.NextToken();
		checkForCommandNameMatch(token, {"on", "off"}, suggestions);
		checkForNsNameMatch(token, suggestions);
	} else if (token == "\\databases") {
		token = parser.NextToken();
		if (token == "use") {
			std::vector<std::string> dbList;
			Error err = getAvailableDatabases(dbList);
			if (err.ok()) {
				token = parser.NextToken();
				for (const std::string& dbName : dbList) {
					if (token.empty() || reindexer::isBlank(token) ||
						((token.length() < dbName.length()) && reindexer::checkIfStartsWith(token, dbName))) {
						suggestions.emplace_back(dbName);
					}
				}
			}
		} else {
			checkForCommandNameMatch(token, {"use", "list"}, suggestions);
		}
	} else {
		for (const commandDefinition& cmdDef : cmds_) {
			if (token.empty() || reindexer::isBlank(token) ||
				((token.length() < cmdDef.command.length()) && reindexer::checkIfStartsWith(token, cmdDef.command))) {
				suggestions.emplace_back(cmdDef.command[0] == '\\' ? cmdDef.command.substr(1) : cmdDef.command);
			}
		}
	}
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::checkForNsNameMatch(std::string_view str, std::vector<std::string>& suggestions) {
	std::vector<NamespaceDef> allNsDefs;
	Error err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
	if (!err.ok()) return;
	for (auto& ns : allNsDefs) {
		if (str.empty() || reindexer::isBlank(str) || ((str.length() < ns.name.length()) && reindexer::checkIfStartsWith(str, ns.name))) {
			suggestions.emplace_back(ns.name);
		}
	}
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::checkForCommandNameMatch(std::string_view str, std::initializer_list<std::string_view> cmds,
															 std::vector<std::string>& suggestions) {
	for (std::string_view cmd : cmds) {
		if (str.empty() || reindexer::isBlank(str) || ((str.length() < cmd.length()) && reindexer::checkIfStartsWith(str, cmd))) {
			suggestions.emplace_back(cmd);
		}
	}
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::processImpl(const std::string& command) noexcept {
	LineParser parser(command);
	auto token = parser.NextToken();

	if (!token.length() || token.substr(0, 2) == "--") return errOK;

	Error ret;
	for (auto& c : cmds_) {
		try {
			if (iequals(token, c.command)) {
				ret = (this->*(c.handler))(command);
				if (cancelCtx_.IsCancelled()) {
					ret = Error(errCanceled, "Canceled");
				}
				cancelCtx_.Reset();
				return ret;
			}
		} catch (Error& e) {
			return e;
		} catch (gason::Exception& e) {
			return Error(errLogic, "JSON exception during command's execution: %s", e.what());
		} catch (std::exception& e) {
			return Error(errLogic, "std::exception during command's execution: %s", e.what());
		} catch (...) {
			return Error(errLogic, "Unknow exception during command's execution");
		}
	}
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token);
}

template <>
Error CommandsExecutor<reindexer::Reindexer>::stop(bool terminate) {
	if (terminate) {
		stopCh_.close();
	}
	return Error();
}

template <>
Error CommandsExecutor<reindexer::client::CoroReindexer>::stop(bool terminate) {
	if (terminate) {
		stopCh_.close();
	}
	return db().Stop();
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::getSuggestions(const std::string& input, std::vector<std::string>& suggestions) {
	if (!input.empty() && input[0] != '\\') db().GetSqlSuggestions(input, input.length() - 1, suggestions);
	if (suggestions.empty()) {
		addCommandsSuggestions(input, suggestions);
	}
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandSelect(const std::string& command) {
	typename DBInterface::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}

	auto err = db().Select(q, results);

	if (err.ok()) {
		if (results.Count()) {
			auto& outputType = variables_[kVariableOutput];
			if (outputType == kOutputModeTable) {
				auto isCanceled = [this]() -> bool { return cancelCtx_.IsCancelled(); };
				reindexer::TableViewBuilder<typename DBInterface::QueryResultsT> tableResultsBuilder(results);
				if (output_.IsCout() && !reindexer::isStdoutRedirected()) {
					TableViewScroller<typename DBInterface::QueryResultsT> resultsScroller(results, tableResultsBuilder,
																						   reindexer::getTerminalSize().height - 1);
					resultsScroller.Scroll(output_, isCanceled);
				} else {
					tableResultsBuilder.Build(output_(), isCanceled);
				}
			} else {
				if (!cancelCtx_.IsCancelled()) {
					output_() << "[" << std::endl;
					err = queryResultsToJson(output_(), results, q.IsWALQuery(), !output_.IsCout());
					output_() << "]" << std::endl;
				}
			}
		}

		const std::string& explain = results.GetExplainResults();
		if (!explain.empty() && !cancelCtx_.IsCancelled()) {
			output_() << "Explain: " << std::endl;
			if (variables_[kVariableOutput] == kOutputModePretty) {
				WrSerializer ser;
				prettyPrintJSON(reindexer::giftStr(explain), ser);
				output_() << ser.Slice() << std::endl;
			} else {
				output_() << explain << std::endl;
			}
		}
		if (!cancelCtx_.IsCancelled()) {
			output_() << "Returned " << results.Count() << " rows";
			if (results.TotalCount()) output_() << ", total count " << results.TotalCount();
			output_() << std::endl;
		}

		auto& aggResults = results.GetAggregationResults();
		if (aggResults.size() && !cancelCtx_.IsCancelled()) {
			output_() << "Aggregations: " << std::endl;
			for (auto& agg : aggResults) {
				switch (agg.type) {
					case AggFacet: {
						assertrx(!agg.fields.empty());
						reindexer::h_vector<int, 1> maxW;
						maxW.reserve(agg.fields.size());
						for (const auto& field : agg.fields) {
							maxW.emplace_back(field.length());
						}
						for (auto& row : agg.facets) {
							assertrx(row.values.size() == agg.fields.size());
							for (size_t i = 0; i < row.values.size(); ++i) {
								maxW.at(i) = std::max(maxW.at(i), int(row.values[i].length()));
							}
						}
						int rowWidth = 8 + (maxW.size() - 1) * 2;
						for (auto& mW : maxW) {
							mW += 3;
							rowWidth += mW;
						}
						for (size_t i = 0; i < agg.fields.size(); ++i) {
							if (i != 0) output_() << "| ";
							output_() << std::left << std::setw(maxW.at(i)) << agg.fields[i];
						}
						output_() << "| count" << std::endl;
						output_() << std::left << std::setw(rowWidth) << std::setfill('-') << "" << std::endl << std::setfill(' ');
						for (auto& row : agg.facets) {
							for (size_t i = 0; i < row.values.size(); ++i) {
								if (i != 0) output_() << "| ";
								output_() << std::left << std::setw(maxW.at(i)) << row.values[i];
							}
							output_() << "| " << row.count << std::endl;
						}
					} break;
					case AggDistinct:
						assertrx(agg.fields.size() == 1);
						output_() << "Distinct (" << agg.fields.front() << ")" << std::endl;
						for (auto& v : agg.distincts) {
							output_() << v.template As<std::string>(agg.payloadType, agg.distinctsFields) << std::endl;
						}
						output_() << "Returned " << agg.distincts.size() << " values" << std::endl;
						break;
					case AggSum:
					case AggAvg:
					case AggMin:
					case AggMax:
					case AggCount:
					case AggCountCached:
					case AggUnknown:
						assertrx(agg.fields.size() == 1);
						output_() << reindexer::AggTypeToStr(agg.type) << "(" << agg.fields.front() << ") = " << agg.GetValueOrZero()
								  << std::endl;
				}
			}
		}
	}
	return err;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandUpsert(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	std::string nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db().NewItem(nsName);

	Error status = item.Status();
	if (!status.ok()) {
		return status;
	}

	status = item.Unsafe().FromJSON(parser.CurPtr());
	if (!status.ok()) {
		return status;
	}

	if (!parser.CurPtr().empty() && (parser.CurPtr())[0] == '[') {
		return Error(errParams, "Impossible to update entire item with array - only objects are allowed");
	}

	status = db().Upsert(nsName, item);
	if (!fromFile_ && status.ok()) {
		output_() << "Upserted successfuly: 1 items" << std::endl;
	}
	return status;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandUpdateSQL(const std::string& command) {
	typename DBInterface::QueryResultsT results;
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}

	auto err = db().Update(q, results);

	if (err.ok()) {
		output_() << "Updated " << results.Count() << " documents" << std::endl;
	}
	return err;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDelete(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db().NewItem(nsName);
	if (!item.Status().ok()) return item.Status();

	auto err = item.Unsafe().FromJSON(parser.CurPtr());
	if (!err.ok()) return err;

	return db().Delete(nsName, item);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDeleteSQL(const std::string& command) {
	typename DBInterface::QueryResultsT results;
	Query q;
	try {
		q.FromSQL(command);
	} catch (const Error& err) {
		return err;
	}
	auto err = db().Delete(q, results);

	if (err.ok()) {
		output_() << "Deleted " << results.Count() << " documents" << std::endl;
	}
	return err;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDump(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	std::vector<NamespaceDef> allNsDefs, doNsDefs;

	auto err = db().WithContext(&cancelCtx_).EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts());
	if (err) return err;

	if (!parser.End()) {
		// build list of namespaces for dumped
		while (!parser.End()) {
			auto ns = parser.NextToken();
			auto nsDef = std::find_if(allNsDefs.begin(), allNsDefs.end(), [&ns](const NamespaceDef& nsDef) { return ns == nsDef.name; });
			if (nsDef != allNsDefs.end()) {
				doNsDefs.emplace_back(std::move(*nsDef));
				allNsDefs.erase(nsDef);
			} else {
				std::cerr << "Namespace '" << ns << "' - skipped. (not found in storage)" << std::endl;
			}
		}
	} else {
		doNsDefs = std::move(allNsDefs);
	}

	reindexer::WrSerializer wrser;

	wrser << "-- Reindexer DB backup file" << '\n';
	wrser << "-- VERSION 1.0" << '\n';

	for (auto& nsDef : doNsDefs) {
		// skip system namespaces, except #config
		if (reindexer::isSystemNamespaceNameFast(nsDef.name) && nsDef.name != "#config") continue;

		wrser << "-- Dumping namespace '" << nsDef.name << "' ..." << '\n';

		wrser << "\\NAMESPACES ADD " << reindexer::escapeString(nsDef.name) << " ";
		nsDef.GetJSON(wrser);
		wrser << '\n';

		std::vector<std::string> meta;
		err = db().WithContext(&cancelCtx_).EnumMeta(nsDef.name, meta);
		if (err) {
			return err;
		}

		for (auto& mkey : meta) {
			std::string mdata;
			err = db().WithContext(&cancelCtx_).GetMeta(nsDef.name, mkey, mdata);
			if (err) {
				return err;
			}

			wrser << "\\META PUT " << reindexer::escapeString(nsDef.name) << " " << reindexer::escapeString(mkey) << " "
				  << reindexer::escapeString(mdata) << '\n';
		}

		typename DBInterface::QueryResultsT itemResults;
		err = db().WithContext(&cancelCtx_).Select(Query(nsDef.name), itemResults);

		if (!err.ok()) return err;

		for (auto it : itemResults) {
			if (auto err = it.Status(); !err.ok()) return err;
			if (cancelCtx_.IsCancelled()) {
				return Error(errCanceled, "Canceled");
			}
			wrser << "\\UPSERT " << reindexer::escapeString(nsDef.name) << ' ';
			it.GetJSON(wrser, false);
			wrser << '\n';
			if (wrser.Len() > 0x100000) {
				output_() << wrser.Slice();
				wrser.Reset();
			}
		}
	}
	output_() << wrser.Slice();

	return errOK;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandNamespaces(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	std::string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "add")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());

		NamespaceDef def("");
		Error err = def.FromJSON(reindexer::giftStr(parser.CurPtr()));
		if (!err.ok()) {
			return Error(errParseJson, "Namespace structure is not valid - %s", err.what());
		}

		def.storage.DropOnFileFormatError(true);
		def.storage.CreateIfMissing(true);

		err = db().OpenNamespace(def.name);
		if (!err.ok()) {
			return err;
		}
		for (auto& idx : def.indexes) {
			err = db().AddIndex(def.name, idx);
			if (!err.ok()) {
				return err;
			}
		}
		err = db().SetSchema(def.name, def.schemaJson);
		if (!err.ok()) {
			return err;
		}
		return errOK;

	} else if (iequals(subCommand, "list")) {
		std::vector<NamespaceDef> allNsDefs;

		auto err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
		for (auto& ns : allNsDefs) {
			output_() << ns.name << std::endl;
		}
		return err;

	} else if (iequals(subCommand, "drop")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		return db().DropNamespace(nsName);
	} else if (iequals(subCommand, "truncate")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		return db().TruncateNamespace(nsName);
	} else if (iequals(subCommand, "rename")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		auto nsNewName = reindexer::unescapeString(parser.NextToken());
		return db().RenameNamespace(nsName, nsNewName);
	}
	return Error(errParams, "Unknown sub command '%s' of namespaces command", subCommand);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandMeta(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();
	std::string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "put")) {
		std::string nsName = reindexer::unescapeString(parser.NextToken());
		std::string metaKey = reindexer::unescapeString(parser.NextToken());
		std::string metaData = reindexer::unescapeString(parser.NextToken());
		return db().PutMeta(nsName, metaKey, metaData);
	} else if (iequals(subCommand, "list")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		std::vector<std::string> allMeta;
		auto err = db().EnumMeta(nsName, allMeta);
		for (auto& metaKey : allMeta) {
			std::string metaData;
			db().GetMeta(nsName, metaKey, metaData);
			output_() << metaKey << " = " << metaData << std::endl;
		}
		return err;
	}
	return Error(errParams, "Unknown sub command '%s' of meta command", subCommand);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandHelp(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();
	std::string_view subCommand = parser.NextToken();

	if (!subCommand.length()) {
		output_() << "Available commands:\n\n";
		for (const auto& cmd : cmds_) {
			output_() << "  " << std::left << std::setw(20) << cmd.command << "- " << cmd.description << std::endl;
		}
	} else {
		auto it = std::find_if(cmds_.begin(), cmds_.end(),
							   [&subCommand](const commandDefinition& def) { return iequals(def.command, subCommand); });

		if (it == cmds_.end()) {
			return Error(errParams, "Unknown command '%s' to help. To list of available command type '\\help'", subCommand);
		}
		output_() << it->command << " - " << it->description << ":" << std::endl << it->help << std::endl;
	}

	return errOK;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandQuit(const std::string&) {
	stop(true);
	setStatus(Status());
	return errOK;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandSet(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	std::string_view variableName = parser.NextToken();
	std::string_view variableValue = parser.NextToken();

	variables_[std::string(variableName)] = std::string(variableValue);

	WrSerializer wrser;
	reindexer::JsonBuilder configBuilder(wrser);
	for (auto it = variables_.begin(); it != variables_.end(); ++it) {
		configBuilder.Put(it->first, it->second);
	}
	configBuilder.End();
	const auto cfgPath = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile);
	const int writeRes = reindexer::fs::WriteFile(cfgPath, wrser.Slice());
	if (writeRes < 0) {
		return Error(errLogic, "Unable to write config file: '%s'", cfgPath);
	}
	return Error();
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandBench(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	int benchTime = reindexer::stoi(parser.NextToken());
	if (benchTime == 0) benchTime = kBenchDefaultTime;

	db().DropNamespace(kBenchNamespace);

	NamespaceDef nsDef(kBenchNamespace);
	nsDef.AddIndex("id", "hash", "int", IndexOpts().PK());

	auto err = db().AddNamespace(nsDef);
	if (!err.ok()) return err;

	output_() << "Seeding " << kBenchItemsCount << " documents to bench namespace..." << std::endl;
	err = seedBenchItems();
	output_() << "done." << std::endl;
	if (!err.ok()) {
		return err;
	}

	output_() << "Running " << benchTime << "s benchmark..." << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(benchTime);
	std::atomic<int> count(0), errCount(0);

	auto worker = std::bind(getBenchWorkerFn(count, errCount), deadline);
	auto threads = std::unique_ptr<std::thread[]>(new std::thread[numThreads_]);
	for (int i = 0; i < numThreads_; i++) threads[i] = std::thread(worker);
	for (int i = 0; i < numThreads_; i++) threads[i].join();

	output_() << "Done. Got " << count / benchTime << " QPS, " << errCount << " errors" << std::endl;
	return err;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandSubscribe(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	reindexer::UpdatesFilters filters;
	auto token = parser.NextToken();
	if (iequals(token, "off")) {
		return db().UnsubscribeUpdates(this);
	} else if (token.empty() || iequals(token, "on")) {
		return db().SubscribeUpdates(this, filters);
	}
	std::vector<std::string> nsInSubscription;
	while (!token.empty()) {
		filters.AddFilter(token, reindexer::UpdatesFilters::Filter());
		nsInSubscription.emplace_back(token);
		token = parser.NextToken();
	}

	auto err = db().SubscribeUpdates(this, filters);
	if (!err.ok()) {
		return err;
	}
	std::vector<NamespaceDef> allNsDefs;
	err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
	if (!err.ok()) {
		return err;
	}
	for (auto& ns : allNsDefs) {
		for (auto it = nsInSubscription.begin(); it != nsInSubscription.end();) {
			if (*it == ns.name) {
				it = nsInSubscription.erase(it);
			} else {
				++it;
			}
		}
	}
	if (!nsInSubscription.empty()) {
		output_() << "WARNING: You have subscribed for non-existing namespace updates: ";
		for (auto it = nsInSubscription.begin(); it != nsInSubscription.end(); ++it) {
			if (it != nsInSubscription.begin()) {
				output_() << ", ";
			}
			output_() << *it;
		}
		output_() << std::endl;
	}
	return errOK;
}

template <>
Error CommandsExecutor<reindexer::client::CoroReindexer>::commandProcessDatabases(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();
	std::string_view subCommand = parser.NextToken();
	assertrx(uri_.scheme() == "cproto");
	if (subCommand == "list") {
		std::vector<std::string> dbList;
		Error err = getAvailableDatabases(dbList);
		if (!err.ok()) return err;
		for (const std::string& dbName : dbList) output_() << dbName << std::endl;
		return Error();
	} else if (subCommand == "use") {
		std::string currentDsn = getCurrentDsn() + std::string(parser.NextToken());
		Error err = stop(false);
		if (!err.ok()) return err;
		err = db().Connect(currentDsn, loop_);
		if (err.ok()) err = db().Status();
		if (err.ok()) output_() << "Succesfully connected to " << currentDsn << std::endl;
		return err;
	} else if (subCommand == "create") {
		auto dbName = parser.NextToken();
		std::string currentDsn = getCurrentDsn() + std::string(dbName);
		Error err = stop(false);
		if (!err.ok()) return err;
		output_() << "Creating database '" << dbName << "'" << std::endl;
		err = db().Connect(currentDsn, loop_, reindexer::client::ConnectOpts().CreateDBIfMissing());
		if (!err.ok()) {
			std::cerr << "Error on database '" << dbName << "' creation" << std::endl;
			return err;
		}
		std::vector<std::string> dbNames;
		err = db().EnumDatabases(dbNames);
		if (std::find(dbNames.begin(), dbNames.end(), std::string(dbName)) != dbNames.end()) {
			output_() << "Succesfully created database '" << dbName << "'" << std::endl;
		} else {
			std::cerr << "Error on database '" << dbName << "' creation" << std::endl;
		}
		return err;
	}
	return Error(errNotValid, "Invalid command");
}

template <>
Error CommandsExecutor<reindexer::Reindexer>::commandProcessDatabases(const std::string& command) {
	(void)command;
	return Error(errNotValid, "Database processing commands are not supported in builtin mode");
}

template <>
Error CommandsExecutor<reindexer::client::CoroReindexer>::seedBenchItems() {
	for (int i = 0; i < kBenchItemsCount; i++) {
		auto item = db().NewItem(kBenchNamespace);
		WrSerializer ser;
		JsonBuilder(ser).Put("id", i).Put("data", i);

		auto err = item.Unsafe().FromJSON(ser.Slice());
		if (!err.ok()) return err;

		err = db().Upsert(kBenchNamespace, item);
		if (!err.ok()) return err;
	}
	return errOK;
}

template <>
Error CommandsExecutor<reindexer::Reindexer>::seedBenchItems() {
	using reindexer::coroutine::wait_group;
	Error err;
	auto upsertFn = [this, &err](size_t beg, size_t end, wait_group& wg) {
		reindexer::coroutine::wait_group_guard wgg(wg);
		for (size_t i = beg; i < end; ++i) {
			auto item = db().NewItem(kBenchNamespace);
			WrSerializer ser;
			JsonBuilder(ser).Put("id", i).Put("data", i);

			auto intErr = item.Unsafe().FromJSON(ser.Slice());
			if (intErr.ok()) intErr = db().Upsert(kBenchNamespace, item);
			if (!intErr.ok()) {
				err = intErr;
				return;
			}
			if (!err.ok()) {
				return;
			}
		}
	};

	auto itemsPerCoro = kBenchItemsCount / kSingleThreadCoroCount;
	wait_group wg;
	wg.add(kSingleThreadCoroCount);
	for (int i = 0; i < kBenchItemsCount; i += itemsPerCoro) {
		loop_.spawn(std::bind(upsertFn, i, std::min(i + itemsPerCoro, kBenchItemsCount), std::ref(wg)), k24KStack);
	}
	wg.wait();
	return err;
}

template <>
std::function<void(std::chrono::system_clock::time_point)> CommandsExecutor<reindexer::client::CoroReindexer>::getBenchWorkerFn(
	std::atomic<int>& count, std::atomic<int>& errCount) {
	using reindexer::coroutine::wait_group;
	return [this, &count, &errCount](std::chrono::system_clock::time_point deadline) {
		reindexer::net::ev::dynamic_loop loop;
		loop.spawn([this, &loop, deadline, &count, &errCount] {
			reindexer::client::CoroReindexer rx;
			const auto dsn = getCurrentDsn(true);
			auto err = rx.Connect(dsn, loop);
			if (!err.ok()) {
				output_() << "[bench] Unable to connect with provided DSN '" << dsn << "': " << err.what() << std::endl;
				rx.Stop();
				return;
			}
			auto selectFn = [&rx, deadline, &count, &errCount](wait_group& wg) {
				reindexer::coroutine::wait_group_guard wgg(wg);
				for (; std::chrono::system_clock::now() < deadline; ++count) {
					Query q(kBenchNamespace);
					q.Where(kBenchIndex, CondEq, count % kBenchItemsCount);
					reindexer::client::CoroReindexer::QueryResultsT results;
					auto err = rx.Select(q, results);
					if (!err.ok()) errCount++;
				}
			};
			wait_group wg;
			wg.add(kSingleThreadCoroCount);
			for (int i = 0; i < kSingleThreadCoroCount; ++i) {
				loop.spawn(std::bind(selectFn, std::ref(wg)), k24KStack);
			}
			wg.wait();
			rx.Stop();
		});

		loop.run();
	};
}

template <>
std::function<void(std::chrono::system_clock::time_point)> CommandsExecutor<reindexer::Reindexer>::getBenchWorkerFn(
	std::atomic<int>& count, std::atomic<int>& errCount) {
	return [this, &count, &errCount](std::chrono::system_clock::time_point deadline) {
		for (; (count % 1000) || std::chrono::system_clock::now() < deadline; count++) {
			Query q(kBenchNamespace);
			q.Where(kBenchIndex, CondEq, count % kBenchItemsCount);
			auto results = new typename reindexer::Reindexer::QueryResultsT;

			db().WithCompletion([results, &errCount](const Error& err) {
					delete results;
					if (!err.ok()) errCount++;
				})
				.Select(q, *results);
		}
	};
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::OnWALUpdate(reindexer::LSNPair LSNs, std::string_view nsName, const reindexer::WALRecord& wrec) {
	WrSerializer ser;
	ser << "# LSN " << int64_t(LSNs.upstreamLSN_) << " originLSN " << int64_t(LSNs.originLSN_) << " " << nsName << " ";
	try {
		wrec.Dump(ser, [this, nsName](std::string_view cjson) {
			auto item = db().NewItem(nsName);
			item.FromCJSONImpl(cjson);
			return std::string(item.GetJSON());
		});
	} catch (const Error& err) {
		std::cerr << "[OnWALUpdate] " << err.what() << std::endl;
		return;
	}
	output_() << ser.Slice() << std::endl;
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::OnConnectionState(const Error& err) {
	if (err.ok())
		output_() << "[OnConnectionState] connected" << std::endl;
	else
		output_() << "[OnConnectionState] closed, reason: " << err.what() << std::endl;
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::OnUpdatesLost(std::string_view nsName) {
	output_() << "[OnUpdatesLost] " << nsName << std::endl;
}

template class CommandsExecutor<reindexer::client::CoroReindexer>;
template class CommandsExecutor<reindexer::Reindexer>;
template Error CommandsExecutor<reindexer::Reindexer>::Run(const std::string& dsn, const ConnectOpts& opts);
template Error CommandsExecutor<reindexer::client::CoroReindexer>::Run(const std::string& dsn, const reindexer::client::ConnectOpts& opts);

}  // namespace reindexer_tool
