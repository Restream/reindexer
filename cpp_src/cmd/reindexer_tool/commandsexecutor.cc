#include "commandsexecutor.h"
#include <iomanip>
#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "core/cjson/jsonbuilder.h"
#include "core/reindexer.h"
#include "coroutine/waitgroup.h"
#include "executorscommand.h"
#include "tableviewscroller.h"
#include "tools/catch_and_return.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/stringstools.h"
#include "wal/walrecord.h"

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
const std::string kVariableWithShardId = "with_shard_id";
const std::string kBenchNamespace = "rxtool_bench";
const std::string kBenchIndex = "id";
const std::string kDumpModePrefix = "-- __dump_mode:";

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
Error CommandsExecutor<DBInterface>::GetSuggestions(const std::string& input, std::vector<std::string>& suggestions) {
	OutParamCommand<std::vector<std::string>> cmd(
		[this, &input](std::vector<std::string>& suggestions) { return getSuggestions(input, suggestions); }, suggestions);
	return execCommand(cmd);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::Stop() {
	Error err;
	if (GetStatus().running) {
		GenericCommand cmd([this] {
			stop(true);
			return Error{};
		});
		err = execCommand(cmd);
	}
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
reindexer::Error CommandsExecutor<DBInterface>::SetDumpMode(const std::string& mode) {
	try {
		dumpMode_ = DumpOptions::ModeFromStr(mode);
	} catch (Error& err) {
		return err;
	}
	return Error();
}

template <typename DBInterface>
void CommandsExecutor<DBInterface>::setStatus(CommandsExecutor::Status&& status) {
	std::lock_guard<std::mutex> lck(mtx_);
	status_ = std::move(status);
}

template <typename DBInterface>
bool CommandsExecutor<DBInterface>::isHavingReplicationConfig() {
	using namespace std::string_view_literals;
	if (uri_.db().empty()) {
		return false;
	}
	WrSerializer wser;
	if (isHavingReplicationConfig(wser, "cluster"sv)) {
		return true;
	}
	if (isHavingReplicationConfig(wser, "async"sv)) {
		return true;
	}
	return false;
}

template <typename DBInterface>
bool CommandsExecutor<DBInterface>::isHavingReplicationConfig(WrSerializer& wser, std::string_view type) {
	try {
		Query q;
		typename DBInterface::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);

		auto err = db().Select(Query("#replicationstats").Where("type", CondEq, type), results);
		if (!err.ok()) {
			throw err;
		}

		if (results.Count() == 1) {
			wser.Reset();
			err = results.begin().GetJSON(wser, false);
			if (!err.ok()) {
				throw err;
			}

			gason::JsonParser parser;
			auto root = parser.Parse(reindexer::giftStr(wser.Slice()));
			const auto& nodesArray = root["nodes"];
			if (gason::begin(nodesArray) != gason::end(nodesArray)) {
				return true;
			}
		}
	} catch (const gason::Exception&) {
		std::cerr << "Gason exception" << std::endl;
	} catch (const Error& err) {
		std::cerr << "Error ex: " << err.what() << std::endl;
	}
	return false;
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

	targetHasReplicationConfig_ = isHavingReplicationConfig();
	if (targetHasReplicationConfig_) {
		output_() << "Target DB has configured replication, so corresponding configs will not be overriden" << std::endl;
	}

	wait_group wg;
	wg.add(kSingleThreadCoroCount);
	for (size_t i = 0; i < kSingleThreadCoroCount; ++i) {
		loop_.spawn(std::bind(workerFn, handleResultFn, std::ref(wg)));
	}

	LineData line;
	while (GetStatus().running && std::getline(in, line.str)) {
		if (reindexer::checkIfStartsWith("\\upsert ", line.str) || reindexer::checkIfStartsWith("\\delete ", line.str)) {
			try {
				LineData l;
				l.lineNum = line.lineNum;
				reindexer::deepCopy(l.str, line.str);
				cmdCh.push(std::move(l));
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
	targetHasReplicationConfig_ = false;

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

	auto fnLoop = [this](const std::string& dsn, Args&&... args) {
		Error err;
		std::string config;
		if (reindexer::fs::ReadFile(reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile), config) > 0) {
			try {
				gason::JsonParser jsonParser;
				gason::JsonNode value = jsonParser.Parse(reindexer::giftStr(config));
				for (auto node : value) {
					WrSerializer ser;
					reindexer::jsonValueToString(node.value, ser, 0, 0, false);
					variables_[kVariableOutput] = std::string(ser.Slice());
				}
			} catch (const gason::Exception& e) {
				err = Error(errParseJson, "Unable to parse output mode: %s", e.what());
			}
		}
		if (err.ok() && (variables_.empty() || variables_.find(kVariableOutput) == variables_.end())) {
			variables_[kVariableOutput] = kOutputModeJson;
		}
		if (err.ok() && !uri_.parse(dsn)) {
			err = Error(errNotValid, "Cannot connect to DB: Not a valid uri");
		}
		if (err.ok()) {
			err = db().Connect(dsn, std::forward<Args>(args)...);
		}
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

	auto fnThread = [this, &fnLoop](const std::string& dsn, Args&&... args) {
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
		loop_.spawn(std::bind(fnLoop, std::cref(dsn), std::forward<Args>(args)...));
		loop_.run();
	};

	setStatus(Status());
	executorThr_ = std::thread(std::bind(fnThread, std::cref(dsn), std::forward<Args>(args)...));
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
reindexer::DSN CommandsExecutor<DBInterface>::getCurrentDsn(bool withPath) const {
	using namespace std::string_view_literals;
	std::string dsn(uri_.scheme() + "://");
	if (!uri_.password().empty() && !uri_.username().empty()) {
		dsn += uri_.username() + ':' + uri_.password() + '@';
	}
	if (uri_.scheme() == "ucproto"sv) {
		std::vector<std::string_view> pathParts;
		reindexer::split(std::string_view(uri_.path()), ":", true, pathParts);
		std::string_view dbName;
		if (pathParts.size() >= 2) {
			dbName = pathParts.back().substr(1);  // ignore '/' in dbName
		}

		// after URI-parsing uri_.path() looks like e.g. /tmp/reindexer.sock:/db or /tmp/reindexer.sock
		// and hostname-, port- fields are empty
		dsn += uri_.path().substr(0, uri_.path().size() - (withPath ? 0 : dbName.size())) + (dbName.empty() ? ":/" : "");
	} else {
		dsn += uri_.hostname() + ':' + uri_.port() + (withPath ? uri_.path() : "/");
	}
	return reindexer::DSN(dsn);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::queryResultsToJson(std::ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery,
														bool fstream) {
	if (cancelCtx_.IsCancelled()) {
		return errOK;
	}
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
		if (auto err = it.Status(); !err.ok()) {
			return err;
		}
		if (cancelCtx_.IsCancelled()) {
			break;
		}
		if (isWALQuery) {
			ser << '#';
			{
				JsonBuilder lsnObj(ser);
				it.GetLSN().GetJSON(lsnObj);
			}
			ser << ' ';
		}
		if (isWALQuery && it.IsRaw()) {
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
			if (isWALQuery) {
				ser << "WalItemUpdate ";
			}

			if (prettyPrint) {
				WrSerializer json;
				Error err = it.GetJSON(json, false);
				if (!err.ok()) {
					return err;
				}
				prettyPrintJSON(reindexer::giftStr(json.Slice()), ser);
			} else {
				Error err = it.GetJSON(ser, false);
				if (!err.ok()) {
					return err;
				}
			}
		}
		if ((++i != r.Count()) && !isWALQuery) {
			ser << ',';
		}
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
void CommandsExecutor<DBInterface>::addCommandsSuggestions(const std::string& cmd, std::vector<std::string>& suggestions) {
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
	if (!err.ok()) {
		return;
	}
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

	if (!token.length()) {
		return Error();
	}
	if (fromFile_ && reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kDumpModePrefix, command)) {
		DumpOptions opts;
		auto err = opts.FromJSON(reindexer::giftStr(command.substr(kDumpModePrefix.size())));
		if (!err.ok()) {
			return Error(errParams, "Unable to parse dump mode from cmd: %s", err.what());
		}
		dumpMode_ = opts.mode;
	}
	if (token.substr(0, 2) == "--") {
		return Error();
	}

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
			return Error(errLogic, "Unknown exception during command's execution");
		}
	}
	return Error(errParams, "Unknown command '%s'. Type '\\help' to list of available commands", token);
}

template <>
void CommandsExecutor<reindexer::Reindexer>::stop(bool terminate) {
	if (terminate) {
		stopCh_.close();
	}
}

template <>
void CommandsExecutor<reindexer::client::CoroReindexer>::stop(bool terminate) {
	if (terminate) {
		stopCh_.close();
	}
	return db().Stop();
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::getSuggestions(const std::string& input, std::vector<std::string>& suggestions) {
	if (!input.empty() && input[0] != '\\') {
		auto err = db().GetSqlSuggestions(input, input.length() - 1, suggestions);
		if (!err.ok()) {
			return err;
		}
	}
	if (suggestions.empty()) {
		addCommandsSuggestions(input, suggestions);
	}
	return {};
}

template <typename QueryResultsT>
std::vector<std::string> ToJSONVector(const QueryResultsT& r) {
	std::vector<std::string> vec;
	vec.reserve(r.Count());
	reindexer::WrSerializer ser;
	for (auto& it : r) {
		if (!it.Status().ok()) {
			continue;
		}
		ser.Reset();
		if (it.IsRaw()) {
			continue;
		}
		Error err = it.GetJSON(ser, false);
		if (!err.ok()) {
			continue;
		}
		vec.emplace_back(ser.Slice());
	}
	return vec;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandSelect(const std::string& command) noexcept {
	try {
		typename DBInterface::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw);
		const auto q = Query::FromSQL(command);

		auto err = db().Select(q, results);

		if (err.ok()) {
			if (results.Count()) {
				auto& outputType = variables_[kVariableOutput];
				if (outputType == kOutputModeTable) {
					auto jsonData = ToJSONVector(results);
					auto isCanceled = [this]() -> bool { return cancelCtx_.IsCancelled(); };

					reindexer::TableViewBuilder tableResultsBuilder;
					if (output_.IsCout() && !reindexer::isStdoutRedirected()) {
						TableViewScroller resultsScroller(tableResultsBuilder, reindexer::getTerminalSize().height - 1);
						resultsScroller.Scroll(output_, std::move(jsonData), isCanceled);
					} else {
						tableResultsBuilder.Build(output_(), std::move(jsonData), isCanceled);
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
				if (results.TotalCount()) {
					output_() << ", total count " << results.TotalCount();
				}
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
								if (i != 0) {
									output_() << "| ";
								}
								output_() << std::left << std::setw(maxW.at(i)) << agg.fields[i];
							}
							output_() << "| count" << std::endl;
							output_() << std::left << std::setw(rowWidth) << std::setfill('-') << "" << std::endl << std::setfill(' ');
							for (auto& row : agg.facets) {
								for (size_t i = 0; i < row.values.size(); ++i) {
									if (i != 0) {
										output_() << "| ";
									}
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
	CATCH_AND_RETURN;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandUpsert(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	const std::string nsName = reindexer::unescapeString(parser.NextToken());

	if (!parser.CurPtr().empty() && (parser.CurPtr())[0] == '[') {
		return Error(errParams, "Impossible to update entire item with array - only objects are allowed");
	}

	auto item = db().NewItem(nsName);

	Error status = item.Status();
	if (!status.ok()) {
		return status;
	}

	using namespace std::string_view_literals;
	if (fromFile_ && std::string_view(nsName) == "#config"sv) {
		try {
			gason::JsonParser p;
			auto root = p.Parse(parser.CurPtr());
			const std::string type = root["type"].As<std::string>();
			if (type == "action"sv) {
				return Error();
			}
			if (type == "async_replication"sv || type == "replication"sv) {
				if (targetHasReplicationConfig_) {
					output_() << "Skipping #config item: " << type << std::endl;
					return Error();
				}
			} else if (type == "sharding"sv) {
				output_() << "Skipping #config item: " << type << " (sharding config is read-only)" << std::endl;
				return Error();
			}
		} catch (const gason::Exception& ex) {
			return Error(errParseJson, "Unable to parse JSON for #config item: %s", ex.what());
		}
	}

	status = item.Unsafe().FromJSON(parser.CurPtr());
	if (!status.ok()) {
		return status;
	}

	status = db().Upsert(nsName, item);
	if (!fromFile_ && status.ok()) {
		output_() << "Upserted successfully: 1 items" << std::endl;
	}
	return status;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandUpdateSQL(const std::string& command) noexcept {
	try {
		typename DBInterface::QueryResultsT results;
		Query q = Query::FromSQL(command);

		auto err = db().Update(q, results);

		if (err.ok()) {
			output_() << "Updated " << results.Count() << " documents" << std::endl;
		}
		return err;
	}
	CATCH_AND_RETURN;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDelete(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	auto nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db().NewItem(nsName);
	if (!item.Status().ok()) {
		return item.Status();
	}

	auto err = item.Unsafe().FromJSON(parser.CurPtr());
	if (!err.ok()) {
		return err;
	}

	return db().Delete(nsName, item);
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDeleteSQL(const std::string& command) noexcept {
	try {
		typename DBInterface::QueryResultsT results;
		Query q = Query::FromSQL(command);
		auto err = db().Delete(q, results);

		if (err.ok()) {
			output_() << "Deleted " << results.Count() << " documents" << std::endl;
		}
		return err;
	}
	CATCH_AND_RETURN;
}

template <typename DBInterface>
Error CommandsExecutor<DBInterface>::commandDump(const std::string& command) {
	LineParser parser(command);
	parser.NextToken();

	std::vector<NamespaceDef> allNsDefs, doNsDefs;
	const auto dumpMode = dumpMode_.load();

	auto err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().HideTemporary());
	if (err) {
		return err;
	}

	err = filterNamespacesByDumpMode(allNsDefs, dumpMode);
	if (err) {
		return err;
	}

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
	wrser << "-- VERSION 1.1" << '\n';
	wrser << kDumpModePrefix;
	DumpOptions opts;
	opts.mode = dumpMode;
	opts.GetJSON(wrser);
	wrser << '\n';

	auto parametrizedDb = (dumpMode == DumpOptions::Mode::ShardedOnly) ? db() : db().WithShardId(ShardingKeyType::ProxyOff, false);

	for (auto& nsDef : doNsDefs) {
		// skip system namespaces, except #config
		if (reindexer::isSystemNamespaceNameFast(nsDef.name) && nsDef.name != "#config") {
			continue;
		}

		wrser << "-- Dumping namespace '" << nsDef.name << "' ..." << '\n';

		wrser << "\\NAMESPACES ADD " << reindexer::escapeString(nsDef.name) << " ";
		nsDef.GetJSON(wrser);
		wrser << '\n';

		std::vector<std::string> meta;
		err = parametrizedDb.EnumMeta(nsDef.name, meta);
		if (err) {
			return err;
		}

		std::string mdata;
		for (auto& mkey : meta) {
			mdata.clear();
			const bool isSerial = reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kSerialPrefix, mkey);
			if (isSerial) {
				err = getMergedSerialMeta(parametrizedDb, nsDef.name, mkey, mdata);
			} else {
				err = parametrizedDb.GetMeta(nsDef.name, mkey, mdata);
			}
			if (err) {
				return err;
			}

			wrser << "\\META PUT " << reindexer::escapeString(nsDef.name) << " " << reindexer::escapeString(mkey) << " "
				  << reindexer::escapeString(mdata) << '\n';
		}

		typename DBInterface::QueryResultsT itemResults;
		err = parametrizedDb.Select(Query(nsDef.name), itemResults);

		if (!err.ok()) {
			return err;
		}

		for (auto it : itemResults) {
			if (err = it.Status(); !err.ok()) {
				return err;
			}
			if (cancelCtx_.IsCancelled()) {
				return Error(errCanceled, "Canceled");
			}
			wrser << "\\UPSERT " << reindexer::escapeString(nsDef.name) << ' ';
			err = it.GetJSON(wrser, false);
			if (!err.ok()) {
				return err;
			}
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
		parser.NextToken();	 // nsName

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
		return parametrizedDb().PutMeta(nsName, metaKey, metaData);
	} else if (iequals(subCommand, "delete")) {
		std::string nsName = reindexer::unescapeString(parser.NextToken());
		std::string metaKey = reindexer::unescapeString(parser.NextToken());
		return db().DeleteMeta(nsName, metaKey);
	} else if (iequals(subCommand, "list")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		std::vector<std::string> allMeta;
		auto err = db().EnumMeta(nsName, allMeta);
		if (!err.ok()) {
			return err;
		}
		for (auto& metaKey : allMeta) {
			std::string metaData;
			err = db().GetMeta(nsName, metaKey, metaData);
			if (!err.ok()) {
				return err;
			}
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

	const std::string_view benchTimeToken = parser.NextToken();
	const int benchTime = benchTimeToken.empty() ? kBenchDefaultTime : reindexer::stoi(benchTimeToken);

	auto err = db().DropNamespace(kBenchNamespace);
	if (!err.ok() && err.code() != errNotFound) {
		return err;
	}

	NamespaceDef nsDef(kBenchNamespace);
	nsDef.AddIndex("id", "hash", "int", IndexOpts().PK());

	err = db().AddNamespace(nsDef);
	if (!err.ok()) {
		return err;
	}

	output_() << "Seeding " << kBenchItemsCount << " documents to bench namespace..." << std::endl;
	err = seedBenchItems();
	output_() << "done." << std::endl;
	if (!err.ok()) {
		return err;
	}

	output_() << "Running " << benchTime << "s benchmark..." << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto deadline = reindexer::system_clock_w::now_coarse() + std::chrono::seconds(benchTime);
	std::atomic<int> count(0), errCount(0);

	auto worker = std::bind(getBenchWorkerFn(count, errCount), deadline);
	const auto numThreads = std::min(std::max(numThreads_, 1u), 65535u);
	auto threads = std::unique_ptr<std::thread[]>(new std::thread[numThreads]);
	for (unsigned i = 0; i < numThreads; i++) {
		threads[i] = std::thread(worker);
	}
	for (unsigned i = 0; i < numThreads; i++) {
		threads[i].join();
	}

	output_() << "Done. Got " << count / benchTime << " QPS, " << errCount << " errors" << std::endl;
	return err;
}

template <>
Error CommandsExecutor<reindexer::client::CoroReindexer>::commandProcessDatabases(const std::string& command) {
	using namespace std::string_view_literals;
	LineParser parser(command);
	parser.NextToken();
	std::string_view subCommand = parser.NextToken();
	assertrx(uri_.scheme() == "cproto"sv || uri_.scheme() == "cprotos"sv || uri_.scheme() == "ucproto"sv);
	if (subCommand == "list"sv) {
		std::vector<std::string> dbList;
		Error err = getAvailableDatabases(dbList);
		if (!err.ok()) {
			return err;
		}
		for (const std::string& dbName : dbList) {
			output_() << dbName << std::endl;
		}
		return Error();
	} else if (subCommand == "use"sv) {
		reindexer::DSN currentDsn = getCurrentDsn().WithDb(std::string(parser.NextToken()));
		stop(false);
		auto err = db().Connect(currentDsn, loop_);
		if (err.ok()) {
			err = db().Status();
		}
		if (err.ok()) {
			output_() << "Successfully connected to " << currentDsn << std::endl;
		}
		return err;
	} else if (subCommand == "create"sv) {
		auto dbName = parser.NextToken();
		reindexer::DSN currentDsn = getCurrentDsn().WithDb(std::string(dbName));
		stop(false);
		output_() << "Creating database '" << dbName << "'" << std::endl;
		auto err = db().Connect(currentDsn, loop_, reindexer::client::ConnectOpts().CreateDBIfMissing());
		if (!err.ok()) {
			std::cerr << "Error on database '" << dbName << "' creation" << std::endl;
			return err;
		}
		std::vector<std::string> dbNames;
		err = db().EnumDatabases(dbNames);
		if (std::find(dbNames.begin(), dbNames.end(), std::string(dbName)) != dbNames.end()) {
			output_() << "Successfully created database '" << dbName << "'" << std::endl;
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
		if (!err.ok()) {
			return err;
		}

		err = db().Upsert(kBenchNamespace, item);
		if (!err.ok()) {
			return err;
		}
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
			if (intErr.ok()) {
				intErr = db().Upsert(kBenchNamespace, item);
			}
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
std::function<void(reindexer::system_clock_w::time_point)> CommandsExecutor<reindexer::client::CoroReindexer>::getBenchWorkerFn(
	std::atomic<int>& count, std::atomic<int>& errCount) {
	using reindexer::coroutine::wait_group;
	return [this, &count, &errCount](reindexer::system_clock_w::time_point deadline) {
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
				for (; reindexer::system_clock_w::now_coarse() < deadline; ++count) {
					Query q = Query(kBenchNamespace).Where(kBenchIndex, CondEq, count % kBenchItemsCount);
					reindexer::client::CoroReindexer::QueryResultsT results;
					auto err = rx.Select(q, results);
					if (!err.ok()) {
						errCount++;
					}
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
std::function<void(reindexer::system_clock_w::time_point)> CommandsExecutor<reindexer::Reindexer>::getBenchWorkerFn(
	std::atomic<int>& count, std::atomic<int>& errCount) {
	return [this, &count, &errCount](reindexer::system_clock_w::time_point deadline) {
		for (; ((count & 0x3FF) == 0) || reindexer::system_clock_w::now_coarse() < deadline; count++) {
			Query q(kBenchNamespace);
			q.Where(kBenchIndex, CondEq, count % kBenchItemsCount);
			auto results = new typename reindexer::Reindexer::QueryResultsT;

			const auto err = db().WithCompletion([results, &errCount](const Error& err) {
									 delete results;
									 if (!err.ok()) {
										 errCount++;
									 }
								 })
								 .Select(q, *results);
			if (!err.ok()) {
				++errCount;
			}
		}
	};
}

template <typename DBInterface>
reindexer::Error CommandsExecutor<DBInterface>::filterNamespacesByDumpMode(std::vector<NamespaceDef>& defs, DumpOptions::Mode mode) {
	if (mode == DumpOptions::Mode::FullNode) {
		return Error();
	}

	typename DBInterface::QueryResultsT qr;
	auto err = db().Select(Query("#config").Where("type", CondEq, "sharding"), qr);
	if (!err.ok()) {
		return err;
	}
	if (qr.Count() != 1) {
		output_() << "Sharding is not enabled, hovewer non-default dump mode is detected. That's weird...";
		return Error();
	}
	using reindexer::cluster::ShardingConfig;
	ShardingConfig cfg;
	WrSerializer ser;
	err = qr.begin().GetJSON(ser, false);
	if (!err.ok()) {
		return err;
	}

	auto json = reindexer::giftStr(ser.Slice());
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		err = cfg.FromJSON(root["sharding"]);
		if (!err.ok()) {
			return err;
		}
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "Unable to parse sharding config: %s", ex.what());
	}

	if (mode == DumpOptions::Mode::LocalOnly) {
		for (auto& shNs : cfg.namespaces) {
			const auto found = std::find_if(defs.begin(), defs.end(), [&shNs](const NamespaceDef& nsDef) {
				return reindexer::toLower(nsDef.name) == reindexer::toLower(shNs.ns);
			});
			if (found != defs.end()) {
				defs.erase(found);
			}
		}
	} else {
		defs.erase(std::remove_if(defs.begin(), defs.end(),
								  [&cfg](const NamespaceDef& nsDef) {
									  const auto found = std::find_if(
										  cfg.namespaces.begin(), cfg.namespaces.end(), [&nsDef](const ShardingConfig::Namespace& shNs) {
											  return reindexer::toLower(nsDef.name) == reindexer::toLower(shNs.ns);
										  });
									  return found == cfg.namespaces.end();
								  }),
				   defs.end());
	}
	return Error();
}

template <typename DBInterface>
reindexer::Error CommandsExecutor<DBInterface>::getMergedSerialMeta(DBInterface& db, std::string_view nsName, const std::string& key,
																	std::string& result) {
	std::vector<reindexer::ShardedMeta> meta;
	auto err = db.GetMeta(nsName, key, meta);
	if (!err.ok()) {
		return err;
	}

	int64_t maxVal = 0;
	for (auto& sm : meta) {
		try {
			const int64_t val = std::stoll(sm.data);
			if (val > maxVal) {
				maxVal = val;
			}
			// NOLINTBEGIN(bugprone-empty-catch)
		} catch (std::exception&) {
		}
		// NOLINTEND(bugprone-empty-catch)
	}
	result = std::to_string(maxVal);
	return Error();
}

template <typename DBInterface>
std::string CommandsExecutor<DBInterface>::URI::getDBFromUCproto() const {
	std::vector<std::string_view> pathParts;
	reindexer::split(std::string_view(uri_.path()), ":", true, pathParts);
	return pathParts.size() >= 2 ? std::string(pathParts.back()) : std::string();
}

template class CommandsExecutor<reindexer::client::CoroReindexer>;
template class CommandsExecutor<reindexer::Reindexer>;
template Error CommandsExecutor<reindexer::Reindexer>::Run(const std::string& dsn, const ConnectOpts& opts);
template Error CommandsExecutor<reindexer::client::CoroReindexer>::Run(const std::string& dsn, const reindexer::client::ConnectOpts& opts);

}  // namespace reindexer_tool
