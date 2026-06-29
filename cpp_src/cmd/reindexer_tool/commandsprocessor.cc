#include "commandsprocessor.h"

#include <filesystem>
#include <iomanip>
#include <limits>
#include <optional>
#include <thread>

#include "client/reindexer.h"
#include "cluster/config.h"
#include "core/cjson/jsonbuilder.h"
#include "core/query/impl.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"
#include "core/query/sql/sql_suggestions.h"
#include "core/reindexer.h"
#include "core/system_ns_names.h"
#include "estl/condition_variable.h"
#include "estl/gift_str.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "progress.h"
#include "tableviewscroller.h"
#include "tools/catch_and_return.h"
#include "tools/fsops.h"
#include "tools/jsontools.h"
#include "tools/scope_guard.h"
#include "vendor/hash/md5.h"
#include "wal/walrecord.h"

namespace reindexer_tool {

using reindexer::iequals;
using reindexer::WrSerializer;
using reindexer::NamespaceDef;
using reindexer::JsonBuilder;
using reindexer::Query;
using reindexer::Expected;
using reindexer::Unexpected;
using QueryImpl = reindexer::impl::Query;

const std::string kConfigFile = "rxtool_settings.txt";

const std::string kVariableOutput = "output";
const std::string kOutputModeJson = "json";
const std::string kOutputModeTable = "table";
const std::string kOutputModePretty = "pretty";
const std::string kVariableWithShardId = "with_shard_ids";
const std::string kBenchNamespace = "rxtool_bench";
const std::string kBenchIndex = "id";
const std::string kDumpModePrefix = "-- __dump_mode:";
const std::string kChecksumPrefix = "-- __checksum:";
const std::string kDumpingNamespacePrefix = "-- Dumping namespace";
const std::string kHeadCommandPrefix = "--";

const std::string kChecksumMismatchMessage = "Dump checksum mismatch";

constexpr int kBenchItemsCount = 10000;
constexpr int kBenchDefaultTime = 5;
constexpr int kMaxParallelOps = 5000;
constexpr int kChecksumLength = 32;

static void throwIfError(Error err) {
	if (!err.ok()) {
		throw err;
	}
}

static Expected<std::string> parseChecksum(std::string_view command) {
	command.remove_prefix(kChecksumPrefix.size());
	while (!command.empty() && std::isspace(static_cast<unsigned char>(command.front()))) {
		command.remove_prefix(1);
	}

	if (!command.empty() && command.front() == '"') {
		command.remove_prefix(1);
		if (command.empty() || command.back() != '"') {
			return Unexpected{Error{errParams, "Malformed quoted checksum value"}};
		}
		command.remove_suffix(1);
	}

	if (command.size() != kChecksumLength) {
		return Unexpected{Error{errParams, "Checksum must be {} hex digits, got {} symbol(s)", kChecksumLength, command.size()}};
	}
	for (char ch : command) {
		if (!std::isxdigit(static_cast<unsigned char>(ch))) {
			return Unexpected{Error{errParams, "Checksum contains non-hex characters"}};
		}
	}

	char resHex[kChecksumLength];
	for (int i = 0; i < kChecksumLength; ++i) {
		// for compatibility with MD5::getHash() output in lowercase
		resHex[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(command[i])));
	}
	return std::string{resHex, kChecksumLength};
}

static void appendCommandToChecksum(MD5& md5, std::string_view command) {
	if (command.empty() || reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kHeadCommandPrefix, command)) {
		return;
	}
	md5.add(command.data(), command.size());
}

static std::optional<size_t> fileSize(const std::string& filename) {
	std::error_code ec;
	const auto size = std::filesystem::file_size(filename, ec);
	if (ec || size > std::numeric_limits<size_t>::max()) {
		return std::nullopt;
	}
	return static_cast<size_t>(size);
}

static size_t streamPositionToBytes(std::streampos pos, size_t fallback) noexcept {
	if (pos == std::streampos(-1)) {
		return fallback;
	}
	const auto offset = static_cast<std::streamoff>(pos);
	return offset < 0 ? fallback : static_cast<size_t>(offset);
}

class [[nodiscard]] NamespaceIndex {
public:
	friend class DumpFileIndex;

	explicit NamespaceIndex(std::string name) : name_(std::move(name)) {}

	void ProcessCommand(std::string&& commandStr, size_t lineNum, std::streampos commandPos) {
		if (commandStr.empty()) {
			return;
		}

		LineParser parser(commandStr);
		std::string_view command = parser.NextToken();
		std::string_view subCommand = parser.NextToken();

		if (iequals(command, "\\namespaces")) {
			if (!addCommand_.first.empty() || !metaCommands_.empty() || numUpserts_ > 0) {
				throw Error(errLogic, "Invalid dump file command: {}", commandStr);
			}
			if (!iequals(subCommand, "add")) {
				throw Error(errLogic, "Unknown dump file namespaces command: {}", commandStr);
			}
			addCommand_ = {std::move(commandStr), lineNum};
		} else if (iequals(command, "\\meta")) {
			if (addCommand_.first.empty() || numUpserts_ > 0) {
				throw Error(errLogic, "Invalid dump file command: {}", commandStr);
			}
			metaCommands_.push_back({std::move(commandStr), lineNum});
		} else if (iequals(command, "\\upsert")) {
			if (addCommand_.first.empty()) {
				throw Error(errLogic, "Invalid dump file command: {}", commandStr);
			}
			if (numUpserts_ == 0) {
				nextUpsertPos_ = commandPos;
				firstUpsertLineNum_ = lineNum;
			}
			++numUpserts_;
		} else {
			throw Error(errLogic, "Unknown dump file command: {}", commandStr);
		}
	}

	void GetUpserts(std::ifstream& file, std::vector<std::pair<std::string, uint64_t>>& commands, size_t maxNumUpserts) {
		file.seekg(nextUpsertPos_);
		commands.resize(0);

		while (commands.size() < maxNumUpserts && nextUpsertIdx_ < numUpserts_) {
			std::string command;
			if (!std::getline(file, command)) {
				throw Error(errLogic, "Can't parse dump file: expected \\upsert command");
			}
			commands.emplace_back(std::move(command), nextUpsertIdx_ + firstUpsertLineNum_);
			nextUpsertPos_ = file.tellg();
			++nextUpsertIdx_;
		}
	}

	size_t GetProgress() const { return (1000 * nextUpsertIdx_) / (numUpserts_ + 1); }

private:
	std::string name_;
	std::pair<std::string, uint64_t> addCommand_;
	std::vector<std::pair<std::string, uint64_t>> metaCommands_;

	size_t firstUpsertLineNum_ = 0;
	size_t numUpserts_ = 0;
	size_t processedUpserts_ = 0;

	std::streampos nextUpsertPos_;
	size_t nextUpsertIdx_ = 0;
	size_t numProcessors_ = 0;
};

static std::string_view removeQuotes(std::string_view str) {
	if (str.size() > 1 && str.front() == '\'' && str.back() == '\'') {
		return str.substr(1, str.size() - 2);
	}

	return str;
}

class [[nodiscard]] DumpFileIndex {
public:
	Error Indexate(const std::string& filename, const StringsSetT& selectedNamespaces) noexcept {
		try {
			reindexer::lock_guard lock(dumpLock_);
			headCommands_.resize(0);
			nsDumps_.clear();
			expectedChecksum_.reset();
			computedChecksum_.clear();
			MD5 checksum;
			std::ifstream file(filename, std::ios::binary);
			if (!file) {
				return Error(errSystem, "Can not open file: {}", filename);
			}

			std::string command;
			std::streampos commandPos;
			size_t lineNum = 0;
			bool lastNamespaceSelected = true;
			std::string currentNamespace;
			const auto totalBytes = fileSize(filename);
			ConsoleProgress progress;
			bool progressFinished = false;
			auto progressGuard = reindexer::MakeScopeGuard([&] {
				if (totalBytes && !progressFinished) {
					progress.Done("Indexing dump: stopped");
				}
			});
			auto updateProgress = [&] {
				if (totalBytes) {
					std::string nsInfo;
					if (!currentNamespace.empty()) {
						nsInfo = fmt::format(" (namespace: {})", currentNamespace);
					}
					progress.Print(fmt::format("Indexing dump{}", nsInfo), streamPositionToBytes(file.tellg(), *totalBytes), *totalBytes);
				}
			};

			while (commandPos = file.tellg(), std::getline(file, command)) {
				++lineNum;

				auto isNotSpace = [](char c) { return !std::isspace(static_cast<unsigned char>(c)); };
				// Trim spaces from the beginning of the command
				if (auto it = std::find_if(command.begin(), command.end(), isNotSpace); it != command.end()) {
					command.erase(command.begin(), it);
				} else {
					updateProgress();
					continue;
				}
				// Trim spaces from the end of the command
				command.erase(std::find_if(command.rbegin(), command.rend(), isNotSpace).base(), command.end());

				if (reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kChecksumPrefix, command)) {
					if (expectedChecksum_) {
						return Error(errLogic, "Duplicate -- __checksum lines in dump file");
					}

					if (auto expected = parseChecksum(command)) {
						expectedChecksum_ = std::move(expected.value());
					} else {
						return expected.error();
					}
					updateProgress();
					continue;
				}

				if (reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kDumpingNamespacePrefix, command)) {
					LineParser parser(command);
					std::ignore = parser.NextToken();
					std::ignore = parser.NextToken();
					std::ignore = parser.NextToken();
					std::string_view nsName = removeQuotes(parser.NextToken());

					if (nsName.empty()) {
						return Error(errLogic, "Incorrect namespace name in dump file command: {}", command);
					}

					currentNamespace = nsName;
					lastNamespaceSelected = selectedNamespaces.empty() || selectedNamespaces.find(nsName) != selectedNamespaces.end();

					nsDumps_.emplace_back(currentNamespace);
					updateProgress();
					continue;
				}

				if (nsDumps_.empty()) {
					if (!reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kHeadCommandPrefix, command)) {
						return Error(errLogic, "Can't parse dump file : unknown head command {}", command);
					}
					headCommands_.emplace_back(std::move(command), lineNum);
					updateProgress();
					continue;
				}

				appendCommandToChecksum(checksum, command);

				if (lastNamespaceSelected) {
					nsDumps_.back().ProcessCommand(std::move(command), lineNum, commandPos);
				}
				updateProgress();
			}

			computedChecksum_ = checksum.getHash();
			if (totalBytes) {
				progressFinished = true;
				progress.Done("Indexing dump: done");
			}

			return errOK;
		} CATCH_AND_RETURN;
	}

	std::vector<std::pair<std::string, uint64_t>> GetHeadCommands() {
		reindexer::lock_guard lock(dumpLock_);
		std::vector<std::pair<std::string, uint64_t>> res = headCommands_;
		for (const NamespaceIndex& ns : nsDumps_) {
			res.push_back(ns.addCommand_);
			for (const auto& cmd : ns.metaCommands_) {
				res.push_back(cmd);
			}
		}
		return res;
	}

	Error GetUpserts(std::ifstream& file, std::vector<std::pair<std::string, uint64_t>>& commands, size_t numUpsertsRequired,
					 size_t& nsUsed) noexcept {
		try {
			commands.resize(0);
			reindexer::lock_guard lock(dumpLock_);

			std::vector<size_t> namespacesPriority;
			namespacesPriority.reserve(nsDumps_.size());
			for (size_t i = 0; i < nsDumps_.size(); i++) {
				namespacesPriority.push_back(i);
			}

			std::sort(namespacesPriority.begin(), namespacesPriority.end(), [&](size_t lhs, size_t rhs) {
				return nsDumps_[lhs].numProcessors_ < nsDumps_[rhs].numProcessors_ ||
					   (nsDumps_[lhs].numProcessors_ == nsDumps_[rhs].numProcessors_ &&
						nsDumps_[lhs].GetProgress() < nsDumps_[rhs].GetProgress());
			});

			for (size_t nsIndex : namespacesPriority) {
				nsDumps_[nsIndex].GetUpserts(file, commands, numUpsertsRequired);
				if (!commands.empty()) {
					nsUsed = nsIndex;
					++nsDumps_[nsIndex].numProcessors_;
					return errOK;
				}
			}

			return errOK;
		} CATCH_AND_RETURN;
	}

	void ProcessingEnded(size_t nsIndex, size_t processedUpserts) {
		reindexer::lock_guard lock(dumpLock_);
		assertrx_dbg(nsDumps_[nsIndex].numProcessors_ > 0);
		nsDumps_[nsIndex].numProcessors_--;
		nsDumps_[nsIndex].processedUpserts_ += processedUpserts;
	}

	std::vector<ProgressInfo> GetProgressSnapshot() {
		reindexer::lock_guard lock(dumpLock_);
		std::vector<ProgressInfo> res;
		res.reserve(nsDumps_.size());
		for (const auto& ns : nsDumps_) {
			res.emplace_back(ns.name_, ns.processedUpserts_, ns.numUpserts_, ns.numProcessors_);
		}
		return res;
	}

	const std::string& ComputedChecksum() const& noexcept { return computedChecksum_; }
	const std::optional<std::string>& ExpectedChecksum() const& noexcept { return expectedChecksum_; }

private:
	std::vector<std::pair<std::string, uint64_t>> headCommands_;
	std::vector<NamespaceIndex> nsDumps_;
	std::optional<std::string> expectedChecksum_;
	std::string computedChecksum_;
	reindexer::mutex dumpLock_;
};

template <typename DBInterface>
struct [[nodiscard]] CommandsProcessor<DBInterface>::CommandDefinition {
	std::string_view command;
	std::string_view description;
	void (CommandsProcessor::*handler)(std::string_view);
	std::string_view help;
};

// clang-format off
template <typename DBInterface>
const std::initializer_list<typename CommandsProcessor<DBInterface>::CommandDefinition> CommandsProcessor<DBInterface>::cmds_ = {
		CommandDefinition{"select",		"Query to database",&CommandsProcessor::commandSelectSQL,R"help(
	Syntax:
		See SQL Select statement
	Example:
		SELECT * FROM media_items where name = 'Thor'
		)help"},
		CommandDefinition{"delete",		"Delete documents from database",&CommandsProcessor::commandDeleteSQL,R"help(
	Syntax:
		See SQL Delete statement
	Example:
		DELETE FROM media_items where name = 'Thor'
		)help"},
		CommandDefinition{"update",		"Update documents in database",&CommandsProcessor::commandUpdateSQL,R"help(
	Syntax:
		See SQL Update statement
	Example:
		UPDATE media_items SET year='2011' where name = 'Thor'
		)help"},
		CommandDefinition{"explain",		"Explain query execution plan",&CommandsProcessor::commandSelectSQL,R"help(
	Syntax:
		See SQL Select statement
	Example:
		EXPLAIN SELECT * FROM media_items where name = 'Thor'
		)help"},
		CommandDefinition{"\\upsert",	"Upsert new item to namespace",&CommandsProcessor::commandUpsert,R"help(
	Syntax:
		\upsert <namespace> <document>
	Example:
		\upsert books {"id":5,"name":"xx"}
		)help"},
		CommandDefinition{"\\delete",	"Delete item from namespace",&CommandsProcessor::commandDelete,R"help(
	Syntax:
		\delete <namespace> <document>
	Example:
		\delete books {"id":5}
		)help"},
		CommandDefinition{"\\dump",		"Dump namespaces",&CommandsProcessor::commandDump,R"help(
	Syntax:
		\dump [namespace1 [namespace2]...]
		)help"},
		CommandDefinition{"\\namespaces","Manipulate namespaces",&CommandsProcessor::commandNamespaces,R"help(
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
		CommandDefinition{"\\meta",		"Manipulate meta",&CommandsProcessor::commandMeta,R"help(
	Syntax:
		\meta put <namespace> <key> <value>
		Put metadata key value

		\meta delete <namespace> <key>
		Delete metadata key value

		\meta list
		List all metadata in name
		)help"},
		CommandDefinition{"\\set",		"Set configuration variables values",&CommandsProcessor::commandSet,R"help(
	Syntax:
		\set <variable> <value>
		Variable can be one of the following:
			-'output'
				possible values:
					- 'json' Unformatted JSON
					- 'pretty' Pretty printed JSON
					- 'table' Table view
			-'with_shard_ids'
				possible values:
					- 'on'  Add '#shard_id' field to items from sharded namespaces
					- 'off'
		)help"},
		CommandDefinition{"\\bench",		"Run benchmark",&CommandsProcessor::commandBench,R"help(
	Syntax:
		\bench <time>
		)help"},
		CommandDefinition{"\\quit",		"Exit from tool",&CommandsProcessor::commandQuit,""},
		CommandDefinition{"\\help",		"Show help",&CommandsProcessor::commandHelp,""},
		CommandDefinition{"\\version",	"Show Reindexer server version when connected to it via [u]cproto://, or the rx-tool version when using built-in",&CommandsProcessor::commandVersion, ""},
		CommandDefinition{"\\databases", "Works with available databases",&CommandsProcessor::commandProcessDatabases, R"help(
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
template <typename DBInterface>
class [[nodiscard]] WaitGroup;

template <>
class [[nodiscard]] WaitGroup<reindexer::client::Reindexer> {
public:
	WaitGroup() noexcept = default;
	WaitGroup(const WaitGroup&) = delete;
	~WaitGroup() { assertrx_dbg(count_ == 0); }
	void operator=(WaitGroup&) = delete;

	void Add(int increment = 1) noexcept {
		reindexer::lock_guard lock(mutex_);
		count_ += increment;
	}

	void Done(const Error& err) noexcept {
		reindexer::lock_guard lock(mutex_);
		if (!err.ok()) {
			err_ = err;
		}
		--count_;
		if (count_ <= 0) {
			cv_.notify_all();
		}
	}

	void Wait() noexcept {
		reindexer::unique_lock lock(mutex_);
		assertrx_dbg(count_ >= 0);
		cv_.wait(lock, [this] { return count_ <= 0; });
	}

	reindexer::Error Error() const noexcept {
		reindexer::lock_guard lock(mutex_);
		return err_;
	}

private:
	int count_ = 0;
	reindexer::Error err_;
	mutable reindexer::mutex mutex_;
	reindexer::condition_variable cv_;
};

template <>
class [[nodiscard]] WaitGroup<reindexer::Reindexer> {
public:
	WaitGroup() noexcept = default;
	WaitGroup(const WaitGroup&) = delete;
	void operator=(WaitGroup&) = delete;

	void Add(int) const noexcept {}

	void Done(const Error& err) noexcept {
		if (!err.ok()) {
			err_ = err;
		}
	}

	void Wait() const noexcept {}

	reindexer::Error Error() const noexcept { return err_; }

private:
	reindexer::Error err_;
};

template <typename DBInterface>
void CommandsProcessor<DBInterface>::loadVariables() {
	std::string config;
	if (reindexer::fs::ReadFile(reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile), config) > 0) {
		try {
			gason::JsonParser jsonParser;
			gason::JsonNode value = jsonParser.Parse(reindexer::giftStr(config));
			for (auto node : value) {
				WrSerializer ser;
				reindexer::jsonValueToString(node.value, ser, 0, 0, false);
				if (std::string_view(node.key) == kVariableOutput) {
					variables_[kVariableOutput] = ser.Slice();
				} else if (std::string_view(node.key) == kVariableWithShardId) {
					variables_[kVariableWithShardId] = ser.Slice();
				}
			}
		} catch (const gason::Exception& e) {
			throw Error(errParseJson, "Unable to parse output mode: {}", e.what());
		}
	}

	if (variables_.find(kVariableOutput) == variables_.end()) {
		variables_[kVariableOutput] = kOutputModeJson;
	}
}

template <typename DBInterface>
template <typename ConnectOpts>
Error CommandsProcessor<DBInterface>::Connect(const std::string& dsn, const ConnectOpts& connectOpts) noexcept {
	try {
		loadVariables();
		if (!uri_.parse(dsn)) {
			return Error(errNotValid, "Cannot connect to DB: Invalid URI");
		}

		return db().Connect(dsn, connectOpts);
	} CATCH_AND_RETURN;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::setDumpMode(const std::string& mode) {
	dumpMode_.store(DumpOptions::ModeFromStr(mode));
}

static void printError(const Error& err) { std::cerr << fmt::format("ERROR: {}\n", err.what()); }
static void printWarning(const std::string& msg) { std::cerr << "Warning: " << msg << std::endl; }
static void printError(const Error& err, uint64_t lineNum) { std::cerr << fmt::format("LINE: {} ERROR: {}\n", lineNum, err.what()); }

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::Run(const std::string& command, const std::string& dumpMode, bool dryRun,
										  bool ignoreChecksumMismatch) noexcept {
	try {
		if (!dumpMode.empty()) {
			setDumpMode(dumpMode);
		}

		if (dryRun) {
			if (!command.empty()) {
				return Error(errParams, "--dry-run is incompatible with -c/--command, use -f/--filename instead");
			}
			if (inFileName_.empty()) {
				return Error(errParams, "--dry-run can only be used together with -f/--filename");
			}
			if (auto err = db().Status(); !err.ok()) {
				printError(err);
				return err;
			}
			return dryRunDumpFile();
		}

		if (!command.empty()) {
			if (Error err = process(command); !err.ok()) {
				printError(err);
				return err;
			}
			return errOK;
		}

		if (!inFileName_.empty()) {
			std::ifstream infile(inFileName_, std::ios::binary);
			if (!infile) {
				Error err(errTerminated, "ERROR: Can't open {}: {}", inFileName_, strerror(errno));
				printError(err);
				return err;
			}
			const auto inputFileSize = fileSize(inFileName_);
			DumpFileIndex dumpFileIdx;
			if (Error err = dumpFileIdx.Indexate(inFileName_, selectedNamespaces_); !err.ok()) {
				printError(err);
				if (!selectedNamespaces_.empty()) {
					printWarning("Can not parse file sequentially because of selected namespaces set, ending...");
					return err;
				}
				printWarning("Input file does not look like a dump file, file will be parsed sequentially");
				fromFile(infile, inputFileSize);
				return errOK;
			}

			if (auto expectedChecksum = dumpFileIdx.ExpectedChecksum();
				expectedChecksum && *expectedChecksum != dumpFileIdx.ComputedChecksum()) {
				auto message = fmt::format("{}. Expected '{}', Computed '{}'", kChecksumMismatchMessage, *expectedChecksum,
										   dumpFileIdx.ComputedChecksum());
				if (ignoreChecksumMismatch) {
					printWarning(message);
				} else {
					Error err(errParams, fmt::format("{}. Use --ignore-checksum-mismatch to apply dump despite mismatch", message));
					printError(err);
					return err;
				}
			}

			fromDumpFile(infile, dumpFileIdx);
			return errOK;
		} else if (reindexer::isStdinRedirected()) {
			fromFile(std::cin);
			return errOK;
		} else {
			return interactive();
		}
	} CATCH_AND_RETURN;
}

static std::string indexDefDiffDescription(const reindexer::IndexDef& dumpIdx, const reindexer::IndexDef& targetIdx) {
	const auto diff = dumpIdx.Compare(targetIdx);
	// Mirror NamespaceImpl::checkIfSameIndexExists() — restore considers two indexes the same when only
	// "non-structural" options differ (dense flag, sort_order_letters, FT config, etc.). Anything that
	// would actually break re-creation of the index is reported below.
	if (reindexer::IndexDef::IsBasicCompatibility(diff)) {
		return {};
	}

	std::vector<std::string> parts;
	if (diff.Get<reindexer::IndexDef::Diff>() & uint8_t(reindexer::IndexDef::Diff::JsonPaths)) {
		WrSerializer dumpJP, targetJP;
		dumpJP << '[';
		for (size_t i = 0; i < dumpIdx.JsonPaths().size(); ++i) {
			if (i != 0) {
				dumpJP << ',';
			}
			dumpJP << dumpIdx.JsonPaths()[i];
		}
		dumpJP << ']';
		targetJP << '[';
		for (size_t i = 0; i < targetIdx.JsonPaths().size(); ++i) {
			if (i != 0) {
				targetJP << ',';
			}
			targetJP << targetIdx.JsonPaths()[i];
		}
		targetJP << ']';
		parts.emplace_back(fmt::format("json_paths {}!={}", dumpJP.Slice(), targetJP.Slice()));
	}
	if ((diff.Get<reindexer::IndexDef::Diff>() & uint8_t(reindexer::IndexDef::Diff::IndexType)) &&
		dumpIdx.IndexTypeStr() != targetIdx.IndexTypeStr()) {
		// IndexDef::Compare sets Diff::IndexType by comparing the resolved ::IndexType enum, which is
		// derived from (indexType_, fieldType_). Two indexes can have identical IndexTypeStr() but
		// different resolved enum values (e.g. indexType="" with fieldType="int" vs fieldType="double").
		// Without the string check we would emit confusing "index_type 'hash'!='hash'"; the underlying
		// difference is then surfaced via the field_type mismatch below.
		parts.emplace_back(fmt::format("index_type '{}'!='{}'", dumpIdx.IndexTypeStr(), targetIdx.IndexTypeStr()));
	}
	if (diff.Get<reindexer::IndexDef::Diff>() & uint8_t(reindexer::IndexDef::Diff::FieldType)) {
		// Diff::FieldType is purely a string comparison in IndexDef::Compare, so the bit being set
		// already implies dumpIdx.FieldType() != targetIdx.FieldType() — no extra guard needed.
		parts.emplace_back(fmt::format("field_type '{}'!='{}'", dumpIdx.FieldType(), targetIdx.FieldType()));
	}
	if (diff.Get<reindexer::IndexDef::Diff>() & uint8_t(reindexer::IndexDef::Diff::ExpireAfter)) {
		parts.emplace_back(fmt::format("expire_after {}!={}", dumpIdx.ExpireAfter(), targetIdx.ExpireAfter()));
	}
	if (diff.Get<reindexer::IndexOpts::OptsDiff>() != 0 || diff.Get<reindexer::IndexOpts::ParamsDiff>() != 0 ||
		diff.Get<reindexer::FloatVectorIndexOpts::Diff>() != 0) {
		parts.emplace_back("options/params");
	}
	if (parts.empty()) {
		return "differs";
	}
	std::string res = parts.front();
	for (size_t i = 1; i < parts.size(); ++i) {
		res += ", ";
		res += parts[i];
	}
	return res;
}

std::string static namespaceIndexesMismatchDescription(const std::vector<reindexer::IndexDef>& dumpIdx,
													   const std::vector<reindexer::IndexDef>& targetIdx) {
	std::vector<std::string> diffs;

	for (const auto& dumped : dumpIdx) {
		const auto found = std::find_if(targetIdx.begin(), targetIdx.end(),
										[&dumped](const reindexer::IndexDef& i) { return iequals(i.Name(), dumped.Name()); });
		if (found == targetIdx.end()) {
			diffs.emplace_back(fmt::format("index '{}' is missing on target", dumped.Name()));
		} else if (auto desc = indexDefDiffDescription(dumped, *found); !desc.empty()) {
			diffs.emplace_back(fmt::format("index '{}' differs: {}", dumped.Name(), desc));
		}
	}
	for (const auto& targetIdxDef : targetIdx) {
		const auto found = std::find_if(dumpIdx.begin(), dumpIdx.end(),
										[&targetIdxDef](const reindexer::IndexDef& i) { return iequals(i.Name(), targetIdxDef.Name()); });
		if (found == dumpIdx.end()) {
			diffs.emplace_back(fmt::format("index '{}' is missing in dump", targetIdxDef.Name()));
		}
	}

	if (diffs.empty()) {
		return {};
	}
	std::string res = diffs.front();
	for (size_t i = 1; i < diffs.size(); ++i) {
		res += "; ";
		res += diffs[i];
	}
	return res;
}

namespace {
struct [[nodiscard]] DryRunError {
	uint64_t line = 0;
	std::string message;
};
}  // namespace

static void dryRunPrintReport(std::ostream& out, const std::vector<DryRunError>& errors,
							  const std::vector<std::string>& nonEmptyExistingDumpNamespaces,
							  const std::vector<std::string>& targetOnlyNamespaces) noexcept {
	if (!errors.empty()) {
		out << "Dry run errors:" << std::endl;
		for (const auto& err : errors) {
			if (err.line == 0) {
				out << (err.message.find(kChecksumMismatchMessage) != std::string::npos ? "  Warning: " : "  ERROR: ") << err.message
					<< std::endl;
			} else {
				out << "  LINE: " << err.line << " ERROR: " << err.message << std::endl;
			}
		}
	}

	if (!nonEmptyExistingDumpNamespaces.empty()) {
		out << "Existing non-empty namespaces present in dump (potential conflicts):" << std::endl;
		for (const auto& nsName : nonEmptyExistingDumpNamespaces) {
			out << "  " << nsName << std::endl;
		}
	}

	if (!targetOnlyNamespaces.empty()) {
		out << "Namespaces present on target but absent in dump:" << std::endl;
		for (const auto& nsName : targetOnlyNamespaces) {
			out << "  " << nsName << std::endl;
		}
	}
}

using NsDefMapT = reindexer::fast_hash_map<std::string, NamespaceDef, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

static void dryRunValidateNamespace(std::string_view command, uint64_t lineNum, reindexer::Reindexer& validatorDb,
									const NsDefMapT& targetNsDefs, StringsSetT& dumpNamespaces, std::vector<DryRunError>& errors) {
	LineParser parser(command);
	std::ignore = parser.NextToken();
	const std::string_view subCommand = parser.NextToken();
	if (!iequals(subCommand, "add")) {
		errors.emplace_back(lineNum, fmt::format("Unknown \\namespaces subcommand: '{}'", subCommand));
		return;
	}

	const std::string nsName = reindexer::unescapeString(parser.NextToken());
	if (nsName.empty() || parser.CurPtr().empty()) {
		errors.emplace_back(lineNum, fmt::format("Invalid \\namespaces add command: {}", command));
		return;
	}

	NamespaceDef def("");
	if (Error err = def.FromJSON(reindexer::giftStr(parser.CurPtr())); !err.ok()) {
		errors.emplace_back(lineNum, fmt::format("Namespace structure is not valid: {}", err.what()));
		return;
	}
	if (!iequals(nsName, def.name)) {
		errors.emplace_back(lineNum, fmt::format("Namespace name '{}' does not match definition name '{}'", nsName, def.name));
		return;
	}

	if (auto [_, inserted] = dumpNamespaces.emplace(def.name); !inserted) {
		errors.emplace_back(lineNum, fmt::format("Duplicate \\NAMESPACES ADD for '{}'", def.name));
		return;
	}

	if (auto targetIt = targetNsDefs.find(def.name); targetIt != targetNsDefs.end()) {
		if (auto desc = namespaceIndexesMismatchDescription(def.indexes, targetIt->second.indexes); !desc.empty()) {
			errors.emplace_back(lineNum, fmt::format("Indexes mismatch for namespace '{}': {}", def.name, desc));
		}
	}

	def.storage = StorageOpts().Enabled(false);
	if (Error err = validatorDb.OpenNamespace(def.name, def.storage); !err.ok()) {
		errors.emplace_back(lineNum, fmt::format("Failed to open ns '{}' in validator: {}", def.name, err.what()));
		return;
	}
	for (auto& idx : def.indexes) {
		if (Error err = validatorDb.AddIndex(def.name, idx); !err.ok()) {
			errors.emplace_back(lineNum,
								fmt::format("Failed to add index '{}' to ns '{}' in validator: {}", idx.Name(), def.name, err.what()));
			return;
		}
	}
	if (def.HasSchema()) {
		if (Error err = validatorDb.SetSchema(def.name, def.schemaJson); !err.ok()) {
			errors.emplace_back(lineNum, fmt::format("Failed to set schema for ns '{}' in validator: {}", def.name, err.what()));
		}
	}
}

static void dryRunValidateUpsert(std::string_view command, uint64_t lineNum, reindexer::Reindexer& validatorDb,
								 const StringsSetT& dumpNamespaces, std::vector<DryRunError>& errors) {
	LineParser parser(command);
	std::ignore = parser.NextToken();
	const std::string nsName = reindexer::unescapeString(parser.NextToken());
	if (nsName.empty() || parser.CurPtr().empty()) {
		errors.emplace_back(lineNum, fmt::format("Invalid \\upsert command: {}", command));
		return;
	}
	if (parser.CurPtr().front() == '[') {
		errors.emplace_back(lineNum, "Impossible to update entire item with array - only objects are allowed");
		return;
	}

	const bool isConfigNs = (std::string_view(nsName) == reindexer::kConfigNamespace);
	if (isConfigNs) {
		using namespace std::string_view_literals;
		try {
			gason::JsonParser p;
			auto root = p.Parse(parser.CurPtr());
			const auto type = root["type"].As<std::string_view>();
			if (type == "action"sv || type == "sharding"sv) {
				return;
			}
		} catch (const gason::Exception& ex) {
			errors.emplace_back(lineNum, fmt::format("Unable to parse JSON for #config item: {}", ex.what()));
			return;
		}
	}

	if (dumpNamespaces.find(nsName) == dumpNamespaces.end()) {
		errors.emplace_back(lineNum, fmt::format("\\UPSERT references namespace '{}' that has no preceding \\NAMESPACES ADD", nsName));
		return;
	}

	auto item = validatorDb.NewItem(nsName);
	if (Error err = item.Status(); !err.ok()) {
		errors.emplace_back(lineNum, fmt::format("\\UPSERT into ns '{}' failed: {}", nsName, err.what()));
		return;
	}
	if (Error err = item.Unsafe().FromJSON(parser.CurPtr()); !err.ok()) {
		errors.emplace_back(lineNum, fmt::format("\\UPSERT JSON for ns '{}' is invalid: {}", nsName, err.what()));
	}
}

static void dryRunValidateMeta(std::string_view command, uint64_t lineNum, const StringsSetT& dumpNamespaces,
							   std::vector<DryRunError>& errors) {
	LineParser parser(command);
	std::ignore = parser.NextToken();
	const std::string_view subCommand = parser.NextToken();
	if (!iequals(subCommand, "put")) {
		errors.emplace_back(lineNum, fmt::format("Unknown \\meta subcommand in dump: '{}'", subCommand));
		return;
	}
	const std::string nsName = reindexer::unescapeString(parser.NextToken());
	const std::string metaKey = reindexer::unescapeString(parser.NextToken());
	if (nsName.empty() || metaKey.empty()) {
		errors.emplace_back(lineNum,
							fmt::format("Invalid \\meta put command ({}): {}", nsName.empty() ? "empty ns name" : "empty key", command));
		return;
	}
	if (dumpNamespaces.find(nsName) == dumpNamespaces.end()) {
		errors.emplace_back(lineNum, fmt::format("\\META PUT for namespace '{}' has no preceding \\NAMESPACES ADD", nsName));
	}
}

static void dryRunValidateHeadCommand(std::string_view command, uint64_t lineNum, reindexer::Reindexer& validatorDb,
									  const NsDefMapT& targetNsDefs, StringsSetT& dumpNamespaces, std::vector<DryRunError>& errors) {
	if (command.empty()) {
		// Empty entry produced by GetHeadCommands() for a namespace block that was filtered out by
		// selectedNamespaces_ — nothing to validate.
		return;
	}
	if (reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kDumpModePrefix, command)) {
		DumpOptions opts;
		if (Error err = opts.FromJSON(reindexer::giftStr(command.substr(kDumpModePrefix.size()))); !err.ok()) {
			errors.emplace_back(lineNum, fmt::format("Unable to parse dump mode from cmd: {}", err.what()));
		}
		return;
	}
	if (reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kHeadCommandPrefix, command)) {
		// Other comments (e.g. version banner, "Dumping namespace ..." headers) — no validation required.
		return;
	}

	LineParser parser(command);
	const auto token = parser.NextToken();
	if (token.empty()) {
		return;
	}
	if (iequals(token, "\\namespaces")) {
		dryRunValidateNamespace(command, lineNum, validatorDb, targetNsDefs, dumpNamespaces, errors);
	} else if (iequals(token, "\\meta")) {
		dryRunValidateMeta(command, lineNum, dumpNamespaces, errors);
	} else {
		// \UPSERT is processed via GetUpserts(); anything else here is unexpected.
		errors.emplace_back(lineNum, fmt::format("Unknown dump file command: {}", command));
	}
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::dryRunDumpFile() noexcept {
	try {
		std::ifstream infile(inFileName_, std::ios::binary);
		if (!infile) {
			Error err(errTerminated, "Can't open {}: {}", inFileName_, strerror(errno));
			printError(err);
			return err;
		}

		std::vector<NamespaceDef> targetNsList;
		throwIfError(db().EnumNamespaces(targetNsList, reindexer::EnumNamespacesOpts().WithClosed().HideSystem()));
		NsDefMapT targetNsDefs;
		for (auto& nsDef : targetNsList) {
			targetNsDefs.emplace(nsDef.name, std::move(nsDef));
		}

		// Local in-memory database used purely as a schema validator. Per-namespace storage is disabled
		// below so that no files can be written even if the underlying builtin engine has a non-empty
		// default storage path.
		reindexer::Reindexer validatorDb;
		throwIfError(validatorDb.Connect("builtin://", ConnectOpts().DisableReplication()));

		std::vector<DryRunError> errors;
		StringsSetT dumpNamespaces;

		DumpFileIndex dumpFileIdx;
		if (Error err = dumpFileIdx.Indexate(inFileName_, selectedNamespaces_); !err.ok()) {
			errors.emplace_back(0, fmt::format("Failed to parse dump file: {}", err.what()));
			dryRunPrintReport(std::cout, errors, /*nonEmptyExistingDumpNamespaces=*/{}, /*targetOnlyNamespaces=*/{});
			std::cout << "Dry run finished: " << errors.size() << " error(s), 0 conflict(s), 0 target-only namespace(s)" << std::endl;
			return Error(errParams, "Dry run found {} error(s) in dump file", errors.size());
		}
		if (auto expectedChecksum = dumpFileIdx.ExpectedChecksum();
			expectedChecksum && *expectedChecksum != dumpFileIdx.ComputedChecksum()) {
			errors.emplace_back(0, fmt::format("{}. Expected '{}', Computed '{}'", kChecksumMismatchMessage, *expectedChecksum,
											   dumpFileIdx.ComputedChecksum()));
		}

		for (const auto& [cmd, lineNum] : dumpFileIdx.GetHeadCommands()) {
			dryRunValidateHeadCommand(cmd, lineNum, validatorDb, targetNsDefs, dumpNamespaces, errors);
		}

		// \UPSERT validation is the heaviest part of dry-run on big dumps. It mirrors fromDumpFile():
		// DumpFileIndex::GetUpserts is thread-safe (guarded by dumpLock_) and distributes batches among
		// workers per-namespace via numProcessors_/ProcessingEnded(). Each worker validates JSON against
		// the in-memory validatorDb, which supports concurrent NewItem/FromJSON on a single namespace.
		const size_t numThreads = std::max<size_t>(1, static_cast<size_t>(numThreads_));
		std::vector<std::vector<DryRunError>> threadErrors(numThreads);
		std::vector<Error> threadErrs(numThreads);
		std::atomic<bool> abort{false};
		ConsoleProgress progress;
		std::atomic<bool> progressDone{false};
		std::thread progressThread([&] {
			while (!progressDone.load(std::memory_order_relaxed)) {
				progress.Print("Dry run upserts", dumpFileIdx.GetProgressSnapshot());
				std::this_thread::sleep_for(std::chrono::milliseconds(200));
			}
			progress.Done(abort.load(std::memory_order_relaxed) ? "Dry run upserts: stopped" : "Dry run upserts: done");
		});
		auto stopProgress = [&] {
			progressDone.exchange(true, std::memory_order_relaxed);
			if (progressThread.joinable()) {
				progressThread.join();
			}
		};
		auto progressGuard = reindexer::MakeScopeGuard(stopProgress);
		auto workerFn = [&](size_t threadIdx) {
			std::vector<std::pair<std::string, uint64_t>> batch;
			while (!abort.load(std::memory_order_relaxed)) {
				size_t nsUsed = 0;
				threadErrs[threadIdx] = dumpFileIdx.GetUpserts(infile, batch, kMaxParallelOps, nsUsed);
				if (!threadErrs[threadIdx].ok()) {
					abort.store(true, std::memory_order_relaxed);
					return;
				}
				if (batch.empty()) {
					return;
				}
				for (const auto& [cmd, lineNum] : batch) {
					dryRunValidateUpsert(cmd, lineNum, validatorDb, dumpNamespaces, threadErrors[threadIdx]);
				}
				dumpFileIdx.ProcessingEnded(nsUsed, batch.size());
			}
		};

		std::vector<std::thread> threads;
		threads.reserve(numThreads);
		for (size_t i = 0; i < numThreads; ++i) {
			threads.emplace_back(workerFn, i);
		}
		for (auto& t : threads) {
			t.join();
		}
		stopProgress();

		for (size_t i = 0; i < numThreads; ++i) {
			if (!threadErrs[i].ok()) {
				errors.emplace_back(0, fmt::format("Failed to read \\UPSERT batch from dump: {}", threadErrs[i].what()));
			}
			for (auto& err : threadErrors[i]) {
				errors.emplace_back(std::move(err));
			}
		}
		std::stable_sort(errors.begin(), errors.end(), [](const DryRunError& a, const DryRunError& b) { return a.line < b.line; });

		std::vector<std::string> nonEmptyExistingDumpNamespaces;
		for (const auto& nsName : dumpNamespaces) {
			if (targetNsDefs.find(nsName) == targetNsDefs.end()) {
				continue;
			}
			typename DBInterface::QueryResultsT results;
			QueryImpl q(nsName);
			q.Limit(1);
			if (Error err = db().Select(q, results); !err.ok()) {
				errors.emplace_back(0, fmt::format("Unable to check namespace '{}' emptiness: {}", nsName, err.what()));
			} else if (results.Count() > 0) {
				nonEmptyExistingDumpNamespaces.emplace_back(nsName);
			}
		}

		std::vector<std::string> targetOnlyNamespaces;
		for (const auto& [nsName, _] : targetNsDefs) {
			// When -n filter is active, do not report target namespaces outside of the user-selected set;
			// the user has explicitly limited the scope to those namespaces.
			if (!selectedNamespaces_.empty() && selectedNamespaces_.find(nsName) == selectedNamespaces_.end()) {
				continue;
			}
			if (dumpNamespaces.find(nsName) == dumpNamespaces.end()) {
				targetOnlyNamespaces.emplace_back(nsName);
			}
		}
		std::sort(nonEmptyExistingDumpNamespaces.begin(), nonEmptyExistingDumpNamespaces.end());
		std::sort(targetOnlyNamespaces.begin(), targetOnlyNamespaces.end());

		// Per task spec: non-zero exit code only when there are actual errors. Conflicts and target-only
		// namespaces are warnings; we still print the report but return success in that case.
		const bool hasIssues = !errors.empty() || !nonEmptyExistingDumpNamespaces.empty() || !targetOnlyNamespaces.empty();
		if (!hasIssues) {
			std::cout << "Dry run completed successfully: no problems found" << std::endl;
			return errOK;
		}

		dryRunPrintReport(std::cout, errors, nonEmptyExistingDumpNamespaces, targetOnlyNamespaces);
		std::cout << "Dry run finished: " << errors.size() << " error(s), " << nonEmptyExistingDumpNamespaces.size() << " conflict(s), "
				  << targetOnlyNamespaces.size() << " target-only namespace(s)" << std::endl;

		if (!errors.empty()) {
			return Error(errParams, "Dry run found {} error(s) in dump file", errors.size());
		}
		return errOK;
	} CATCH_AND_RETURN;
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::process(const std::string& command) noexcept {
	try {
		LineParser parser(command);
		auto token = parser.NextToken();

		if (!token.length()) {
			return errOK;
		}
		if (fromFile_ && reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kDumpModePrefix, command)) {
			DumpOptions opts;
			auto err = opts.FromJSON(reindexer::giftStr(command.substr(kDumpModePrefix.size())));
			if (!err.ok()) {
				return Error(errParams, "Unable to parse dump mode from cmd: {}", err.what());
			}
			dumpMode_.store(opts.mode);
		}
		if (token.substr(0, 2) == "--") {
			return errOK;
		}

		for (auto& c : cmds_) {
			if (iequals(token, c.command)) {
				auto cleanup = reindexer::MakeScopeGuard([this]() { cancelCtx_.Reset(); });
				(this->*(c.handler))(command);
				if (cancelCtx_.IsCancelled()) {
					throw Error(errCanceled, "Canceled");
				}
				return errOK;
			}
		}
		return Error(errParams, "Unknown command '{}'. Type '\\help' to list of available commands", token);
	} CATCH_AND_RETURN;
}

#if REINDEX_WITH_REPLXX
template <typename DBInterface>
template <typename T>
void CommandsProcessor<DBInterface>::setCompletionCallback(T& rx, void (T::*set_completion_callback)(const callback_t&)) {
	(rx.*set_completion_callback)([this](const std::string& input, int) {
		reindexer::SQLSuggestions completions;
		const auto err = getSuggestions(input, completions);
		replxx::Replxx::completions_t result;
		if (err.ok()) {
			for (const std::string& suggestion : completions.suggestions) {
				result.emplace_back(suggestion);
			}
		}
		return result;
	});
}
#endif	// REINDEX_WITH_REPLXX

template <typename T>
class [[nodiscard]] HasSetMaxLineSize {
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
void setMaxLineSize(T* rx, int arg, typename std::enable_if<HasSetMaxLineSize<T>::value>::type* = nullptr) {
	return rx->set_max_line_size(arg);
}

void setMaxLineSize(...) {}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::interactive() noexcept {
	try {
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
		while (!quitCmdAccepted_) {
			const char* input = nullptr;
			do {
				input = rx.input(prompt);
			} while (!input && errno == EAGAIN);

			if (input == nullptr) {
				break;
			}

			if (!*input) {
				continue;
			}

			Error err = process(input);
			if (!err.ok()) {
				std::cerr << "ERROR: " << err.what() << std::endl;
			}
			rx.history_add(input);
		}
		rx.history_save(history_file);
#else
		std::string prompt = "Reindexer> ";
		// main repl loop
		while (!quitCmdAccepted_) {
			std::string command;
			std::cout << prompt;
			if (!std::getline(std::cin, command)) {
				break;
			}
			Error err = process(command);
			if (!err.ok()) {
				std::cerr << "ERROR: " << err.what() << std::endl;
			}
		}
#endif
		return errOK;
	} CATCH_AND_RETURN;
}

template <typename DBInterface>
bool CommandsProcessor<DBInterface>::isHavingReplicationConfig(WrSerializer& wser, std::string_view type) {
	try {
		typename DBInterface::QueryResultsT results(kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID);

		auto err = db().Select(*reindexer::impl::Impl{Query(reindexer::kReplicationStatsNamespace).Where("type", CondEq, type)}, results);
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
bool CommandsProcessor<DBInterface>::isHavingReplicationConfig() {
	using namespace std::string_view_literals;
	if (uri_.db().empty()) {
		return false;
	}
	WrSerializer wser;
	return isHavingReplicationConfig(wser, "cluster"sv) || isHavingReplicationConfig(wser, "async"sv);
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::fromFile(std::istream& infile, std::optional<size_t> inputFileSize) {
	fromFile_ = true;
	auto fromFileGuard = reindexer::MakeScopeGuard([this]() { fromFile_ = false; });
	ConsoleProgress progress;
	bool progressFinished = false;
	auto progressGuard = reindexer::MakeScopeGuard([&] {
		if (inputFileSize && !progressFinished) {
			progress.Done("Restoring dump: stopped");
		}
	});

	targetHasReplicationConfig_ = isHavingReplicationConfig();
	if (targetHasReplicationConfig_) {
		output_() << "Target database has replication configured, so corresponding configs will not be overridden" << std::endl;
	}

	std::string nextCmd;
	unsigned int maxCommands = kMaxParallelOps;
	if (transactionSize_ > 0) {
		maxCommands = std::min(transactionSize_, maxCommands);
	}
	std::vector<std::pair<std::string, uint64_t>> parallelCommands;
	parallelCommands.reserve(maxCommands);
	int64_t lineNum = 0;

	while (std::getline(infile, nextCmd)) {
		++lineNum;
		if (reindexer::checkIfStartsWith("\\upsert ", nextCmd)) {
			parallelCommands.push_back({nextCmd, lineNum});
			if (parallelCommands.size() >= maxCommands) {
				throwIfError(parallelUpsertCommands(parallelCommands));
				parallelCommands.resize(0);
			}
		} else {
			if (!parallelCommands.empty()) {
				throwIfError(parallelUpsertCommands(parallelCommands));
				parallelCommands.resize(0);
			}

			if (Error err = process(nextCmd); !err.ok()) {
				printError(err, lineNum);
				throw err;
			}
		}
		if (inputFileSize) {
			progress.Print("Restoring dump", streamPositionToBytes(infile.tellg(), *inputFileSize), *inputFileSize);
		}
	}

	if (!parallelCommands.empty()) {
		throwIfError(parallelUpsertCommands(parallelCommands));
	}
	if (inputFileSize) {
		progress.Done("Restoring dump: done");
		progressFinished = true;
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::fromDumpFile(std::ifstream& infile, DumpFileIndex& dumpFileIdx) {
	fromFile_ = true;
	auto fromFileGuard = reindexer::MakeScopeGuard([this]() { this->fromFile_ = false; });

	targetHasReplicationConfig_ = isHavingReplicationConfig();
	if (targetHasReplicationConfig_) {
		output_() << "Target database has replication configured, so corresponding configs will not be overridden" << std::endl;
	}

	for (const std::pair<std::string, uint64_t>& nextCmd : dumpFileIdx.GetHeadCommands()) {
		size_t lineNum = nextCmd.second;
		if (Error err = process(nextCmd.first); !err.ok()) {
			printError(err, lineNum);
			throw err;
		}
	}

	std::vector<Error> errs(numThreads_);
	std::atomic<bool> abort{false};
	ConsoleProgress progress;
	std::atomic<bool> progressDone{false};
	std::thread progressThread([&] {
		while (!progressDone.load(std::memory_order_relaxed)) {
			progress.Print("Restoring dump upserts", dumpFileIdx.GetProgressSnapshot());
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
		}
		progress.Done(abort.load(std::memory_order_relaxed) ? "Restoring dump upserts: stopped" : "Restoring dump upserts: done");
	});
	auto stopProgress = [&] {
		progressDone.exchange(true, std::memory_order_relaxed);
		if (progressThread.joinable()) {
			progressThread.join();
		}
	};
	auto progressGuard = reindexer::MakeScopeGuard(stopProgress);
	auto workingThreadFun = [&](size_t threadIdx) {
		std::vector<std::pair<std::string, uint64_t>> parallelCommands;
		do {
			size_t nsUsed = 0;
			unsigned int maxCommands = kMaxParallelOps;
			if (transactionSize_ > 0) {
				maxCommands = std::min(transactionSize_, maxCommands);
			}
			errs[threadIdx] = dumpFileIdx.GetUpserts(infile, parallelCommands, maxCommands, nsUsed);
			if (!errs[threadIdx].ok()) {
				abort.store(true, std::memory_order_relaxed);
				return;
			}
			if (!parallelCommands.empty()) {
				errs[threadIdx] = parallelUpsertCommands(parallelCommands);
				dumpFileIdx.ProcessingEnded(nsUsed, parallelCommands.size());
				if (!errs[threadIdx].ok()) {
					abort.store(true, std::memory_order_relaxed);
					return;
				}
			}
		} while (!parallelCommands.empty() && !abort.load(std::memory_order_relaxed));
	};

	std::vector<std::thread> threads;
	threads.reserve(numThreads_);

	for (size_t i = 0; i < numThreads_; ++i) {
		threads.emplace_back(workingThreadFun, i);
	}

	for (auto& thread : threads) {
		thread.join();
	}
	stopProgress();

	for (auto& err : errs) {
		if (!err.ok()) {
			throw err;
		}
	}
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::getSuggestions(const std::string& input, reindexer::SQLSuggestions& suggestions) noexcept {
	try {
		if (!input.empty() && input[0] != '\\') {
			Error err = db().GetSqlSuggestions(input, input.length() - 1, suggestions);
			if (!err.ok()) {
				return err;
			}
		}
		if (suggestions.suggestions.empty()) {
			addCommandsSuggestions(input, suggestions.suggestions);
		}
		return errOK;
	} CATCH_AND_RETURN;
}

template <>
Error CommandsProcessor<reindexer::client::Reindexer>::getAvailableDatabases(std::vector<std::string>& dbList) noexcept {
	return db().EnumDatabases(dbList);
}

template <>
Error CommandsProcessor<reindexer::Reindexer>::getAvailableDatabases(std::vector<std::string>&) noexcept {
	return errOK;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::checkForNsNameMatch(std::string_view str, std::vector<std::string>& suggestions) {
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
void CommandsProcessor<DBInterface>::checkForCommandNameMatch(std::string_view str, std::initializer_list<std::string_view> cmds,
															  std::vector<std::string>& suggestions) {
	for (std::string_view cmd : cmds) {
		if (str.empty() || reindexer::isBlank(str) || ((str.length() < cmd.length()) && reindexer::checkIfStartsWith(str, cmd))) {
			suggestions.emplace_back(cmd);
		}
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::addCommandsSuggestions(const std::string& cmd, std::vector<std::string>& suggestions) {
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
		for (const CommandDefinition& cmdDef : cmds_) {
			if (token.empty() || reindexer::isBlank(token) ||
				((token.length() < cmdDef.command.length()) && reindexer::checkIfStartsWith(token, cmdDef.command))) {
				suggestions.emplace_back(cmdDef.command[0] == '\\' ? cmdDef.command.substr(1) : cmdDef.command);
			}
		}
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::queryResultsToJson(std::ostream& o, const typename DBInterface::QueryResultsT& r, bool isWALQuery,
														bool fstream) {
	if (cancelCtx_.IsCancelled()) {
		return;
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
		throwIfError(it.Status());
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
			rec.Dump(ser, [this, &r](std::string_view cjson) {
				auto item = db().NewItem(r.GetNamespaces()[0]);
				item.FromCJSONImpl(cjson);
				return std::string(item.GetJSON());
			});
		} else {
			if (isWALQuery) {
				ser << "WalItemUpdate ";
			}

			if (prettyPrint) {
				WrSerializer json;
				throwIfError(it.GetJSON(json, false));
				prettyPrintJSON(reindexer::giftStr(json.Slice()), ser);
			} else {
				throwIfError(it.GetJSON(ser, false));
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
void CommandsProcessor<DBInterface>::commandSelectSQL(std::string_view command) {
	int flags = kResultsWithPayloadTypes | kResultsCJson | kResultsWithItemID | kResultsWithRaw;
	if (variables_[kVariableWithShardId] == "on") {
		flags |= kResultsNeedOutputShardId | kResultsWithShardId;
	}
	typename DBInterface::QueryResultsT results(flags);
	const auto q = QueryImpl::FromSQL(command);
	throwIfError(db().Select(q, results));

	if (results.Count()) {
		auto& outputType = variables_[kVariableOutput];
		if (outputType == kOutputModeTable) {
			auto jsonData = ToJSONVector(results);
			auto isCanceled = [this]() -> bool { return cancelCtx_.IsCancelled(); };

			rx_tv::TableViewBuilder tableResultsBuilder;
			if (output_.IsCout() && !reindexer::isStdoutRedirected()) {
				TableViewScroller resultsScroller(tableResultsBuilder, reindexer::getTerminalSize().height - 1);
				resultsScroller.Scroll(output_, std::move(jsonData), isCanceled);
			} else {
				tableResultsBuilder.Build(output_(), std::move(jsonData), isCanceled,
										  rx_tv::kRemoveRareColumns | rx_tv::kRemoveEmptyColumns);
			}
		} else {
			if (!cancelCtx_.IsCancelled()) {
				output_() << "[" << std::endl;
				queryResultsToJson(output_(), results, q.IsWALQuery(), !output_.IsCout());
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
			switch (agg.GetType()) {
				case AggFacet: {
					const auto& fields = agg.GetFields();
					assertrx(!fields.empty());
					reindexer::h_vector<int, 1> maxW;
					maxW.reserve(fields.size());
					for (const auto& field : fields) {
						maxW.emplace_back(field.length());
					}
					for (auto& row : agg.GetFacets()) {
						assertrx(row.values.size() == fields.size());
						for (size_t i = 0; i < row.values.size(); ++i) {
							maxW.at(i) = std::max(maxW.at(i), int(row.values[i].length()));
						}
					}
					int rowWidth = 8 + (maxW.size() - 1) * 2;
					for (auto& mW : maxW) {
						mW += 3;
						rowWidth += mW;
					}
					for (size_t i = 0; i < fields.size(); ++i) {
						if (i != 0) {
							output_() << "| ";
						}
						output_() << std::left << std::setw(maxW.at(i)) << fields[i];
					}
					output_() << "| count" << std::endl;
					output_() << std::left << std::setw(rowWidth) << std::setfill('-') << "" << std::endl << std::setfill(' ');
					for (auto& row : agg.GetFacets()) {
						for (size_t i = 0; i < row.values.size(); ++i) {
							if (i != 0) {
								output_() << "| ";
							}
							output_() << std::left << std::setw(maxW.at(i)) << row.values[i];
						}
						output_() << "| " << row.count << std::endl;
					}
				} break;
				case AggDistinct: {
					output_() << "Distinct (";
					bool comma = false;
					for (const auto& f : agg.GetFields()) {
						output_() << (comma ? ", " : "") << f;
						comma = true;
					}
					output_() << ')' << std::endl;
					const unsigned int nRows = agg.GetDistinctRowCount();
					const unsigned int nColumn = agg.GetDistinctColumnCount();
					for (unsigned int i = 0; i < nRows; i++) {
						comma = false;
						for (unsigned j = 0; j < nColumn; j++) {
							output_() << (comma ? ", " : "") << agg.AsSingleString(i, j);
							comma = true;
						}
						output_() << std::endl;
					}
					output_() << "Returned " << nRows << " values" << std::endl;
				} break;
				case AggSum:
				case AggAvg:
				case AggMin:
				case AggMax:
				case AggCount:
				case AggCountCached:
				case AggUnknown:
					assertrx(agg.GetFields().size() == 1);
					output_() << reindexer::AggTypeToStr(agg.GetType()) << '(' << agg.GetFields().front() << ") = " << agg.GetValueOrZero()
							  << std::endl;
			}
		}
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandUpsert(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	const std::string nsName = reindexer::unescapeString(parser.NextToken());

	if (!parser.CurPtr().empty() && (parser.CurPtr())[0] == '[') {
		throw Error(errParams, "Impossible to update entire item with array - only objects are allowed");
	}

	auto item = db().NewItem(nsName);
	throwIfError(item.Status());

	using namespace std::string_view_literals;
	if (fromFile_ && std::string_view(nsName) == reindexer::kConfigNamespace) {
		try {
			gason::JsonParser p;
			auto root = p.Parse(parser.CurPtr());
			const auto type = root["type"].As<std::string_view>();
			if (type == "action"sv) {
				return;
			}
			if (type == "async_replication"sv || type == "replication"sv) {
				if (targetHasReplicationConfig_) {
					output_() << "Skipping #config item: " << type << std::endl;
					return;
				}
			} else if (type == "sharding"sv) {
				output_() << "Skipping #config item: " << type << " (sharding config is read-only)" << std::endl;
				return;
			}
		} catch (const gason::Exception& ex) {
			throw Error(errParseJson, "Unable to parse JSON for #config item: {}", ex.what());
		}
	}

	throwIfError(item.Unsafe().FromJSON(parser.CurPtr()));
	throwIfError(db().Upsert(nsName, item));

	if (!fromFile_) {
		output_() << "Upserted successfully: 1 items" << std::endl;
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::parseCommand(const std::string& command, std::string& nsName, std::string& cmdBody, bool& needSkip) {
	needSkip = false;
	cmdBody.resize(0);
	LineParser parser(command);
	std::ignore = parser.NextToken();
	nsName = reindexer::unescapeString(parser.NextToken());

	if (!parser.CurPtr().empty() && (parser.CurPtr())[0] == '[') {
		throw Error(errParams, "Impossible to update entire item with array - only objects are allowed");
	}

	using namespace std::string_view_literals;

	if (std::string_view(nsName) == reindexer::kConfigNamespace) {
		gason::JsonParser p;
		auto root = p.Parse(parser.CurPtr());
		const std::string type = root["type"].As<std::string>();
		if (type == "action"sv) {
			needSkip = true;
			return;
		}
		if (targetHasReplicationConfig_ && (type == "async_replication"sv || type == "replication"sv)) {
			output_() << "Skipping #config item: " << type << std::endl;
			needSkip = true;
			return;
		}
		if (type == "sharding"sv) {
			output_() << "Skipping #config item: " << type << " (sharding config is read-only)" << std::endl;
			needSkip = true;
			return;
		}
	}

	cmdBody = parser.CurPtr();
}

template <typename DBInterface>
Error CommandsProcessor<DBInterface>::parallelUpsertCommands(const std::vector<std::pair<std::string, uint64_t>>& commands) noexcept {
	try {
		assertrx_throw(fromFile_);
		Error lastErr;
		WaitGroup<DBInterface> wg;
		auto cleanup = reindexer::MakeScopeGuard([&wg]() { wg.Wait(); });

		std::vector<typename DBInterface::ItemType> items(commands.size());
		std::vector<std::string> nsNames(commands.size());

		typedef typename DBInterface::TransactionType TransactionType;
		std::unique_ptr<TransactionType> tr;
		typename DBInterface::QueryResultsT qr;
		std::string_view trNsName;
		size_t trSize = 0;
		std::string cmdBody;

		for (size_t i = 0; i < commands.size(); ++i) {
			const std::string& command = commands[i].first;
			const size_t lineNum = commands[i].second;

			bool needSkip = false;
			parseCommand(command, nsNames[i], cmdBody, needSkip);

			if (needSkip) {
				continue;
			}

			if (trSize > 0 && (trSize >= transactionSize_ || nsNames[i] != trNsName)) {
				wg.Wait();
				if (Error err = wg.Error(); !err.ok()) {
					lastErr = err;
				}

				if (Error err = db().CommitTransaction(*tr, qr); !err.ok()) {
					printError(err);
					return err;
				}

				trSize = 0;
			}

			bool useTransaction = transactionSize_ > 0 && std::string_view(nsNames[i]) != reindexer::kConfigNamespace;
			if (useTransaction) {
				if (trSize == 0) {
					trNsName = nsNames[i];
					tr = std::make_unique<TransactionType>(db().NewTransaction(trNsName));
				}
				items[i] = tr->NewItem();
				++trSize;
			} else {
				items[i] = db().NewItem(nsNames[i]);
			}
			items[i].SetPrecepts({"*=skip_embedding()"});

			if (Error err = items[i].Status(); !err.ok()) {
				printError(err, lineNum);
				lastErr = err;
				continue;
			}

			if (Error err = items[i].Unsafe().FromJSON(cmdBody); !err.ok()) {
				printError(err, lineNum);
				lastErr = err;
				continue;
			}

			auto completeCallback = [&wg, lineNum](const Error& e) {
				if (!e.ok()) {
					printError(e, lineNum);
				}
				wg.Done(e);
			};

			Error err;
			if (useTransaction) {
				err = tr->Upsert(std::move(items[i]), completeCallback);
			} else {
				err = db().WithCompletion(std::move(completeCallback)).Upsert(nsNames[i], items[i]);
			}

			if (!err.ok()) {
				printError(err);
			} else {
				wg.Add(1);
			}
		}

		wg.Wait();
		if (Error err = wg.Error(); !err.ok()) {
			lastErr = err;
		}

		if (lastErr.ok() && trSize > 0) {
			if (Error err = db().CommitTransaction(*tr, qr); !err.ok()) {
				printError(err);
				return err;
			}

			return errOK;
		}

		return lastErr;
	} CATCH_AND_RETURN;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandUpdateSQL(std::string_view command) {
	typename DBInterface::QueryResultsT results;
	auto q = QueryImpl::FromSQL(command);
	throwIfError(db().Update(q, results));
	output_() << "Updated " << results.Count() << " documents" << std::endl;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandDelete(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	const auto nsName = reindexer::unescapeString(parser.NextToken());

	auto item = db().NewItem(nsName);
	throwIfError(item.Status());
	throwIfError(item.Unsafe().FromJSON(parser.CurPtr()));
	throwIfError(db().Delete(nsName, item));
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandDeleteSQL(std::string_view command) {
	typename DBInterface::QueryResultsT results;
	auto q = QueryImpl::FromSQL(command);
	throwIfError(db().Delete(q, results));
	output_() << "Deleted " << results.Count() << " documents" << std::endl;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandDump(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	std::vector<NamespaceDef> allNsDefs, doNsDefs;
	const auto dumpMode = dumpMode_.load();

	auto err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().HideTemporary());
	throwIfError(err);
	filterNamespacesByDumpMode(allNsDefs, dumpMode);

	if (!parser.End()) {
		// build list of namespaces for dumped
		while (!parser.End()) {
			auto ns = parser.NextToken();
			auto nsDef = std::find_if(allNsDefs.begin(), allNsDefs.end(), [&ns](const NamespaceDef& nsDef) { return ns == nsDef.name; });
			if (nsDef != allNsDefs.end()) {
				doNsDefs.emplace_back(std::move(*nsDef));
				allNsDefs.erase(nsDef);
			} else {
				std::cerr << "Namespace '" << ns << "' - skipped (not found in storage)" << std::endl;
			}
		}
	} else {
		doNsDefs = std::move(allNsDefs);
	}

	reindexer::WrSerializer wrser;
	MD5 checksum;

	wrser << "-- Reindexer DB backup file" << '\n';
	wrser << "-- VERSION 1.1" << '\n';
	wrser << kDumpModePrefix;
	DumpOptions opts;
	opts.mode = dumpMode;
	opts.GetJSON(wrser);
	wrser << '\n';

	auto parametrizedDb = (dumpMode == DumpOptions::Mode::ShardedOnly) ? db() : db().WithShardId(ShardingKeyType::ProxyOff, false);

	std::ranges::sort(doNsDefs, [&](const NamespaceDef& lhs, const NamespaceDef& rhs) { return lhs.name < rhs.name; });
	for (auto& nsDef : doNsDefs) {
		// skip system namespaces, except #config
		if (reindexer::isSystemNamespaceNameFast(nsDef.name) && nsDef.name != reindexer::kConfigNamespace) {
			continue;
		}

		wrser << "-- Dumping namespace '" << nsDef.name << "' ..." << '\n';

		{
			WrSerializer namespacesLine;
			namespacesLine << "\\NAMESPACES ADD " << reindexer::escapeString(nsDef.name) << " ";
			nsDef.GetJSON(namespacesLine);
			appendCommandToChecksum(checksum, namespacesLine.Slice());
			wrser << namespacesLine.Slice() << '\n';
		}

		std::vector<std::string> meta;
		throwIfError(parametrizedDb.EnumMeta(nsDef.name, meta));
		std::sort(meta.begin(), meta.end());

		std::string mdata;
		for (auto& mkey : meta) {
			mdata.clear();
			const bool isSerial = reindexer::checkIfStartsWith<reindexer::CaseSensitive::Yes>(kSerialPrefix, mkey);
			if (isSerial) {
				getMergedSerialMeta(parametrizedDb, nsDef.name, mkey, mdata);
			} else {
				throwIfError(parametrizedDb.GetMeta(nsDef.name, mkey, mdata));
			}

			WrSerializer metaLine;
			metaLine << "\\META PUT " << reindexer::escapeString(nsDef.name) << " " << reindexer::escapeString(mkey) << " "
					 << reindexer::escapeString(mdata);
			appendCommandToChecksum(checksum, metaLine.Slice());
			wrser << metaLine.Slice() << '\n';
		}

		typename DBInterface::QueryResultsT itemResults;
		err = parametrizedDb.Select(*reindexer::impl::Impl{Query(nsDef.name).SelectAllFields()}, itemResults);
		throwIfError(err);

		for (auto it : itemResults) {
			throwIfError(it.Status());
			if (cancelCtx_.IsCancelled()) {
				throw Error(errCanceled, "Canceled");
			}
			WrSerializer upsertLine;
			upsertLine << "\\UPSERT " << reindexer::escapeString(nsDef.name) << ' ';
			throwIfError(it.GetJSON(upsertLine, false));
			appendCommandToChecksum(checksum, upsertLine.Slice());
			wrser << upsertLine.Slice() << '\n';

			if (wrser.Len() > 0x100000) {
				output_() << wrser.Slice();
				wrser.Reset();
			}
		}
	}
	output_() << wrser.Slice();
	output_() << kChecksumPrefix << " \"" << checksum.getHash() << "\"\n";
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandNamespaces(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	std::string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "add")) {
		std::ignore = parser.NextToken();  // nsName

		NamespaceDef def("");
		Error err = def.FromJSON(reindexer::giftStr(parser.CurPtr()));
		if (!err.ok()) {
			throw Error(errParseJson, "Namespace structure is not valid - {}", err.what());
		}

		def.storage.DropOnFileFormatError(true);
		def.storage.CreateIfMissing(true);

		throwIfError(db().OpenNamespace(def.name));
		for (auto& idx : def.indexes) {
			throwIfError(db().AddIndex(def.name, idx));
		}
		throwIfError(db().SetSchema(def.name, def.schemaJson));
	} else if (iequals(subCommand, "list")) {
		std::vector<NamespaceDef> allNsDefs;

		auto err = db().EnumNamespaces(allNsDefs, reindexer::EnumNamespacesOpts().WithClosed());
		for (auto& ns : allNsDefs) {
			output_() << ns.name << std::endl;
		}
		throwIfError(err);
	} else if (iequals(subCommand, "drop")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		throwIfError(db().DropNamespace(nsName));
	} else if (iequals(subCommand, "truncate")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		throwIfError(db().TruncateNamespace(nsName));
	} else if (iequals(subCommand, "rename")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		auto nsNewName = reindexer::unescapeString(parser.NextToken());
		throwIfError(db().RenameNamespace(nsName, nsNewName));
	} else {
		throw Error(errParams, "Unknown sub command '{}' of namespaces command", subCommand);
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandMeta(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();
	std::string_view subCommand = parser.NextToken();

	if (iequals(subCommand, "put")) {
		std::string nsName = reindexer::unescapeString(parser.NextToken());
		std::string metaKey = reindexer::unescapeString(parser.NextToken());
		std::string metaData = reindexer::unescapeString(parser.NextToken());
		throwIfError(parametrizedDb().PutMeta(nsName, metaKey, metaData));
	} else if (iequals(subCommand, "delete")) {
		std::string nsName = reindexer::unescapeString(parser.NextToken());
		std::string metaKey = reindexer::unescapeString(parser.NextToken());
		throwIfError(db().DeleteMeta(nsName, metaKey));
	} else if (iequals(subCommand, "list")) {
		auto nsName = reindexer::unescapeString(parser.NextToken());
		std::vector<std::string> allMeta;
		throwIfError(db().EnumMeta(nsName, allMeta));

		for (auto& metaKey : allMeta) {
			std::string metaData;
			throwIfError(db().GetMeta(nsName, metaKey, metaData));
			output_() << reindexer::escapeString(metaKey) << " = " << reindexer::escapeString(metaData) << std::endl;
		}
	} else {
		throw Error(errParams, "Unknown sub command '{}' of meta command", subCommand);
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandHelp(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();
	std::string_view subCommand = parser.NextToken();

	if (!subCommand.length()) {
		output_() << "Available commands:\n\n";
		for (const auto& cmd : cmds_) {
			output_() << "  " << std::left << std::setw(20) << cmd.command << "- " << cmd.description << std::endl;
		}
	} else {
		auto it = std::find_if(cmds_.begin(), cmds_.end(),
							   [&subCommand](const CommandDefinition& def) { return iequals(def.command, subCommand); });

		if (it == cmds_.end()) {
			throw Error(errParams, "Unknown command '{}' to help. To list of available command type '\\help'", subCommand);
		}
		output_() << it->command << " - " << it->description << ":" << std::endl << it->help << std::endl;
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandVersion(std::string_view) {
	std::string version;
	throwIfError(db().Version(version));
	output_() << "Reindexer version: " << version << std::endl;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandQuit(std::string_view) {
	quitCmdAccepted_ = true;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandSet(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	std::string_view variableName = parser.NextToken();
	std::string_view variableValue = parser.NextToken();

	variables_[std::string(variableName)] = std::string(variableValue);

	WrSerializer wrser;
	JsonBuilder configBuilder(wrser);
	for (auto it = variables_.begin(); it != variables_.end(); ++it) {
		configBuilder.Put(it->first, it->second);
	}
	configBuilder.End();
	const auto cfgPath = reindexer::fs::JoinPath(reindexer::fs::GetHomeDir(), kConfigFile);
	if (reindexer::fs::WriteFile(cfgPath, wrser.Slice()) < 0) {
		throw Error(errLogic, "Unable to write config file: '{}'", cfgPath);
	}
}

template <>
void CommandsProcessor<reindexer::client::Reindexer>::commandProcessDatabases(std::string_view command) {
	using namespace std::string_view_literals;
	LineParser parser(command);
	std::ignore = parser.NextToken();
	std::string_view subCommand = parser.NextToken();
	assertrx(uri_.scheme() == "cproto"sv || uri_.scheme() == "cprotos"sv || uri_.scheme() == "ucproto"sv);
	if (subCommand == "list"sv) {
		std::vector<std::string> dbList;
		throwIfError(getAvailableDatabases(dbList));
		for (const std::string& dbName : dbList) {
			output_() << dbName << std::endl;
		}
	} else if (subCommand == "use"sv) {
		reindexer::DSN currentDsn = getCurrentDsn().WithDb(std::string(parser.NextToken()));
		db().Stop();
		throwIfError(db().Connect(currentDsn));
		throwIfError(db().Status());
		output_() << "Successfully connected to " << currentDsn << std::endl;
	} else if (subCommand == "create"sv) {
		auto dbName = parser.NextToken();
		reindexer::DSN currentDsn = getCurrentDsn().WithDb(std::string(dbName));
		db().Stop();
		output_() << "Creating database '" << dbName << "'" << std::endl;
		Error err = db().Connect(currentDsn, reindexer::client::ConnectOpts().CreateDBIfMissing());
		if (!err.ok()) {
			std::cerr << "Error on database '" << dbName << "' creation" << std::endl;
			throw err;
		}
		std::vector<std::string> dbNames;
		err = db().EnumDatabases(dbNames);
		if (std::find(dbNames.begin(), dbNames.end(), std::string(dbName)) != dbNames.end()) {
			output_() << "Successfully created database '" << dbName << "'" << std::endl;
		} else {
			std::cerr << "Error on database '" << dbName << "' creation" << std::endl;
		}
		throwIfError(err);
	} else {
		throw Error(errNotValid, "Invalid command");
	}
}

template <>
void CommandsProcessor<reindexer::Reindexer>::commandProcessDatabases(std::string_view) {
	throw Error(errNotValid, "Database processing commands are not supported in builtin mode");
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::filterNamespacesByDumpMode(std::vector<NamespaceDef>& defs, DumpOptions::Mode mode) {
	if (mode == DumpOptions::Mode::FullNode) {
		return;
	}

	typename DBInterface::QueryResultsT qr;
	throwIfError(db().Select(*reindexer::impl::Impl{Query(reindexer::kConfigNamespace).Where("type", CondEq, "sharding")}, qr));

	if (qr.Count() != 1) {
		output_() << "Sharding is not enabled, however non-default dump mode is detected. That's weird...";
		return;
	}
	using reindexer::cluster::ShardingConfig;
	ShardingConfig cfg;
	WrSerializer ser;
	throwIfError(qr.begin().GetJSON(ser, false));

	auto json = reindexer::giftStr(ser.Slice());
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		throwIfError(cfg.FromJSON(root[reindexer::kShardingCfgName]));
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "Unable to parse sharding config: {}", ex.what());
	}

	if (mode == DumpOptions::Mode::LocalOnly) {
		for (auto& shNs : cfg.namespaces) {
			const auto found =
				std::find_if(defs.begin(), defs.end(), [&shNs](const NamespaceDef& nsDef) { return iequals(nsDef.name, shNs.ns); });
			if (found != defs.end()) {
				defs.erase(found);
			}
		}
	} else {
		defs.erase(std::remove_if(defs.begin(), defs.end(),
								  [&cfg](const NamespaceDef& nsDef) {
									  const auto found = std::find_if(
										  cfg.namespaces.begin(), cfg.namespaces.end(),
										  [&nsDef](const ShardingConfig::Namespace& shNs) { return iequals(nsDef.name, shNs.ns); });
									  return found == cfg.namespaces.end();
								  }),
				   defs.end());
	}
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::commandBench(std::string_view command) {
	LineParser parser(command);
	std::ignore = parser.NextToken();

	const std::string_view benchTimeToken = parser.NextToken();
	const int benchTime = benchTimeToken.empty() ? kBenchDefaultTime : reindexer::stoi(benchTimeToken);

	auto err = db().DropNamespace(kBenchNamespace);
	if (!err.ok() && err.code() != errNotFound) {
		throw err;
	}

	NamespaceDef nsDef(kBenchNamespace);
	nsDef.AddIndex("id", "hash", "int", reindexer::IndexOpts().PK());

	throwIfError(db().AddNamespace(nsDef));
	output_() << "Seeding " << kBenchItemsCount << " documents to bench namespace..." << std::endl;
	seedBenchItems();
	output_() << "done." << std::endl;

	output_() << "Running " << benchTime << "s benchmark..." << std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(1));
	const auto numThreads = std::min(std::max(numThreads_, 1u), 65535u);
	bench(numThreads, benchTime);
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::seedBenchItems() {
	WaitGroup<DBInterface> wg;
	std::vector<WrSerializer> sers(kMaxParallelOps);
	std::vector<typename DBInterface::ItemType> items(kMaxParallelOps);
	size_t numCollected = 0;

	auto upsertAllCollectedItems = [&]() -> Error {
		if (numCollected <= 0) {
			return errOK;
		}
		auto cleanup = reindexer::MakeScopeGuard([&wg]() { wg.Wait(); });

		for (size_t j = 0; j < numCollected; ++j) {
			Error errDB = db().WithCompletion([&wg](const Error& e) { wg.Done(e); }).Upsert(kBenchNamespace, items[j]);
			if (!errDB.ok()) {
				return errDB;
			} else {
				wg.Add(1);
			}
		}

		wg.Wait();
		numCollected = 0;
		return wg.Error();
	};

	for (int i = 0; i < kBenchItemsCount; i++) {
		if (numCollected >= kMaxParallelOps) {
			throwIfError(upsertAllCollectedItems());
		}

		items[numCollected] = db().NewItem(kBenchNamespace);
		WrSerializer& ser = sers[numCollected];
		ser.Reset();
		auto bld = JsonBuilder(ser);
		bld.Put("id", i);
		bld.Put("data", i);
		bld.End();
		throwIfError(items[numCollected].Unsafe().FromJSON(ser.Slice()));
		++numCollected;
	}

	throwIfError(upsertAllCollectedItems());
}

template <>
void CommandsProcessor<reindexer::client::Reindexer>::bench(unsigned int numThreads, int benchTime) {
	auto deadline = reindexer::system_clock_w::now_coarse() + std::chrono::seconds(benchTime);
	std::atomic<int> count(0), errCount(0);
	reindexer::client::Reindexer rx(reindexer::client::ReindexerConfig(), numThreads, numThreads);
	const auto dsn = getCurrentDsn(true);
	if (Error err = rx.Connect(dsn); !err.ok()) {
		output_() << "[bench] Unable to connect with provided DSN '" << dsn << "': " << err.what() << std::endl;
		rx.Stop();
		return;
	}

	std::vector<reindexer::client::Reindexer::QueryResultsT> results(kMaxParallelOps);
	std::vector<Query> queries(kMaxParallelOps, Query{kBenchNamespace});
	WaitGroup<reindexer::client::Reindexer> wg;
	auto cleanup = reindexer::MakeScopeGuard([&wg]() { wg.Wait(); });

	while (reindexer::system_clock_w::now_coarse() < deadline) {
		for (int j = 0; j < kMaxParallelOps; j++, count++) {
			queries[j] = Query(kBenchNamespace).Where(kBenchIndex, CondEq, count % kBenchItemsCount);
			results[j] = reindexer::client::Reindexer::QueryResultsT();

			auto err = rx.WithCompletion([&wg, &errCount](const Error& e) {
							 wg.Done(e);
							 if (!e.ok()) {
								 ++errCount;
							 }
						 }).Select(*reindexer::impl::Impl<const Query&>{queries[j]}, results[j]);
			if (!err.ok()) {
				++errCount;
			} else {
				wg.Add(1);
			}
		}

		wg.Wait();
	}

	output_() << "Done. Got " << count / benchTime << " QPS, " << errCount << " errors" << std::endl;
	rx.Stop();
}

template <>
void CommandsProcessor<reindexer::Reindexer>::bench(unsigned int numThreads, int benchTime) {
	auto deadline = reindexer::system_clock_w::now_coarse() + std::chrono::seconds(benchTime);
	std::atomic<int> count(0), errCount(0);

	auto worker = [&]() {
		for (; ((count & 0x3FF) == 0) || reindexer::system_clock_w::now_coarse() < deadline; count++) {
			Query q(kBenchNamespace);
			q.Where(kBenchIndex, CondEq, count % kBenchItemsCount);
			auto results = new typename reindexer::Reindexer::QueryResultsT;

			const auto err = db().WithCompletion([results, &errCount](const Error& err) {
									 delete results;
									 if (!err.ok()) {
										 ++errCount;
									 }
								 })
								 .Select(q, *results);
			if (!err.ok()) {
				++errCount;
			}
		}
	};

	auto threads = std::unique_ptr<std::thread[]>(new std::thread[numThreads]);
	for (unsigned i = 0; i < numThreads; i++) {
		threads[i] = std::thread(worker);
	}
	for (unsigned i = 0; i < numThreads; i++) {
		threads[i].join();
	}

	output_() << "Done. Got " << count / benchTime << " QPS, " << errCount << " errors" << std::endl;
}

template <typename DBInterface>
void CommandsProcessor<DBInterface>::getMergedSerialMeta(DBInterface& db, std::string_view nsName, const std::string& key,
														 std::string& result) {
	std::vector<reindexer::ShardedMeta> meta;
	throwIfError(db.GetMeta(nsName, key, meta));

	auto safeStoll = [](const std::string& str) -> int64_t {
		try {
			return std::stoll(str);
		} catch (...) {
			return 0;
		}
	};

	int64_t maxVal = 0;
	for (auto& sm : meta) {
		maxVal = std::max(maxVal, safeStoll(sm.data));
	}
	result = std::to_string(maxVal);
}

template <typename DBInterface>
reindexer::DSN CommandsProcessor<DBInterface>::getCurrentDsn(bool withPath) const {
	using namespace std::string_view_literals;
	std::string dsn(uri_.scheme() + "://");
	if (!uri_.password().empty() && !uri_.username().empty()) {
		dsn += uri_.username() + ':' + uri_.password() + '@';
	}
	if (uri_.scheme() == "ucproto"sv) {
		std::vector<std::string_view> pathParts;
		std::ignore = reindexer::split(std::string_view(uri_.path()), ":", true, pathParts);
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

template class CommandsProcessor<reindexer::client::Reindexer>;
template class CommandsProcessor<reindexer::Reindexer>;
template Error CommandsProcessor<reindexer::Reindexer>::Connect(const std::string& dsn, const ConnectOpts& opts);
template Error CommandsProcessor<reindexer::client::Reindexer>::Connect(const std::string& dsn, const reindexer::client::ConnectOpts& opt);

}  // namespace reindexer_tool
