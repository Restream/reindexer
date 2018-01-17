#include <stdio.h>
#include <memory>

#include "core/reindexer.h"
#include "debug/backtrace.h"
#include "tools/logger.h"

#include "cxxopts/cxxopts.hpp"

template <typename T>
using scoped_ptr = std::unique_ptr<T, std::function<void(T*)>>;

typedef scoped_ptr<FILE> scoped_file_ptr;

static auto db = std::make_shared<reindexer::Reindexer>();

using std::string;

static struct config {
	enum actions { dump = 1, query = 2, dlt = 4, upsert = 8, meta = 16 };

	string value_;
	string ns_;
	string source_;
	string dest_;
	int logLevel_;
	int action_;
} config;

inline bool isStdout(string const& v) { return v == "stdout" || v.empty(); }

reindexer::Error parseOptions(int argc, char** argv, cxxopts::Options& opts) {
	opts.add_options()("h,help", "show this message");

	auto startGroup = opts.add_options("");

	startGroup("d,dump", "dump whole namespace", cxxopts::value<string>(config.ns_), "NAMESPACE");
	startGroup("q,query", "dump by query (in quotes)", cxxopts::value<string>(config.value_), "\"QUERY\"");
	startGroup("r,delete", "delete item from namespace (JSON object in quotes)", cxxopts::value<string>(config.value_), "\"JSON\"");
	startGroup("u,upsert", "Insert or update item in namespace (JSON object in quotes)", cxxopts::value<string>(config.value_), "\"JSON\"");
	startGroup("m,meta", "dump meta information by KEY from namespace", cxxopts::value<string>(config.value_), "KEY");
	startGroup("n,namespace", "needed for --delete, --upsert, --meta actions", cxxopts::value<string>(config.ns_), "NAMESPACE");

	opts.add_options("source")("db", "path to 'reindexer' cache", cxxopts::value<string>(config.source_)->implicit_value(""), "DIRECTORY");

	opts.add_options("dest")("o,out", "path to result output file (console by default)",
							 cxxopts::value<string>(config.dest_)->default_value("stdout"), "FILE");

	opts.add_options("logging")("l,log", "log level (MAX = 5)", cxxopts::value<int>(config.logLevel_)->default_value("3"), "INT");

	try {
		opts.parse(argc, argv);
	} catch (cxxopts::OptionException& exc) {
		return reindexer::Error(errParams, exc.what());
	}

	return reindexer::Error();
}

reindexer::Error validate(cxxopts::Options& opts) {
	try {
		int result_one_of = 0;

		// All checks for option 'db'
		{
			cxxopts::check_required(opts, {"db"});
			if (opts.count("db") > 1) {
				return reindexer::Error(errParams, "Error in use options (must be one). See help: \n%s\n", opts.help({"source"}).c_str());
			}

			if (config.source_.empty()) {
				return reindexer::Error(errParams, "Parameter 'db' could not be empty. See help: \n%s\n", opts.help({"source"}).c_str());
			}
		}

		// Check for start options
		result_one_of = opts.count("dump") + opts.count("query") + opts.count("upsert") + opts.count("delete") + opts.count("meta");

		if (result_one_of != 1) {
			return reindexer::Error(errParams, "Error in use options (must be one). See help: \n%s\n", opts.help(opts.groups()).c_str());
		}

		config.action_ = ((opts.count("dump") > 0 ? config::dump : 0) | (opts.count("meta") > 0 ? config::meta : 0) |
						  (opts.count("query") > 0 ? config::query : 0) | (opts.count("delete") > 0 ? config::dlt : 0) |
						  (opts.count("upsert") > 0 ? config::upsert : 0));

		switch (config.action_) {
			case config::dump:
				if (config.ns_.empty()) {
					return reindexer::Error(errParams, "For 'dump' required namespace name. See help: \n%s\n",
											opts.help({"start"}).c_str());
				}
				break;

			case config::query:
				if (opts.count("namespace") != 0) {
					return reindexer::Error(errParams, "For 'query' namespace is not needed. See help: \n%s\n",
											opts.help({"start"}).c_str());
				}

				if (config.value_.empty()) {
					return reindexer::Error(errParams, "Parameter 'query' could not be empty. See help: \n%s\n",
											opts.help({"start"}).c_str());
				}
				break;

			case config::dlt:
			case config::upsert:
			case config::meta:
				cxxopts::check_required(opts, {"namespace"});

				if (config.value_.empty()) {
					return reindexer::Error(errParams, "Parameter 'delete|upsert|meta' could not be empty. See help: \n%s\n",
											opts.help({"start"}).c_str());
				}
				break;

			default:
				return reindexer::Error(reindexer::Error(errParams, "Unknown actions. See help: \n%s\n", opts.help(opts.groups()).c_str()));
		}

		// Checks for 'out' parameter
		if (opts.count("out") > 1) {
			return reindexer::Error(errParams, "Error in use dest options. See help: \n%s\n", opts.help({"dest"}).c_str());
		}

		// Checks for 'log' parameter
		if (opts.count("log") > 1) {
			return reindexer::Error(errParams, "Error in use logging options. See help: \n%s\n", opts.help({"logging"}).c_str());
		}

	} catch (cxxopts::OptionException& exc) {
		return reindexer::Error(errParams, "%s. %s", exc.what(), opts.help(opts.groups()).c_str());
	}

	return reindexer::Error();
}

string extractNamespace(const string& str) {
	string delimiter(" ");
	string ns;
	std::vector<string> tokens;

	string::size_type lastPos = str.find_first_not_of(delimiter, 0);
	string::size_type pos = str.find_first_of(delimiter, lastPos);

	while (string::npos != pos || string::npos != lastPos) {
		tokens.push_back(str.substr(lastPos, pos - lastPos));

		lastPos = str.find_first_not_of(delimiter, pos);
		pos = str.find_first_of(delimiter, lastPos);
	}

	auto it = std::find(tokens.begin(), tokens.end(), "from");
	if ((it != tokens.end()) && (it++ != tokens.end())) {
		ns = *it;
	}

	return ns;
}

reindexer::Error prepareSelectQuery(string& query, reindexer::Query& q) {
	reindexer::Error result;
	try {
		q.Parse(query);
	} catch (const reindexer::Error& err) {
		result = err;
	}
	return result;
}

int openNamespace(const string& ns) {
	auto status = db->EnableStorage(config.source_);
	if (!status.ok()) {
		printf("Enable storage error: %s\n", status.what().c_str());
		return -1;
	}

	status = db->OpenNamespace(reindexer::NamespaceDef(ns, StorageOpts(true, false, false, false)));
	if (!status.ok()) {
		printf("Open namespace error: %s\n", status.what().c_str());
		return -1;
	}
	return 0;
}

int invokeRead(int action) {
	string ns;
	reindexer::Query q;

	auto status = reindexer::Error();

	if (action == config::dump) {
		ns = config.ns_;
		q = reindexer::Query(ns);
	}

	if (action == config::query) {
		ns = extractNamespace(config.value_);
		status = prepareSelectQuery(config.value_, q);
	}

	if (!status.ok()) {
		printf("Prepare query error: %s\n", status.what().c_str());
		return -1;
	}

	if (openNamespace(ns) < 0) {
		return -1;
	}

	scoped_file_ptr file;
	if (isStdout(config.dest_)) {
		file = scoped_file_ptr(stdout, [](FILE*) { ; });
	} else {
		file = scoped_file_ptr(fopen(config.dest_.c_str(), "w"), [](FILE* f) {
			if (f != nullptr) fclose(f);
		});
	}

	if (!file) {
		printf("Open file error: %s. Path: %s\n", strerror(errno), config.dest_.c_str());
		return -1;
	}

	reindexer::QueryResults res;
	status = db->Select(q, res);
	if (!status.ok()) {
		printf("Query error: %s\n", status.what().c_str());
		return -1;
	}

	string begin_data("{ \"items\": [ ");
	string total("], \"total_items\": ");
	string end_data("}");
	std::size_t total_items = 0;

	fwrite(begin_data.data(), 1, begin_data.size(), file.get());
	reindexer::WrSerializer ser;

	for (unsigned i = 0; i < res.size(); i++, total_items++) {
		ser.Reset();
		if (i != 0) ser.PutChar(',');
		res.GetJSON(i, ser, false);
		fwrite(ser.Buf(), 1, ser.Len(), file.get());
	}
	total += std::to_string(total_items);
	fwrite(total.data(), 1, total.size(), file.get());
	fwrite(end_data.data(), 1, end_data.size(), file.get());
	return 0;
}

int invokeWrite(int action) {
	if (openNamespace(config.ns_) < 0) {
		return -1;
	}

	reindexer::Slice json(config.value_);
	auto item = db->NewItem(config.ns_);
	auto status = item->FromJSON(json);

	if (!status.ok()) {
		printf("Item error: %s\n", status.what().c_str());
		return -1;
	}

	if (action == config::dlt) {
		status = db->Delete(config.ns_, item);
		if (!status.ok()) {
			printf("Delete error: %s\n", status.what().c_str());
			return -1;
		}
	}

	if (action == config::upsert) {
		status = db->Upsert(config.ns_, item);
		if (!status.ok()) {
			printf("Upsert error: %s\n", status.what().c_str());
			return -1;
		}
	}

	status = db->Commit(config.ns_);
	if (!status.ok()) {
		printf("Commit error: %s\n", status.what().c_str());
	} else {
		printf("%s", "Success!");
	}
	return 0;
}

int invokeMeta() {
	if (openNamespace(config.ns_) < 0) {
		return -1;
	}

	string data;
	auto status = db->GetMeta(config.ns_, config.value_, data);
	if (!status.ok()) {
		printf("Meta error: %s\n", status.what().c_str());
		return -1;
	}

	scoped_file_ptr file;
	if (isStdout(config.dest_)) {
		file = scoped_file_ptr(stdout, [](FILE*) { ; });
	} else {
		file = scoped_file_ptr(fopen(config.dest_.c_str(), "w"), [](FILE* f) {
			if (f != nullptr) fclose(f);
		});
	}

	if (!file) {
		printf("Open file error: %s. Path: %s\n", strerror(errno), config.dest_.c_str());
		return -1;
	}

	fwrite(data.data(), 1, data.size(), file.get());

	return 0;
}

int main(int argc, char* argv[]) {
	backtrace_init();
	cxxopts::Options opts("reindexer_dump", "");
	auto status = parseOptions(argc, argv, opts);

	if (!status.ok()) {
		printf("Parameters error: %s\n\n", status.what().c_str());
		printf("%s", opts.help(opts.groups()).c_str());
		return -1;
	}

	if (opts.count("help")) {
		printf("%s", opts.help(opts.groups()).c_str());
		return 0;
	}

	status = validate(opts);
	if (!status.ok()) {
		printf("%s", status.what().c_str());
		return -1;
	}

	reindexer::logInstallWriter([](int level, char* buf) {
		if (level <= config.logLevel_) {
			fprintf(stderr, "%s\n", buf);
		}
	});

	if (config.action_ == config::dump || config.action_ == config::query) {
		return invokeRead(config.action_);
	}

	if (config.action_ == config::dlt || config.action_ == config::upsert) {
		return invokeWrite(config.action_);
	}

	if (config.action_ == config::meta) {
		return invokeMeta();
	}

	printf("%s", opts.help(opts.groups()).c_str());

	return -1;
}
