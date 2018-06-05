#include "debug/backtrace.h"
#include "core/reindexer.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "vendor/gason/gason.h"

#include "args/args.hpp"
#include "iotools.h"

using std::string;
using std::vector;
using std::ifstream;
using std::shared_ptr;
using args::Options;

static auto db = std::make_shared<reindexer::Reindexer>();
int llevel;

void InstallLogLevel(const vector<string>& args) {
	try {
		llevel = std::stoi(args.back());
		if ((llevel < 1) || (llevel > 5)) {
			throw std::out_of_range("value must be in range 1..5");
		}
	} catch (std::invalid_argument&) {
		throw args::UsageError("Value must be integer.");
	} catch (std::out_of_range& exc) {
		std::cout << "WARNING: " << exc.what() << "\n"
				  << "Logging level set to 3" << std::endl;
		llevel = 3;
	}

	reindexer::logInstallWriter([](int level, char* buf) {
		if (level <= llevel) {
			fprintf(stdout, "%s\n", buf);
		}
	});
}

args::Group progOptions("options");
args::ValueFlag<string> storagePath(progOptions, "path", "path to 'reindexer' storage", {'s', "db"}, reindexer::fs::GetCwd(),
									Options::Single | Options::Global);

args::ActionFlag logLevel(progOptions, "INT=1..5", "reindexer logging level", {'l', "log"}, 1, &InstallLogLevel,
						  Options::Single | Options::Global);

void ListCommand(args::Subparser& subparser) {
	subparser.Parse();
	auto err = db->EnableStorage(args::get(storagePath));
	if (err) {
		std::cerr << err.what() << std::endl;
		return;
	}

	vector<reindexer::NamespaceDef> nsDefs;
	err = db->EnumNamespaces(nsDefs, true);
	if (err) {
		std::cout << "ERROR: " << err.what().c_str() << std::endl;
		return;
	}
	for (auto& nsDef : nsDefs) {
		std::cout << nsDef.name << std::endl;
	}
}

static std::vector<char> charsForEscaping = {'\n', '\t', ',', '\0', '\\'};

string escapeName(const string& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		auto findIt = std::find(charsForEscaping.begin(), charsForEscaping.end(), *it);

		if (findIt != charsForEscaping.end()) {
			dst.push_back('\\');
		}

		dst.push_back(*it);
	}

	return dst;
}

string unescapeName(const string& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it == '\\') {
			it++;

			auto findIt = std::find(charsForEscaping.begin(), charsForEscaping.end(), *it);
			if (findIt == charsForEscaping.end()) {
				dst.push_back('\\');
			}
		}

		dst.push_back(*it);
	}

	return dst;
}

void DumpCommand(args::Subparser& subparser) {
	args::Group dumpArgs(subparser, "dump options");
	args::PositionalList<string> namespaces(dumpArgs, "<namespace>", "list of namespaces which need to dump");
	args::ValueFlag<string> outFile(dumpArgs, "FILE", "output filename for dump namespace(s)", {'f', "file"}, "");
	args::HelpFlag dumpHelp(dumpArgs, "help", "help message for 'dump' command", {'h', "help"});
	args::Flag showProgress(dumpArgs, "progress", "show dump progress", {'p', "progress"}, args::Options::Single);

	subparser.Parse();

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	vector<reindexer::NamespaceDef> allNsDefs, doNsDefs;

	err = db->EnumNamespaces(allNsDefs, true);
	if (err) throw err;

	if (namespaces) {
		// build list of namespaces for dumped
		for (auto& ns : namespaces) {
			auto nsDef =
				std::find_if(allNsDefs.begin(), allNsDefs.end(), [&ns](const reindexer::NamespaceDef& nsDef) { return ns == nsDef.name; });
			if (nsDef != allNsDefs.end()) {
				doNsDefs.push_back(std::move(*nsDef));
				allNsDefs.erase(nsDef);
			} else {
				std::cout << "-- Namespace '" << ns << "' - skipped. (not found in storage)" << std::endl;
			}
		}
	} else {
		doNsDefs = std::move(allNsDefs);
	}

	string filename = args::get(outFile);
	ofstream fileDescriptor(filename, std::ios::out | std::ios::trunc);
	std::ostream& file = !filename.empty() ? fileDescriptor : std::cout;

	if (filename.length() && !fileDescriptor) {
		std::cerr << "-- ERROR: " << strerror(errno) << std::endl;
		return;
	}

	file << "-- Reindexer DB backup file" << std::endl;
	file << "VERSION 1.0" << std::endl;

	for (auto& nsDef : doNsDefs) {
		std::cout << "-- Dumping namespace '" << nsDef.name << "' ..." << std::endl;

		err = db->OpenNamespace(nsDef.name, StorageOpts().Enabled());
		if (err) {
			std::cerr << "-- ERROR: " << err.what() << std::endl;
			continue;
		}

		reindexer::WrSerializer wrser;
		nsDef.GetJSON(wrser);
		file << "NAMESPACE " << escapeName(nsDef.name) << " ";
		file.write(reinterpret_cast<char*>(wrser.Buf()), wrser.Len());
		file << "\n";

		vector<string> meta;
		err = db->EnumMeta(nsDef.name, meta);
		if (err) {
			std::cerr << "-- ERROR: " << err.what() << std::endl;
			continue;
		}
		for (auto& mkey : meta) {
			string mdata;
			err = db->GetMeta(nsDef.name, mkey, mdata);
			if (err) {
				std::cerr << "-- ERROR: " << err.what() << std::endl;
				continue;
			}
			file << "META " << escapeName(nsDef.name) << " " << escapeName(mkey) << " " << escapeName(mdata) << std::endl;
		}

		reindexer::Query q(nsDef.name);
		QueryResults itemResults;
		err = db->Select(q, itemResults);
		if (err) {
			std::cerr << "-- ERROR: " << err.what() << std::endl;
		}

		shared_ptr<ProgressPrinter> progressPrinter;
		if (fileDescriptor && showProgress) {
			progressPrinter = std::make_shared<ProgressPrinter>(itemResults.size());
		}

		reindexer::WrSerializer ser;
		for (size_t i = 0; i < itemResults.size(); i++) {
			ser.Reset();
			itemResults.GetJSON(i, ser, false);
			file << "INSERT " << escapeName(nsDef.name) << " ";
			file.write(reinterpret_cast<char*>(ser.Buf()), ser.Len());
			file << "\n";
			if (progressPrinter) progressPrinter->Show(i);
		}
		if (progressPrinter) progressPrinter->Finish();

		err = db->CloseNamespace(nsDef.name);
		if (err) {
			std::cerr << "-- ERROR: " << err.what() << std::endl;
			continue;
		}
	}

	if (fileDescriptor) {
		fileDescriptor.close();
	}
}

void RestoreCommand(args::Subparser& subparser) {
	args::Group restoreArgs(subparser, "RESTORE options");
	args::HelpFlag restoreHelp(restoreArgs, "restore", "show help message for 'RESTORE' command", {'h', "help"});
	args::ValueFlag<string> restorePath(restoreArgs, "dump file", "specify a dump file for restore", {'f', "file"}, "",
										args::Options::Single);
	args::Flag showProgress(restoreArgs, "print progress", "print restore progress", {'p', "progress"}, args::Options::Single);

	subparser.Parse();

	string path = args::get(restorePath);

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	ifstream fileDescriptor(path);
	std::istream& file = !path.empty() ? fileDescriptor : std::cin;
	if (path.length()) {
		if (!fileDescriptor) {
			std::cout << "ERROR: " << strerror(errno) << " [SKIP]" << std::endl;
		}
		std::cout << "Process file: " << path << std::endl;
	}

	shared_ptr<ProgressPrinter> progressPrinter;
	if (fileDescriptor && showProgress) {
		progressPrinter = std::make_shared<ProgressPrinter>();
	}

	if (progressPrinter) progressPrinter->Reset(GetStreamSize(file));

	int lineCount = -1;
	err = 0;
	for (string buffer; std::getline(file, buffer);) {
		lineCount++;

		LineParser parser(buffer);
		auto token = reindexer::lower(parser.NextToken());

		if (token == "namespace") {
			auto nsName = unescapeName(parser.NextToken());

			reindexer::NamespaceDef def("");
			err = def.FromJSON(const_cast<char*>(parser.CurPtr()));
			if (err) {
				std::cout << "ERROR: namespace structure is not valid [SKIP]" << std::endl;
				continue;
			}

			std::cout << "Restore namespace '" << nsName << "' ..." << std::endl;

			def.storage.DropOnFileFormatError(true);
			def.storage.CreateIfMissing(true);

			err = db->AddNamespace(def);
			if (err) {
				std::cout << "ERROR: " << err.what() << std::endl;
				continue;
			}
		} else if (token == "insert") {
			auto nsName = unescapeName(parser.NextToken());

			auto item = db->NewItem(nsName);
			err = item.Status();
			if (err) {
				std::cout << "ERROR: " << err.what() << std::endl;
				continue;
			}

			err = item.Unsafe().FromJSON(const_cast<char*>(parser.CurPtr()));
			if (err) {
				std::cout << "ERROR: " << err.what() << std::endl;
				continue;
			}

			err = db->Upsert(nsName, item);
			if (err) {
				std::cout << "ERROR: " << err.what() << std::endl;
				continue;
			}

			if (progressPrinter) progressPrinter->Show(file.tellg());
		} else if (token == "meta") {
			auto nsName = unescapeName(parser.NextToken());
			auto metaKey = unescapeName(parser.NextToken());
			auto metaData = unescapeName(parser.NextToken());
			err = db->PutMeta(nsName, metaKey, metaData);
			if (err) break;
		} else if (token == "--" || token.empty()) {
			continue;
		} else if (token == "version") {
			continue;
		} else {
			continue;
		}
	}

	if (err) {
		std::cout << "ERROR: " << err.what() << " [SKIP], at line " << lineCount << std::endl;
	}

	if (progressPrinter) progressPrinter->Finish();

	if (fileDescriptor) {
		fileDescriptor.close();
	}
}

void QueryCommand(args::Subparser& subparser) {
	args::Group queryArgs(subparser, "QUERY options");
	args::HelpFlag queryHelp(queryArgs, "help", "Show help message for 'query' command", {'h', "help"});
	args::Positional<string> sqlBody(queryArgs, "<SQL>", "SQL-like query text in quotes", args::Options::Single | args::Options::Required);
	args::ValueFlag<string> outputFile(queryArgs, "<PATH>", "Output file for dump query result", {'o', "file"});

	subparser.Parse();

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	reindexer::Query q;
	q.Parse(args::get(sqlBody));

	err = db->OpenNamespace(q.describe ? q.namespacesNames_.back() : q._namespace, StorageOpts().Enabled());
	if (err) throw err;

	reindexer::QueryResults results;
	err = db->Select(q, results);
	if (err) throw err;

	Output output(args::get(outputFile));
	output() << "[" << results << "]" << std::endl;
}

void MetaCommand(args::Subparser& subparser) {
	args::Group metaArgs(subparser, "META options");
	args::HelpFlag help(metaArgs, "help", "show help message for 'meta' command", {'h', "help"});
	args::ValueFlag<string> key(metaArgs, "key", "key name for dumping", {"key"}, args::Options::Required);
	args::Positional<string> ns(metaArgs, "<namespace>", "namespace", args::Options::Required);

	subparser.Parse();

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	err = db->OpenNamespace(args::get(ns), StorageOpts().Enabled());
	if (err) throw err;

	std::string data;
	err = db->GetMeta(args::get(ns), args::get(key), data);
	if (err) throw err;

	std::cout << data << std::endl;
}

void UpsertCommand(args::Subparser& subparser) {
	args::Group upsertGroup(subparser, "UPSERT options");
	args::HelpFlag upsertHelp(upsertGroup, "help", "show help message for 'UPSERT' command", {'h', "help"});
	args::Positional<string> ns(upsertGroup, "<namespace>", "namespace where the element will be inserted", args::Options::Required);
	args::Positional<string> itemJson(upsertGroup, "<JSON>", "item which will be upserted in JSON-syntax", args::Options::Required);
	args::Group upsertModes(subparser, "UPSERT modes", args::Group::Validators::Xor);
	args::Flag insert(upsertModes, "INSERT", "allow insert only", {"insert"});
	args::Flag update(upsertModes, "UPDATE", "allow update only", {"update"});

	subparser.Parse();

	auto json = args::get(itemJson);

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	err = db->OpenNamespace(args::get(ns), StorageOpts().Enabled());
	if (err) throw err;

	auto item = db->NewItem(args::get(ns));
	err = item.Status();
	if (err) throw err;

	err = item.Unsafe().FromJSON(json);
	if (err) throw err;

	string action;
	if (insert) {
		err = db->Insert(args::get(ns), item);
		action = "inserted";
	} else if (update) {
		err = db->Update(args::get(ns), item);
		action = "updated";
	} else {
		err = db->Upsert(args::get(ns), item);
		action = "upserted";
	}
	if (err) throw err;

	err = db->Commit(args::get(ns));
	if (err) throw err;

	std::cout << "Item" << (item.GetID() == -1 ? " not " : " ") << action << std::endl;
}

void DeleteCommand(args::Subparser& subparser) {
	args::Group deleteGroup(subparser, "DELETE group");
	args::HelpFlag upsertHelp(deleteGroup, "help", "show help message for 'DELETE' command", {'h', "help"});
	args::Positional<string> ns(deleteGroup, "<namespace>", "namespace where the element will be inserted", args::Options::Required);
	args::Positional<string> itemJson(deleteGroup, "<JSON>", "item which will be upserted in JSON-syntax", args::Options::Required);

	subparser.Parse();

	auto json = args::get(itemJson);

	auto err = db->EnableStorage(args::get(storagePath));
	if (err) throw err;

	err = db->OpenNamespace(args::get(ns), StorageOpts().Enabled());
	if (err) throw err;

	auto item = db->NewItem(args::get(ns));
	err = item.Status();
	if (err) throw err;

	err = item.Unsafe().FromJSON(json);
	if (err) throw err;

	err = db->Delete(args::get(ns), item);
	if (err) throw err;

	err = db->Commit(args::get(ns));
	if (err) throw err;

	std::cout << "Item" << (item.GetID() == -1 ? " not " : " ") << "deleted" << std::endl;
}

int main(int argc, char* argv[]) {
	backtrace_init();

	args::ArgumentParser parser("Reindexer dump tool");
	args::HelpFlag help(parser, "help", "show this message", {'h', "help"});

	args::Group commands(parser, "commands");

	args::Command list(commands, "list", "list namespaces in storage", &ListCommand);
	args::Command dump(commands, "dump", "Dump whole namespace(s)", &DumpCommand);
	args::Command query(commands, "query", "Execute SQL-like query", &QueryCommand);
	args::Command meta(commands, "meta", "Get meta info by key from storage", &MetaCommand);
	args::Command restore(commands, "restore", "Write to 'reindexer'", &RestoreCommand);
	args::Command upsert(commands, "upsert", "Upsert item(s) to 'reindexer'", &UpsertCommand);
	args::Command del(commands, "delete", "Delete item(s) or whole namespace", &DeleteCommand);

	args::GlobalOptions globals(parser, progOptions);

	try {
		parser.ParseCLI(argc, argv);
	} catch (const args::Help&) {
		std::cout << parser;
	} catch (const args::Error& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
		std::cout << parser.Help() << std::endl;
		return 1;
	} catch (reindexer::Error& re) {
		std::cerr << "ERROR: " << re.what() << std::endl;
		return 1;
	}

	return 0;
}
