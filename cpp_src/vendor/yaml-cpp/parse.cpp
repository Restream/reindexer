#include "yaml-cpp/node/parse.h"

#include <fstream>
#include <sstream>

#include "nodebuilder.h"
#include "yaml-cpp/node/impl.h"
#include "yaml-cpp/node/node.h"
#include "yaml-cpp/parser.h"

namespace YAML {
Node Load(const std::string& input, const ScannerOpts& opts) {
	std::stringstream stream(input);
	return Load(stream, opts);
}

Node Load(const char* input, const ScannerOpts& opts) {
	std::stringstream stream(input);
	return Load(stream, opts);
}

Node Load(std::istream& input, const ScannerOpts& opts) {
	Parser parser(input, opts);
	NodeBuilder builder;
	if (!parser.HandleNextDocument(builder)) {
		return Node();
	}

	return builder.Root();
}

Node LoadFile(const std::string& filename, const ScannerOpts& opts) {
	std::ifstream fin(filename);
	if (!fin) {
		throw BadFile(filename);
	}
	return Load(fin, opts);
}

std::vector<Node> LoadAll(const std::string& input, const ScannerOpts& opts) {
	std::stringstream stream(input);
	return LoadAll(stream, opts);
}

std::vector<Node> LoadAll(const char* input, const ScannerOpts& opts) {
	std::stringstream stream(input);
	return LoadAll(stream, opts);
}

std::vector<Node> LoadAll(std::istream& input, const ScannerOpts& opts) {
	std::vector<Node> docs;

	Parser parser(input, opts);
	while (true) {
		NodeBuilder builder;
		if (!parser.HandleNextDocument(builder)) {
			break;
		}
		docs.push_back(builder.Root());
	}

	return docs;
}

std::vector<Node> LoadAllFromFile(const std::string& filename, const ScannerOpts& opts) {
	std::ifstream fin(filename);
	if (!fin) {
		throw BadFile(filename);
	}
	return LoadAll(fin, opts);
}
}  // namespace YAML
