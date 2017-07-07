#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include "core/namespace.h"
#include "tools/errors.h"
#include "tools/shared_mutex.h"

using std::shared_ptr;
using std::string;
using std::map;
using std::unordered_map;

namespace reindexer {

class Reindexer {
public:
	Reindexer();
	~Reindexer();

	Error AddNamespace(const string &_namespace);
	Error DeleteNamespace(const string &_namespace);
	Error CloneNamespace(const string &src, const string &dst);
	Error RenameNamespace(const string &src, const string &dst);

	Error AddIndex(const string &_namespace, const string &index, const string &jsonPath, IndexType type, const IndexOpts *opts = nullptr);
	Error EnableStorage(const string &_namespace, const string &path, StorageOpts opts = StorageOpts());

	Error Upsert(const string &_namespace, Item *item);
	Error Delete(const string &_namespace, Item *item);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(const string &query, QueryResults &result);
	Error Select(const Query &query, QueryResults &result);
	Error Commit(const string &namespace_);

	// Get item with payload and userdata by id
	Item *GetItem(const string &_namespace, IdType id);
	// Alloc new item for namespace
	Item *NewItem(const string &_namespace);

	// Get meta data from storage by key
	Error GetMeta(const string &_namespace, const string &key, string &data);
	// Put meta data to storage by key
	Error PutMeta(const string &_namespace, const string &key, const Slice &data);

	Error ResetStats();
	Error GetStats(reindexer_stat &stat);

protected:
	shared_ptr<Namespace> getNamespace(const string &_namespace);
	unordered_map<string, shared_ptr<Namespace> > namespaces;
	shared_timed_mutex ns_mutex;
};

}  // namespace reindexer
