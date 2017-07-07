#include "core/reindexer.h"
#include <stdio.h>
#include <chrono>
#include <memory>
#include <thread>
#include "tools/errors.h"
#include "tools/logger.h"
using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::high_resolution_clock;

namespace reindexer {

#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ < 8
static __thread reindexer_stat local_stat;
#else
static thread_local reindexer_stat local_stat;
#endif

#if 1
#define STAT_FUNC(name)                                                                     \
	class __stat {                                                                          \
	public:                                                                                 \
		__stat() { tmStart = high_resolution_clock::now(); }                                \
		~__stat() {                                                                         \
			auto tmEnd = high_resolution_clock::now();                                      \
			local_stat.time_##name += duration_cast<microseconds>(tmEnd - tmStart).count(); \
			local_stat.count_##name++;                                                      \
		}                                                                                   \
		std::chrono::time_point<std::chrono::high_resolution_clock> tmStart;                \
	} __stater
#else
#define STAT_FUNC(name)
#endif

Reindexer::Reindexer() {}

Reindexer::~Reindexer() {}

Error Reindexer::AddNamespace(const string& _namespace) {
	lock_guard<shared_timed_mutex> lock(ns_mutex);

	if (namespaces.find(_namespace) != namespaces.end()) {
		return Error(errParams, "Namespace '%s' is already exists", _namespace.c_str());
	}

	namespaces.insert({_namespace, std::unique_ptr<Namespace>(new Namespace(_namespace))});
	return 0;
}

Error Reindexer::DeleteNamespace(const string& _namespace) {
	ns_mutex.lock();
	auto nsIt = namespaces.find(_namespace);

	if (nsIt == namespaces.end()) {
		ns_mutex.unlock();
		return Error(errParams, "Namespace '%s' is not exists", _namespace.c_str());
	}

	// Temporary save namespace. This will call destructor without lock
	shared_ptr<Namespace> ns = nsIt->second;
	namespaces.erase(nsIt);
	ns_mutex.unlock();
	// Here will called destructor
	return 0;
}

// Atomically clone namespace. If dst NS exits, error will throwns and NS will not cloned
// Thread safe
Error Reindexer::CloneNamespace(const string& src, const string& dst) {
	ns_mutex.lock();
	auto srcIt = namespaces.find(src);

	if (srcIt == namespaces.end()) {
		ns_mutex.unlock();
		return Error(errParams, "Namespace '%s' is not exists", src.c_str());
	}

	if (namespaces.find(dst) != namespaces.end()) {
		ns_mutex.unlock();
		return Error(errParams, "Namespace '%s' is already exists", dst.c_str());
	}

	Namespace& srcNamespace = *srcIt->second;
	ns_mutex.unlock();
	// Clone data without lock
	shared_ptr<Namespace> dstNamespace = make_shared<Namespace>(srcNamespace);
	srcNamespace.LockSnapshot();

	// Lock again
	ns_mutex.lock();
	namespaces.insert({dst, move(dstNamespace)});
	ns_mutex.unlock();
	return 0;
}

// Atomically rename namespace. If dst NS exists, it will be removed
// Thread safe
Error Reindexer::RenameNamespace(const string& src, const string& dst) {
	shared_ptr<Namespace> dstNamespace;
	ns_mutex.lock();
	auto srcIt = namespaces.find(src);

	if (srcIt == namespaces.end()) {
		ns_mutex.unlock();
		return Error(errParams, "Namespace '%s' is not exists", src.c_str());
	}

	auto dstIt = namespaces.find(dst);

	if (dstIt != namespaces.end()) {
		// If dst NS already exists - prepare to remove it
		// save pointer to dst NS. It will be fried on this function exit without lock
		dstNamespace = dstIt->second;
		namespaces.erase(dstIt);
		// Update iterator. It can be invalidated
		srcIt = namespaces.find(src);
		assert(srcIt != namespaces.end());
	}

	// Save pointer to src NS
	shared_ptr<Namespace> srcNamespace = srcIt->second;
	// Erase src NS iterator from namespaces map
	namespaces.erase(srcIt);
	// Put src NS with new (dst) name
	namespaces.insert({dst, move(srcNamespace)});
	ns_mutex.unlock();
	return 0;
}

Error Reindexer::EnableStorage(const string& _namespace, const string& path, StorageOpts opts) {
	try {
		getNamespace(_namespace)->EnableStorage(path, opts);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::AddIndex(const string& _namespace, const string& index, const string& jsonPath, IndexType type, const IndexOpts* opts) {
	try {
		getNamespace(_namespace)->AddIndex(index, jsonPath, type, opts);

	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Upsert(const string& _namespace, Item* item) {
	STAT_FUNC(upsert);
	try {
		getNamespace(_namespace)->Upsert(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Item* Reindexer::NewItem(const string& _namespace) {
	Item* item;
	try {
		getNamespace(_namespace)->NewItem(&item);
	} catch (const Error& err) {
		item = new ItemImpl(err);
	}
	return item;
}

Item* Reindexer::GetItem(const string& _namespace, IdType id) {
	STAT_FUNC(get_item);
	Item* item;
	try {
		getNamespace(_namespace)->GetItem(id, &item);
	} catch (const Error& err) {
		item = new ItemImpl(err);
	}
	return item;
}

// Get meta data from storage by key
Error Reindexer::GetMeta(const string& _namespace, const string& key, string& data) {
	try {
		data = getNamespace(_namespace)->GetMeta(key);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
// Put meta data to storage by key
Error Reindexer::PutMeta(const string& _namespace, const string& key, const Slice& data) {
	try {
		getNamespace(_namespace)->PutMeta(key, data);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Delete(const string& _namespace, Item* item) {
	STAT_FUNC(delete);
	try {
		getNamespace(_namespace)->Delete(item);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}
Error Reindexer::Delete(const Query& q, QueryResults& result) {
	STAT_FUNC(delete);
	try {
		auto ns = getNamespace(q._namespace);
		result.reserve(100);
		ns->Delete(q, result);
	} catch (const Error& err) {
		return err;
	}
	return 0;
}

Error Reindexer::Select(const string& query, QueryResults& result) {
	STAT_FUNC(select);
	try {
		Query q;
		q.Parse(query);
		return Select(q, result);

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

Error Reindexer::Select(const Query& q, QueryResults& result) {
	try {
		auto ns = getNamespace(q._namespace);
		result.reserve(q.count ? q.count : 100);

		JoinedSelectors joinedSelectors;

		h_vector<Query, 10> queries;

		// For each joined queries
		for (auto& jq : q.joinQueries_) {
			// Get common results from joined namespaces
			auto jns = getNamespace(jq._namespace);
			IdSet preResult;

			if (jq.entries.size()) {
				QueryResults jr;
				jr.reserve(100);
				jns->Select(jq, nullptr, jr);
				preResult.reserve(jr.size());
				for (auto it : jr) preResult.push_back(it.id);
			}
			// Do join for each item in main result
			size_t pos = joinedSelectors.size();
			Query jItemQ(jq._namespace, 0, 0, 0);
			jItemQ.debugLevel = jq.debugLevel;
			jItemQ.entries.reserve(jq.joinEntries_.size());
			// Construct join conditions
			for (auto& je : jq.joinEntries_) {
				QueryEntry qe;
				qe.op = OpAnd;
				qe.condition = je.condition_;
				qe.index = je.joinIndex_;
				qe.idxNo = jns->getIndexByName(je.joinIndex_);
				jItemQ.entries.push_back(qe);
			}
			queries.push_back(jItemQ);
			Query* pjItemQ = &queries.back();

			auto joinedSelector = [&result, jq, ns, jns, preResult, pos, pjItemQ](IdType id, bool match) {

				local_stat.count_join++;  // Do not measure each join time (expensive). Just give count
				KeyRefs krefs;
				auto payload = ns->GetPayload(id);
				// Put values to join conditions
				int cnt = 0;
				for (auto& je : jq.joinEntries_) {
					payload.Get(je.index_, krefs);
					pjItemQ->entries[cnt].values.resize(0);
					pjItemQ->entries[cnt].values.reserve(krefs.size());
					for (auto kref : krefs) pjItemQ->entries[cnt].values.push_back(KeyValue(kref));
					cnt++;
				}
				pjItemQ->count = match ? 0 : 1;
				QueryResults joinItemR;
				jns->Select(*pjItemQ, jq.entries.size() ? &preResult : nullptr, joinItemR);

				if (match && joinItemR.size()) {
					auto& jres = result.joined_.emplace(id, vector<QueryResults>()).first->second;

					if (pos >= jres.size()) jres.resize(pos + 1);

					jres[pos] = joinItemR;
				}
				return joinItemR.size() != 0;
			};
			joinedSelectors.push_back({jq.joinType, joinedSelector});
		}
		{
			STAT_FUNC(select);
			ns->Select(q, nullptr, result, joinedSelectors);
		}
	} catch (const Error& err) {
		return err;
	}

	return 0;
}

Error Reindexer::Commit(const string& _namespace) {
	try {
		getNamespace(_namespace)->Commit();

	} catch (const Error& err) {
		return err;
	}

	return 0;
}

shared_ptr<Namespace> Reindexer::getNamespace(const string& _namespace) {
	shared_lock<shared_timed_mutex> lock(ns_mutex);
	auto nsIt = namespaces.find(_namespace);

	if (nsIt == namespaces.end()) {
		throw Error(errParams, "Namespace '%s' is not exists", _namespace.c_str());
	}

	assert(nsIt->second);
	return nsIt->second;
}
Error Reindexer::ResetStats() {
	memset(&local_stat, 0, sizeof(local_stat));
	return Error(errOK);
}
Error Reindexer::GetStats(reindexer_stat& stat) {
	stat = local_stat;
	return Error(errOK);
}

}  // namespace reindexer
