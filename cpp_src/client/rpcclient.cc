#include "client/rpcclient.h"
#include <stdio.h>
#include <functional>
#include "client/itemimpl.h"
#include "core/namespacedef.h"
#include "core/schema.h"
#include "gason/gason.h"
#include "tools/cpucheck.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "vendor/gason/gason.h"

namespace reindexer {
namespace client {

using reindexer::net::cproto::RPCAnswer;

RPCClient::RPCClient(const ReindexerConfig& config) : workers_(config.WorkerThreads), config_(config), updatesConn_(nullptr) {
	reindexer::CheckRequiredSSESupport();

	if (config_.ConnectTimeout > config_.RequestTimeout) {
		config_.RequestTimeout = config_.ConnectTimeout;
	}
	curConnIdx_ = 0;
}

RPCClient::~RPCClient() { Stop(); }

Error RPCClient::startWorkers() {
	connections_.resize(config_.ConnPoolSize);
	for (size_t i = 0; i < workers_.size(); i++) {
		workers_[i].thread_ = std::thread([this](size_t id) { this->run(id); }, i);
		while (!workers_[i].running) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
	return errOK;
}

Error RPCClient::addConnectEntry(const std::string& dsn, const client::ConnectOpts& opts, size_t idx) {
	assertrx(idx < connectData_.entries.size());
	cproto::ClientConnection::ConnectData::Entry& connectEntry = connectData_.entries[idx];
	if (!connectEntry.uri.parse(dsn)) {
		return Error(errParams, "%s is not valid uri", dsn);
	}
	if (connectEntry.uri.scheme() != "cproto") {
		return Error(errParams, "Scheme must be cproto");
	}
	connectEntry.opts = cproto::ClientConnection::Options(config_.ConnectTimeout, config_.RequestTimeout, opts.IsCreateDBIfMissing(),
														  opts.HasExpectedClusterID(), opts.ExpectedClusterID(), config_.ReconnectAttempts,
														  config_.EnableCompression, config_.AppName);
	return errOK;
}

Error RPCClient::Connect(const std::string& dsn, const client::ConnectOpts& opts) {
	if (connections_.size()) {
		return Error(errLogic, "Client is already started");
	}
	std::vector<cproto::ClientConnection::ConnectData::Entry> tmpConnectData(1);
	connectData_.entries.swap(tmpConnectData);
	Error err = addConnectEntry(dsn, opts, 0);
	if (err.ok()) {
		return startWorkers();
	}
	return err;
}

Error RPCClient::Connect(const std::vector<std::pair<std::string, client::ConnectOpts>>& connectData) {
	if (connections_.size()) {
		return Error(errLogic, "Client is already started");
	}
	if (connectData.empty()) {
		return Error(errLogic, "Connections data is empty!");
	}
	std::vector<cproto::ClientConnection::ConnectData::Entry> tmpConnectData(connectData.size());
	connectData_.entries.swap(tmpConnectData);
	for (size_t i = 0; i < connectData.size(); ++i) {
		Error err = addConnectEntry(connectData[i].first, connectData[i].second, i);
		if (!err.ok()) {
			return err;
		}
	}
	return startWorkers();
}

void RPCClient::Stop() {
	if (!connections_.size()) {
		return;
	}
	for (auto& worker : workers_) {
		worker.stop_.send();
		if (worker.thread_.joinable()) {
			worker.thread_.join();
		}
	}
	connections_.clear();
}

void RPCClient::run(size_t thIdx) {
	bool terminate = false;

	workers_[thIdx].stop_.set(workers_[thIdx].loop_);
	workers_[thIdx].stop_.set([&](ev::async& sig) {
		terminate = true;
		sig.loop.break_loop();
	});

	workers_[thIdx].stop_.start();
	delayedUpdates_.clear();
	serialDelays_ = 0;

	for (size_t i = thIdx; int(i) < config_.ConnPoolSize; i += config_.WorkerThreads) {
		connections_[i].reset(new cproto::ClientConnection(workers_[thIdx].loop_, &connectData_,
														   std::bind(&RPCClient::onConnectionFail, this, std::placeholders::_1)));
	}

	ev::periodic checker;
	if (thIdx == 0) {
		checker.set(workers_[thIdx].loop_);
		checker.set([this](ev::periodic&, int) { checkSubscribes(); });
		checker.start(5, 5);
	}

	workers_[thIdx].running.store(true);
	for (;;) {
		workers_[thIdx].loop_.run();
		bool doTerminate = terminate;
		if (doTerminate) {
			for (size_t i = thIdx; int(i) < config_.ConnPoolSize; i += config_.WorkerThreads) {
				connections_[i]->SetTerminateFlag();
				if (connections_[i]->PendingCompletions()) {
					doTerminate = false;
				}
			}
		}
		if (doTerminate) {
			break;
		}
	}
	for (size_t i = thIdx; int(i) < config_.ConnPoolSize; i += config_.WorkerThreads) {
		connections_[i].reset();
	}
	workers_[thIdx].running.store(false);
}

Error RPCClient::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	nsDef.GetJSON(ser);
	auto status = getConn()->Call(mkCommand(cproto::kCmdOpenNamespace, &ctx), ser.Slice()).Status();

	if (!status.ok()) {
		return status;
	}

	std::unique_lock<shared_timed_mutex> lock(nsMutex_);
	namespaces_.emplace(nsDef.name, Namespace::Ptr(new Namespace(nsDef.name)));

	return errOK;
}

Error RPCClient::OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx, const StorageOpts& sopts) {
	NamespaceDef nsDef(std::string(nsName), sopts);
	return AddNamespace(nsDef, ctx);
}

Error RPCClient::CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdCloseNamespace, &ctx), nsName).Status();
}

Error RPCClient::DropNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdDropNamespace, &ctx), nsName).Status();
}

Error RPCClient::TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdTruncateNamespace, &ctx), nsName).Status();
}

Error RPCClient::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx) {
	auto status = getConn()->Call(mkCommand(cproto::kCmdRenameNamespace, &ctx), srcNsName, dstNsName).Status();

	if (!status.ok()) {
		return status;
	}

	if (srcNsName != dstNsName) {
		std::unique_lock<shared_timed_mutex> lock(nsMutex_);

		auto namespacePtr = namespaces_.find(srcNsName);
		auto namespacePtrDst = namespaces_.find(dstNsName);
		if (namespacePtr != namespaces_.end()) {
			if (namespacePtrDst == namespaces_.end()) {
				namespaces_.emplace(dstNsName, namespacePtr->second);
			} else {
				namespacePtrDst->second = namespacePtr->second;
			}
			namespaces_.erase(namespacePtr);
		} else {
			namespaces_.erase(namespacePtrDst);
		}
	}
	return errOK;
}

Error RPCClient::Insert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, ModeInsert, config_.RequestTimeout, ctx);
}

Error RPCClient::Update(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, ModeUpdate, config_.RequestTimeout, ctx);
}

Error RPCClient::Upsert(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, ModeUpsert, config_.RequestTimeout, ctx);
}

Error RPCClient::Delete(std::string_view nsName, Item& item, const InternalRdxContext& ctx) {
	return modifyItem(nsName, item, ModeDelete, config_.RequestTimeout, ctx);
}

Error RPCClient::modifyItem(std::string_view nsName, Item& item, int mode, seconds netTimeout, const InternalRdxContext& ctx) {
	if (ctx.cmpl()) {
		return modifyItemAsync(nsName, &item, mode, nullptr, netTimeout, ctx);
	}

	WrSerializer ser;
	if (item.impl_->GetPrecepts().size()) {
		ser.PutVarUint(item.impl_->GetPrecepts().size());
		for (auto& p : item.impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}

	bool withNetTimeout = (netTimeout.count() > 0);
	for (int tryCount = 0;; tryCount++) {
		auto conn = getConn();
		auto netDeadline = conn->Now() + netTimeout;
		auto ret = conn->Call(mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), nsName, int(FormatCJson), item.GetCJSON(), mode,
							  ser.Slice(), item.GetStateToken(), 0);
		if (!ret.Status().ok()) {
			if (ret.Status().code() != errStateInvalidated || tryCount > 2) {
				return ret.Status();
			}
			if (withNetTimeout) {
				netTimeout = netDeadline - conn->Now();
			}
			QueryResults qr;
			InternalRdxContext ctxCompl = ctx.WithCompletion(nullptr);
			auto ret = selectImpl(Query(std::string(nsName)).Limit(0), qr, nullptr, netTimeout, ctxCompl);
			if (ret.code() == errTimeout) {
				return Error(errTimeout, "Request timeout");
			}
			if (withNetTimeout) {
				netTimeout = netDeadline - conn->Now();
			}
			auto newItem = NewItem(nsName);
			char* endp = nullptr;
			Error err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
			if (!err.ok()) {
				return err;
			}

			item = std::move(newItem);
			continue;
		}
		try {
			auto args = ret.GetArgs(2);
			NsArray nsArray{getNamespace(nsName)};
			return QueryResults(conn, std::move(nsArray), nullptr, p_string(args[0]), int(args[1]), 0, config_.FetchAmount,
								config_.RequestTimeout)
				.Status();
		} catch (const Error& err) {
			return err;
		}
	}
}

Error RPCClient::modifyItemAsync(std::string_view nsName, Item* item, int mode, cproto::ClientConnection* conn, seconds netTimeout,
								 const InternalRdxContext& ctx) {
	WrSerializer ser;
	if (item->impl_->GetPrecepts().size()) {
		ser.PutVarUint(item->impl_->GetPrecepts().size());
		for (auto& p : item->impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}
	if (!conn) {
		conn = getConn();
	}

	std::string ns(nsName);
	auto deadline = netTimeout.count() ? conn->Now() + netTimeout : seconds(0);
	conn->Call(
		[this, ns, mode, item, deadline, ctx](const net::cproto::RPCAnswer& ret, cproto::ClientConnection* conn) -> void {
			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated) {
					return ctx.cmpl()(ret.Status());
				}
				seconds netTimeout(0);
				if (deadline.count()) {
					netTimeout = deadline - conn->Now();
				}
				// State invalidated - make select to update state
				QueryResults* qr = new QueryResults;
				InternalRdxContext ctxCmpl = ctx.WithCompletion([=](const Error& ret) {
					delete qr;
					if (!ret.ok()) {
						return ctx.cmpl()(ret);
					}

					seconds timeout(0);
					if (deadline.count()) {
						timeout = deadline - conn->Now();
					}

					// Rebuild item with new state
					auto newItem = NewItem(ns);
					Error err = newItem.FromJSON(item->impl_->GetJSON());
					if (err.ok()) {
						return ctx.cmpl()(err);
					}
					newItem.SetPrecepts(item->impl_->GetPrecepts());
					*item = std::move(newItem);
					err = modifyItemAsync(ns, item, mode, conn, timeout, ctx);
					if (err.ok()) {
						return ctx.cmpl()(err);
					}
				});
				auto err = selectImpl(Query(ns).Limit(0), *qr, conn, netTimeout, ctxCmpl);
				if (err.ok()) {
					return ctx.cmpl()(err);
				}
			} else {
				try {
					auto args = ret.GetArgs(2);
					ctx.cmpl()(QueryResults(conn, {getNamespace(ns)}, nullptr, p_string(args[0]), int(args[1]), 0, config_.FetchAmount,
											config_.RequestTimeout)
								   .Status());
				} catch (const Error& err) {
					ctx.cmpl()(err);
				}
			}
		},
		mkCommand(cproto::kCmdModifyItem, netTimeout, &ctx), ns, int(FormatCJson), item->GetCJSON(), mode, ser.Slice(),
		item->GetStateToken(), 0);
	return errOK;
}

Error RPCClient::subscribeImpl(bool subscribe) {
	Error err;
	auto updatesConn = updatesConn_.load();
	if (subscribe) {
		UpdatesFilters filter = observers_.GetMergedFilter();
		WrSerializer ser;
		filter.GetJSON(ser);
		if (updatesConn) {
			err = updatesConn->Call(mkCommand(cproto::kCmdSubscribeUpdates), 1, ser.Slice()).Status();
		} else {
			auto conn = getConn();
			err = conn->Call(mkCommand(cproto::kCmdSubscribeUpdates), 1, ser.Slice()).Status();
			if (err.ok()) {
				updatesConn_ = conn;
			}
			conn->SetUpdatesHandler([this](RPCAnswer&& ans, cproto::ClientConnection* conn) { onUpdates(ans, conn); });
		}
	} else if (updatesConn) {
		err = updatesConn->Call(mkCommand(cproto::kCmdSubscribeUpdates), 0).Status();
		updatesConn_ = nullptr;
	}
	return err;
}

Item RPCClient::NewItem(std::string_view nsName) {
	try {
		auto ns = getNamespace(nsName);
		return ns->NewItem();
	} catch (const Error& err) {
		return Item(err);
	}
}

Error RPCClient::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx) {
	try {
		auto ret = getConn()->Call(mkCommand(cproto::kCmdGetMeta, &ctx), nsName, key);
		if (ret.Status().ok()) {
			data = ret.GetArgs(1)[0].As<std::string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdPutMeta, &ctx), nsName, key, data).Status();
}

Error RPCClient::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx) {
	try {
		auto ret = getConn()->Call(mkCommand(cproto::kCmdEnumMeta, &ctx), nsName);
		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			keys.clear();
			keys.reserve(args.size());
			for (auto& k : args) {
				keys.push_back(k.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdDeleteMeta, &ctx), nsName, key).Status();
}

Error RPCClient::Delete(const Query& query, QueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	NsArray nsArray;
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q.NsName())); });

	result = QueryResults(conn, std::move(nsArray), nullptr, 0, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	auto ret = conn->Call(mkCommand(cproto::kCmdDeleteQuery, &ctx), ser.Slice(), kResultsWithItemID);
	icompl(ret, conn);
	return ret.Status();
}

Error RPCClient::Update(const Query& query, QueryResults& result, const InternalRdxContext& ctx) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	NsArray nsArray;
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q.NsName())); });

	result = QueryResults(conn, std::move(nsArray), nullptr, 0, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	auto ret =
		conn->Call(mkCommand(cproto::kCmdUpdateQuery, &ctx), ser.Slice(), kResultsWithItemID | kResultsWithPayloadTypes | kResultsCJson);
	icompl(ret, conn);
	return ret.Status();
}

Error RPCClient::selectImpl(std::string_view query, QueryResults& result, cproto::ClientConnection* conn, seconds netTimeout,
							const InternalRdxContext& ctx) {
	int flags = result.fetchFlags_ ? (result.fetchFlags_ & ~kResultsFormatMask) | kResultsJson : kResultsJson;

	WrSerializer pser;
	h_vector<int32_t, 4> vers;
	vec2pack(vers, pser);

	if (!conn) {
		conn = getConn();
	}

	result = QueryResults(conn, {}, ctx.cmpl(), result.fetchFlags_, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}

			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!ctx.cmpl()) {
		auto ret = conn->Call(mkCommand(cproto::kCmdSelectSQL, netTimeout, &ctx), query, flags, config_.FetchAmount, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, mkCommand(cproto::kCmdSelectSQL, netTimeout, &ctx), query, flags, config_.FetchAmount, pser.Slice());
		return errOK;
	}
}

Error RPCClient::selectImpl(const Query& query, QueryResults& result, cproto::ClientConnection* conn, seconds netTimeout,
							const InternalRdxContext& ctx) {
	WrSerializer qser, pser;
	int flags = result.fetchFlags_ ? result.fetchFlags_ : (kResultsWithPayloadTypes | kResultsCJson);
	bool hasJoins = !query.GetJoinQueries().empty();
	if (!hasJoins) {
		for (auto& mq : query.GetMergeQueries()) {
			if (!mq.GetJoinQueries().empty()) {
				hasJoins = true;
				break;
			}
		}
	}
	if (hasJoins) {
		flags &= ~kResultsFormatMask;
		flags |= kResultsJson;
	}
	NsArray nsArray;
	query.Serialize(qser);
	query.WalkNested(true, true, false, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q.NsName())); });
	h_vector<int32_t, 4> vers;
	for (auto& ns : nsArray) {
		shared_lock<shared_timed_mutex> lck(ns->lck_);
		vers.push_back(ns->tagsMatcher_.version() ^ ns->tagsMatcher_.stateToken());
	}
	vec2pack(vers, pser);

	if (!conn) {
		conn = getConn();
	}

	result = QueryResults(conn, std::move(nsArray), ctx.cmpl(), result.fetchFlags_, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!ctx.cmpl()) {
		auto ret = conn->Call(mkCommand(cproto::kCmdSelect, netTimeout, &ctx), qser.Slice(), flags, config_.FetchAmount, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, mkCommand(cproto::kCmdSelect, netTimeout, &ctx), qser.Slice(), flags, config_.FetchAmount, pser.Slice());
		return errOK;
	}
}

Error RPCClient::Commit(std::string_view nsName) { return getConn()->Call(mkCommand(cproto::kCmdCommit), nsName).Status(); }

Error RPCClient::AddIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return getConn()->Call(mkCommand(cproto::kCmdAddIndex, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::UpdateIndex(std::string_view nsName, const IndexDef& iDef, const InternalRdxContext& ctx) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return getConn()->Call(mkCommand(cproto::kCmdUpdateIndex, &ctx), nsName, ser.Slice()).Status();
}

Error RPCClient::DropIndex(std::string_view nsName, const IndexDef& idx, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdDropIndex, &ctx), nsName, idx.name_).Status();
}

Error RPCClient::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx) {
	return getConn()->Call(mkCommand(cproto::kCmdSetSchema, &ctx), nsName, schema).Status();
}

Error RPCClient::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
	try {
		auto ret = getConn()->Call(mkCommand(cproto::kCmdEnumNamespaces, &ctx), int(opts.options_), p_string(&opts.filter_));
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));

			for (auto& nselem : root["items"]) {
				NamespaceDef def;
				def.FromJSON(nselem);
				defs.emplace_back(std::move(def));
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& err) {
		return Error(errParseJson, "EnumNamespaces: %s", err.what());
	}
}

Error RPCClient::EnumDatabases(std::vector<std::string>& dbList, const InternalRdxContext& ctx) {
	try {
		auto ret = getConn()->Call(mkCommand(cproto::kCmdEnumDatabases, &ctx), 0);
		if (ret.Status().ok()) {
			gason::JsonParser parser;
			auto json = ret.GetArgs(1)[0].As<std::string>();
			auto root = parser.Parse(giftStr(json));
			for (auto& elem : root["databases"]) {
				dbList.emplace_back(elem.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	} catch (const gason::Exception& err) {
		return Error(errParseJson, "EnumDatabases: %s", err.what());
	}
}

Error RPCClient::SubscribeUpdates(IUpdatesObserver* observer, const UpdatesFilters& filters, SubscriptionOpts opts) {
	observers_.Add(observer, filters, opts);
	return subscribeImpl(true);
}

Error RPCClient::UnsubscribeUpdates(IUpdatesObserver* observer) {
	if (auto err = observers_.Delete(observer); !err.ok()) {
		return err;
	}
	return subscribeImpl(!observers_.Empty());
}

Error RPCClient::GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string>& suggests) {
	try {
		auto ret = getConn()->Call(mkCommand(cproto::kCmdGetSQLSuggestions), query, pos);
		if (ret.Status().ok()) {
			auto rargs = ret.GetArgs();
			suggests.clear();
			suggests.reserve(rargs.size());

			for (auto& rarg : rargs) {
				suggests.push_back(rarg.As<std::string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::Status() { return getConn()->CheckConnection(); }

void RPCClient::checkSubscribes() {
	bool subscribe = !observers_.Empty();

	auto updatesConn = updatesConn_.load();
	if (subscribe && !updatesConn_) {
		getConn()->Call(
			[this](const RPCAnswer& ans, cproto::ClientConnection* conn) {
				if (ans.Status().ok()) {
					updatesConn_ = conn;
					observers_.OnConnectionState(errOK);
					conn->SetUpdatesHandler([this](RPCAnswer&& ans, cproto::ClientConnection* conn) { onUpdates(ans, conn); });
				}
			},
			mkCommand(cproto::kCmdSubscribeUpdates), 1);
	} else if (!subscribe && updatesConn) {
		updatesConn->Call([](const RPCAnswer&, cproto::ClientConnection*) {}, mkCommand(cproto::kCmdSubscribeUpdates), 0);
		updatesConn_ = nullptr;
	}
}

Namespace* RPCClient::getNamespace(std::string_view nsName) {
	nsMutex_.lock_shared();
	auto nsIt = namespaces_.find(nsName);
	if (nsIt != namespaces_.end()) {
		nsMutex_.unlock_shared();
		return nsIt->second.get();
	}
	nsMutex_.unlock_shared();

	nsMutex_.lock();
	nsIt = namespaces_.find(nsName);
	if (nsIt == namespaces_.end()) {
		std::string nsNames(nsName);
		nsIt = namespaces_.emplace(nsNames, Namespace::Ptr(new Namespace(nsNames))).first;
	}
	nsMutex_.unlock();
	return nsIt->second.get();
}

net::cproto::ClientConnection* RPCClient::getConn() {
	assertrx(connections_.size());
	auto conn = connections_.at(curConnIdx_++ % connections_.size()).get();
	assertrx(conn);
	return conn;
}

cproto::CommandParams RPCClient::mkCommand(cproto::CmdCode cmd, const InternalRdxContext* ctx) const noexcept {
	return mkCommand(cmd, config_.RequestTimeout, ctx);
}

cproto::CommandParams RPCClient::mkCommand(cproto::CmdCode cmd, std::chrono::seconds reqTimeout, const InternalRdxContext* ctx) noexcept {
	if (ctx) {
		return {cmd, reqTimeout, ctx->execTimeout(), ctx->getCancelCtx()};
	}
	return {cmd, reqTimeout, std::chrono::milliseconds(0), nullptr};
}

void RPCClient::onUpdates(net::cproto::RPCAnswer& ans, cproto::ClientConnection* conn) {
	if (!ans.Status().ok()) {
		updatesConn_ = nullptr;
		observers_.OnConnectionState(ans.Status());
		return;
	}

	if (!delayedUpdates_.empty()) {
		ans.EnsureHold();
		delayedUpdates_.emplace_back(std::move(ans));
		return;
	}
	cproto::Args args;
	try {
		args = ans.GetArgs();
	} catch (const Error& err) {
		logPrintf(LogError, "Parsing updates error: %s", err.what());
		return;
	}
	if (args.size() == 1) {
		std::string_view nsName(args[0]);
		observers_.OnUpdatesLost(nsName);
		return;
	}
	if (args.size() < 3) {
		logPrintf(LogError, "Parsing updates error: args count %d", args.size());
		return;
	}
	lsn_t lsn{int64_t(args[0])};
	std::string_view nsName(args[1]);
	std::string_view pwalRec(args[2]);
	lsn_t originLSN;
	if (args.size() >= 4) {
		originLSN = lsn_t(args[3].As<int64_t>());
	}
	WALRecord wrec(pwalRec);

	if (wrec.type == WalItemModify) {
		// Special process for Item Modify
		auto ns = getNamespace(nsName);

		// Check if cjson with bundled tagsMatcher
		const bool bundledTagsMatcher =
			wrec.itemModify.itemCJson.length() > 0 && Serializer{wrec.itemModify.itemCJson}.GetCTag() == kCTagEnd;

		ns->lck_.lock_shared();
		auto tmVersion = ns->tagsMatcher_.version();
		ns->lck_.unlock_shared();

		if (tmVersion < wrec.itemModify.tmVersion && !bundledTagsMatcher) {
			// If tagsMatcher has been updated but there is no bundled tagsMatcher in cjson
			// then we need to ask server to send tagsMatcher.

			++serialDelays_;
			// Delay this update and all the further updates until we get responce from server.
			ans.EnsureHold();
			delayedUpdates_.emplace_back(std::move(ans));

			QueryResults* qr = new QueryResults;
			auto err = Select(
				Query(std::string(nsName)).Limit(0), *qr,
				InternalRdxContext(
					nullptr,
					[this, qr, conn](const Error& err) {
						delete qr;
						// If there are delayed updates then send them to client
						auto uq = std::move(delayedUpdates_);
						delayedUpdates_.clear();

						if (!err.ok() || serialDelays_ > 1) {
							// This update was already delayed, but was not able to synchronize tagsmatcher.
							// Such situation usually means, that master's namespace was recreated and must be synchronized via force
							// sync.
							// Current fix is suboptimal and in some cases even incorrect (but still better, than previous
							// implementation) - proper fix requires some versioning info about namespaces, which exists
							// in v4 only
							std::string_view nsName(std::string_view(uq.front().GetArgs(1)[1]));
							logPrintf(
								LogWarning,
								"[repl:%s] Unable to sync tags matcher via online-replication (err: '%s'). Calling UpdatesLost fallback",
								nsName, err.what());
							serialDelays_ = 0;
							observers_.OnUpdatesLost(nsName);
						} else {
							for (auto& a1 : uq) {
								onUpdates(a1, conn);
							}
						}
					}),
				conn);
			if (!err.ok()) {
				logPrintf(LogWarning,
						  "[repl:%s] Unable to sync tags matcher via online-replication (select err: '%s'). Calling UpdatesLost fallback",
						  err.what());
				observers_.OnUpdatesLost(nsName);
			}
			return;
		} else {
			// We have bundled tagsMatcher
			if (bundledTagsMatcher) {
				// printf("%s bundled tm %d to %d\n", ns->name_.c_str(), ns->tagsMatcher_.version(), wrec.itemModify.tmVersion);
				Serializer rdser(wrec.itemModify.itemCJson);
				[[maybe_unused]] const ctag tag = rdser.GetCTag();
				uint32_t tmOffset = rdser.GetUInt32();
				// read tags matcher update
				rdser.SetPos(tmOffset);
				std::unique_lock<shared_timed_mutex> lck(ns->lck_);
				ns->tagsMatcher_ = TagsMatcher();
				ns->tagsMatcher_.deserialize(rdser, wrec.itemModify.tmVersion, ns->tagsMatcher_.stateToken());
			}
		}
	} else if (wrec.type == WalTagsMatcher) {
		TagsMatcher tm;
		Serializer ser(wrec.data.data(), wrec.data.size());
		const auto version = ser.GetVarint();
		const auto stateToken = ser.GetVarint();
		tm.deserialize(ser, version, stateToken);
		auto ns = getNamespace(nsName);
		std::lock_guard lck(ns->lck_);
		ns->tagsMatcher_ = std::move(tm);
	}
	serialDelays_ = 0;
	observers_.OnWALUpdate(LSNPair(lsn, originLSN), nsName, wrec);
}

bool RPCClient::onConnectionFail(int failedDsnIndex) {
	if (!connectData_.ThereAreReconnectOptions()) {
		return false;
	}
	if (!connectData_.CurrDsnFailed(failedDsnIndex)) {
		return false;
	}

	connectData_.lastFailedEntryIdx = failedDsnIndex;
	connectData_.validEntryIdx.store(connectData_.GetNextDsnIndex(), std::memory_order_release);

	for (size_t i = 0; i < connections_.size(); ++i) {
		connections_[i]->Reconnect();
	}
	return true;
}

Transaction RPCClient::NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) {
	net::cproto::ClientConnection* conn = getConn();
	auto res = conn->Call(mkCommand(cproto::kCmdStartTransaction, &ctx), nsName);
	auto err = res.Status();
	if (err.ok()) {
		auto args = res.GetArgs(1);
		return Transaction(this, conn, int64_t(args[0]), config_.RequestTimeout, ctx.execTimeout(),
						   std::string(nsName.data(), nsName.size()));
	}
	return Transaction(std::move(err));
}

Error RPCClient::CommitTransaction(Transaction& tr, const InternalRdxContext& ctx) {
	if (tr.conn_) {
		auto ret = tr.conn_->Call(mkCommand(cproto::kCmdCommitTx, &ctx), tr.txId_).Status();
		tr.clear();
		return ret;
	}
	return Error(errLogic, "connection is nullptr");
}
Error RPCClient::RollBackTransaction(Transaction& tr, const InternalRdxContext& ctx) {
	if (tr.conn_) {
		auto ret = tr.conn_->Call(mkCommand(cproto::kCmdRollbackTx, &ctx), tr.txId_).Status();
		tr.clear();
		return ret;
	}
	return Error(errLogic, "connection is nullptr");
}

}  // namespace client
}  // namespace reindexer
