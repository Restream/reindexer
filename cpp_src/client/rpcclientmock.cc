#include "rpcclientmock.h"
#include <functional>
#include "client/itemimpl.h"
#include "core/namespacedef.h"
#include "gason/gason.h"
#include "tools/errors.h"
#include "tools/logger.h"
#include "vendor/gason/gason.h"

namespace reindexer {

namespace client {

using reindexer::net::cproto::RPCAnswer;

RPCClientMock::RPCClientMock(const ReindexerConfig& config) : RPCClient(config) {}

Error RPCClientMock::Insert(string_view nsName, Item& item, const InternalRdxContext& ctx, int outputFormat) {
	return modifyItem(nsName, item, ModeInsert, config_.RequestTimeout, ctx, outputFormat);
}

Error RPCClientMock::Update(string_view nsName, Item& item, const InternalRdxContext& ctx, int outputFormat) {
	return modifyItem(nsName, item, ModeUpdate, config_.RequestTimeout, ctx, outputFormat);
}

Error RPCClientMock::Upsert(string_view nsName, Item& item, const InternalRdxContext& ctx, int outputFormat) {
	return modifyItem(nsName, item, ModeUpsert, config_.RequestTimeout, ctx, outputFormat);
}

Error RPCClientMock::Delete(string_view nsName, Item& item, const InternalRdxContext& ctx, int outputFormat) {
	return modifyItem(nsName, item, ModeDelete, config_.RequestTimeout, ctx, outputFormat);
}

Error RPCClientMock::Delete(const Query& query, QueryResults& result, const InternalRdxContext& ctx, int outputFormat) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	NSArray nsArray;
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });

	result = QueryResults(conn, std::move(nsArray), nullptr, 0, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result, outputFormat](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				if (outputFormat == FormatMsgPack) {
					result.queryParams_.flags |= kResultsMsgPack;
				}
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	int flags = kResultsWithItemID;
	if (outputFormat == FormatMsgPack) flags |= kResultsMsgPack;
	auto ret = conn->Call({cproto::kCmdDeleteQuery, config_.RequestTimeout, ctx.execTimeout()}, ser.Slice(), flags);
	icompl(ret, conn);
	return ret.Status();
}

Error RPCClientMock::Update(const Query& query, QueryResults& result, const InternalRdxContext& ctx, int outputFormat) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	NSArray nsArray;
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });

	result = QueryResults(conn, std::move(nsArray), nullptr, 0, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result, outputFormat](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				if (outputFormat == FormatMsgPack) {
					result.queryParams_.flags |= kResultsMsgPack;
				}
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	int flags = kResultsWithItemID;
	if (outputFormat == FormatMsgPack) {
		flags |= kResultsMsgPack;
	} else {
		flags |= kResultsCJson;
		flags |= kResultsWithPayloadTypes;
	}
	auto ret = conn->Call({cproto::kCmdUpdateQuery, config_.RequestTimeout, ctx.execTimeout()}, ser.Slice(), flags);
	icompl(ret, conn);
	return ret.Status();
}

Error RPCClientMock::modifyItem(string_view nsName, Item& item, int mode, seconds netTimeout, const InternalRdxContext& ctx, int format) {
	if (ctx.cmpl()) {
		return modifyItemAsync(nsName, &item, mode, nullptr, netTimeout, ctx, format);
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
		string_view data;
		switch (format) {
			case FormatJson:
				data = item.GetJSON();
				break;
			case FormatCJson:
				data = item.GetCJSON();
				break;
			case FormatMsgPack:
				data = item.GetMsgPack();
				break;
			default:
				return Error(errParams, "ModifyItem: Unknow data format [%d]", format);
		}
		auto ret = conn->Call({cproto::kCmdModifyItem, netTimeout, ctx.execTimeout()}, nsName, format, data, mode, ser.Slice(),
							  item.GetStateToken(), 0);
		if (!ret.Status().ok()) {
			if (ret.Status().code() != errStateInvalidated || tryCount > 2) return ret.Status();
			if (withNetTimeout) {
				netTimeout = netDeadline - conn->Now();
			}
			QueryResults qr;
			InternalRdxContext ctxCompl = ctx.WithCompletion(nullptr);
			auto ret = selectImpl(Query(string(nsName)).Limit(0), qr, nullptr, netTimeout, ctxCompl, format);
			if (ret.code() == errTimeout) {
				return Error(errTimeout, "Request timeout");
			}
			if (withNetTimeout) {
				netTimeout = netDeadline - conn->Now();
			}
			auto newItem = NewItem(nsName);
			char* endp = nullptr;
			Error err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
			if (!err.ok()) return err;

			item = std::move(newItem);
			continue;
		}
		try {
			auto args = ret.GetArgs(2);
			NSArray nsArray{getNamespace(nsName)};
			return QueryResults(conn, std::move(nsArray), nullptr, p_string(args[0]), int(args[1]), 0, config_.FetchAmount,
								config_.RequestTimeout)
				.Status();
		} catch (const Error& err) {
			return err;
		}
	}
}

Error RPCClientMock::modifyItemAsync(string_view nsName, Item* item, int mode, cproto::ClientConnection* conn, seconds netTimeout,
									 const InternalRdxContext& ctx, int format) {
	WrSerializer ser;
	if (item->impl_->GetPrecepts().size()) {
		ser.PutVarUint(item->impl_->GetPrecepts().size());
		for (auto& p : item->impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}
	if (!conn) conn = getConn();

	string_view data;
	switch (format) {
		case FormatJson:
			data = item->GetJSON();
			break;
		case FormatCJson:
			data = item->GetCJSON();
			break;
		case FormatMsgPack:
			data = item->GetMsgPack();
			break;
		default:
			return Error(errParams, "ModifyItem: Unknow data format [%d]", format);
	}

	string ns(nsName);
	auto deadline = netTimeout.count() ? conn->Now() + netTimeout : seconds(0);
	conn->Call(
		[this, ns, mode, item, deadline, ctx, format](const net::cproto::RPCAnswer& ret, cproto::ClientConnection* conn) -> void {
			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated) return ctx.cmpl()(ret.Status());
				seconds netTimeout(0);
				if (deadline.count()) {
					netTimeout = deadline - conn->Now();
				}
				// State invalidated - make select to update state
				QueryResults* qr = new QueryResults;
				InternalRdxContext ctxCmpl = ctx.WithCompletion([=](const Error& ret) {
					delete qr;
					if (!ret.ok()) return ctx.cmpl()(ret);

					seconds timeout(0);
					if (deadline.count()) {
						timeout = deadline - conn->Now();
					}

					// Rebuild item with new state
					auto newItem = NewItem(ns);
					Error err = newItem.FromJSON(item->impl_->GetJSON());
					newItem.SetPrecepts(item->impl_->GetPrecepts());
					*item = std::move(newItem);
					InternalRdxContext localCtx = ctx;
					modifyItemAsync(ns, item, mode, conn, timeout, localCtx, format);
				});
				selectImpl(Query(ns).Limit(0), *qr, conn, netTimeout, ctxCmpl, format);
			} else
				try {
					auto args = ret.GetArgs(2);
					ctx.cmpl()(QueryResults(conn, {getNamespace(ns)}, nullptr, p_string(args[0]), int(args[1]), 0, config_.FetchAmount,
											config_.RequestTimeout)
								   .Status());
				} catch (const Error& err) {
					ctx.cmpl()(err);
				}
		},
		{cproto::kCmdModifyItem, netTimeout, ctx.execTimeout()}, ns, format, data, mode, ser.Slice(), item->GetStateToken(), 0);
	return errOK;
}

Error RPCClientMock::selectImpl(string_view query, QueryResults& result, cproto::ClientConnection* conn, seconds netTimeout,
								const InternalRdxContext& ctx, int outputFormat) {
	int flags = 0;
	if (outputFormat == FormatMsgPack) {
		flags = kResultsMsgPack;
	} else {
		flags = result.fetchFlags_ ? (result.fetchFlags_ & ~kResultsFormatMask) | kResultsJson : kResultsJson;
	}

	WrSerializer pser;
	h_vector<int32_t, 4> vers;
	vec2pack(vers, pser);

	if (!conn) conn = getConn();

	result = QueryResults(conn, {}, ctx.cmpl(), result.fetchFlags_, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result, outputFormat](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				if (outputFormat == FormatMsgPack) {
					result.queryParams_.flags |= kResultsMsgPack;
				}
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}

			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!ctx.cmpl()) {
		auto ret =
			conn->Call({cproto::kCmdSelectSQL, netTimeout, ctx.execTimeout(), ctx.getCancelCtx()}, query, flags, INT_MAX, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, {cproto::kCmdSelectSQL, netTimeout, ctx.execTimeout(), ctx.getCancelCtx()}, query, flags, INT_MAX, pser.Slice());
		return errOK;
	}
}

Error RPCClientMock::selectImpl(const Query& query, QueryResults& result, cproto::ClientConnection* conn, seconds netTimeout,
								const InternalRdxContext& ctx, int outputFormat) {
	int flags = 0;
	if (outputFormat == FormatMsgPack) {
		flags = kResultsMsgPack;
	} else {
		flags = result.fetchFlags_ ? result.fetchFlags_ : (kResultsWithPayloadTypes | kResultsCJson);
	}

	bool hasJoins = !query.joinQueries_.empty();
	if (!hasJoins) {
		for (auto& mq : query.mergeQueries_) {
			if (!mq.joinQueries_.empty()) {
				hasJoins = true;
				break;
			}
		}
	}

	if (hasJoins) {
		flags &= ~kResultsFormatMask;
		flags |= kResultsJson;
	}

	WrSerializer qser, pser;

	NSArray nsArray;
	query.Serialize(qser);
	query.WalkNested(true, true, [this, &nsArray](const Query& q) { nsArray.push_back(getNamespace(q._namespace)); });
	h_vector<int32_t, 4> vers;
	for (auto& ns : nsArray) {
		shared_lock<shared_timed_mutex> lck(ns->lck_);
		vers.push_back(ns->tagsMatcher_.version() ^ ns->tagsMatcher_.stateToken());
	}
	vec2pack(vers, pser);

	if (!conn) conn = getConn();

	result = QueryResults(conn, std::move(nsArray), ctx.cmpl(), result.fetchFlags_, config_.FetchAmount, config_.RequestTimeout);

	auto icompl = [&result, outputFormat](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				if (outputFormat == FormatMsgPack) {
					result.queryParams_.flags |= kResultsMsgPack;
				}
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!ctx.cmpl()) {
		auto ret = conn->Call({cproto::kCmdSelect, netTimeout, ctx.execTimeout(), ctx.getCancelCtx()}, qser.Slice(), flags,
							  config_.FetchAmount, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, {cproto::kCmdSelect, netTimeout, ctx.execTimeout(), ctx.getCancelCtx()}, qser.Slice(), flags,
				   config_.FetchAmount, pser.Slice());
		return errOK;
	}
}

}  // namespace client
}  // namespace reindexer
