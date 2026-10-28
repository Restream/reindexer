#pragma once

#include "client/connectopts.h"
#include "client/internalrdxcontext.h"
#include "client/reindexerconfig.h"
#include "client/rpcclient.h"
#include "net/ev/ev.h"

namespace reindexer {

namespace client {

/// The main Reindexer interface. Holds database object<br>
/// *Thread safety*: None of the methods are threadsafe <br>
/// CoroReindexer should be used via multiple coroutins in single thread, while ev-loop is running.
/// *Resources lifetime*: All resources aquired from Reindexer, e.g Item or QueryResults are uses Copy-On-Write
/// semantics, and have independent lifetime<br>
class [[nodiscard]] RaftClient {
public:
	using NodeData = cluster::NodeData;
	using RaftInfo = cluster::RaftInfo;
	/// Completion routine
	typedef std::function<void(const Error& err)> Completion;

	/// Create Reindexer database object
	RaftClient(const ReindexerConfig& = ReindexerConfig());
	/// Destrory Reindexer database object
	~RaftClient();
	RaftClient(const RaftClient&) = delete;
	RaftClient(RaftClient&&) noexcept;
	RaftClient& operator=(const RaftClient&) = delete;
	RaftClient& operator=(RaftClient&&) noexcept;

	/// Connect - connect to reindexer server
	/// @param dsn - uri of server and database, like: `cproto://user@password:127.0.0.1:6534/dbname`
	/// @param loop - event loop for connections and coroutines handling
	/// @param opts - Connect options. May contaion any of <br>
	Error Connect(const DSN& dsn, net::ev::dynamic_loop& loop, const client::ConnectOpts& opts = client::ConnectOpts()) noexcept;
	/// Stop - shutdown connector
	void Stop() noexcept;
	/// SuggestLeader - send cluster leader suggestion
	/// @param suggestion - node, suggested as a leader
	/// @param response - node, which has to be come leader according to remote server
	Error SuggestLeader(const NodeData& suggestion, NodeData& response) noexcept;
	/// LeadersPing - send ping from cluster leader to follower
	/// @param leader - info about current node (leader)
	Error LeadersPing(const NodeData& leader) noexcept;
	/// GetRaftInfo - get raft status of the remote node
	/// @param info - status of the remote node
	Error GetRaftInfo(RaftInfo& info) noexcept;
	/// Get curret connection status
	/// @param forceCheck - forces to check status immediatlly (otherwise result of periodic check will be returned)
	Error Status(bool forceCheck = false) noexcept;

	Error SetDesiredLeaderId(int leaderId) noexcept;
	/// Add cancelable context
	/// @param cancelCtx - context pointer
	RaftClient WithContext(const IRdxCancelContext* cancelCtx) const noexcept {
		return RaftClient(impl_, ctx_.WithCancelContext(cancelCtx));
	}

	/// Add execution timeout to the next query
	/// @param timeout - Optional server-side execution timeout for each subquery
	RaftClient WithTimeout(milliseconds timeout) const noexcept { return RaftClient(impl_, ctx_.WithTimeout(timeout)); }

private:
	RaftClient(RPCClient* impl, InternalRdxContext&& ctx) noexcept : impl_(impl), owner_(false), ctx_(std::move(ctx)) {}
	RPCClient* impl_;
	bool owner_;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
