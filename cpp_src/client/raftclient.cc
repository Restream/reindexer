#include "client/raftclient.h"
#include "client/rpcclient.h"
#include "cluster/clustercontrolrequest.h"
#include "tools/catch_and_return.h"

namespace reindexer {
namespace client {

RaftClient::RaftClient(const ReindexerConfig& config) : impl_(new RPCClient(config, nullptr)), owner_(true), ctx_() {}
RaftClient::~RaftClient() {
	if (owner_) {
		delete impl_;
	}
}
RaftClient::RaftClient(RaftClient&& rdx) noexcept : impl_(rdx.impl_), owner_(rdx.owner_), ctx_(std::move(rdx.ctx_)) { rdx.owner_ = false; }
RaftClient& RaftClient::operator=(RaftClient&& rdx) noexcept {
	if (this != &rdx) {
		impl_ = rdx.impl_;
		owner_ = rdx.owner_;
		ctx_ = std::move(rdx.ctx_);
		rdx.owner_ = false;
	}
	return *this;
}

Error RaftClient::Connect(const DSN& dsn, net::ev::dynamic_loop& loop, const client::ConnectOpts& opts) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->Connect(dsn, loop, opts));
}
void RaftClient::Stop() noexcept {
	try {
		impl_->Stop();
		// Do not excepting any exceptions here
	} catch (std::exception& e) {
		fprintf(stderr, "reindexer error: unexpected exception in RaftClient::Stop: %s\n", e.what());
	} catch (...) {
		fprintf(stderr, "reindexer error: unexpected exception in RaftClient::Stop: <no description available>\n");
	}
}

Error RaftClient::SuggestLeader(const NodeData& suggestion, NodeData& response) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->SuggestLeader(suggestion, response, ctx_.WithLSN(lsn_t{0})));
}

Error RaftClient::SetDesiredLeaderId(int nextLeaderId) noexcept {
	RETURN_RESULT_NOEXCEPT(
		impl_->ClusterControlRequest(ClusterControlRequestData{SetClusterLeaderCommand{nextLeaderId}}, ctx_.WithLSN(lsn_t{0})));
}

Error RaftClient::LeadersPing(const NodeData& leader) noexcept {
	RETURN_RESULT_NOEXCEPT(impl_->LeadersPing(leader, ctx_.WithLSN(lsn_t{0})));
}

Error RaftClient::GetRaftInfo(RaftClient::RaftInfo& info) noexcept { RETURN_RESULT_NOEXCEPT(impl_->GetRaftInfo(info, ctx_)); }

// Error RaftClient::GetClusterConfig() {}

// Error RaftClient::SetClusterConfig() {}

Error RaftClient::Status(bool forceCheck) noexcept { return impl_->Status(forceCheck, ctx_); }

}  // namespace client
}  // namespace reindexer
