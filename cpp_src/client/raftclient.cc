#include "client/raftclient.h"
#include "client/rpcclient.h"
#include "cluster/clustercontrolrequest.h"

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

Error RaftClient::Connect(const std::string& dsn, net::ev::dynamic_loop& loop, const client::ConnectOpts& opts) {
	return impl_->Connect(dsn, loop, opts);
}
Error RaftClient::Stop() { return impl_->Stop(); }

Error RaftClient::SuggestLeader(const NodeData& suggestion, NodeData& response) { return impl_->SuggestLeader(suggestion, response, ctx_); }

Error RaftClient::SetDesiredLeaderId(int nextLeaderId) {
	return impl_->ClusterControlRequest(ClusterControlRequestData{SetClusterLeaderCommand{nextLeaderId}}, ctx_);
}

Error RaftClient::LeadersPing(const NodeData& leader) { return impl_->LeadersPing(leader, ctx_); }

Error RaftClient::GetRaftInfo(RaftClient::RaftInfo& info) { return impl_->GetRaftInfo(info, ctx_); }

// Error RaftClient::GetClusterConfig() {}

// Error RaftClient::SetClusterConfig() {}

Error RaftClient::Status(bool forceCheck) { return impl_->Status(forceCheck, ctx_); }

}  // namespace client
}  // namespace reindexer
