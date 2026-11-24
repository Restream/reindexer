#pragma once

#include "client/reindexerimpl.h"

namespace reindexer_tests {

class [[nodiscard]] RPCTestClient {
public:
	RPCTestClient() : impl_{std::make_shared<reindexer::client::ReindexerImpl>(reindexer::client::ReindexerConfig(), 1, 1)} {}
	reindexer::Error Connect(const reindexer::DSN& dsn, const reindexer::client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
	void Stop() { impl_->Stop(); }
	reindexer::Error OpenNamespace(std::string_view nsName, const ::StorageOpts& opts) {
		return impl_->OpenNamespace(nsName, ctx_, opts, reindexer::NsReplicationOpts());
	}
	reindexer::Error AddIndex(std::string_view nsName, const reindexer::IndexDef& index) { return impl_->AddIndex(nsName, index, ctx_); }
	reindexer::client::Item NewItem(std::string_view nsName) { return impl_->NewItem(nsName, ctx_); }
	reindexer::Error Insert(std::string_view nsName, reindexer::client::Item& item, reindexer::client::RPCDataFormat format) {
		return impl_->Insert(nsName, item, format, ctx_);
	}
	reindexer::Error Update(std::string_view nsName, reindexer::client::Item& item, reindexer::client::RPCDataFormat format) {
		return impl_->Update(nsName, item, format, ctx_);
	}
	reindexer::Error Upsert(std::string_view nsName, reindexer::client::Item& item, reindexer::client::RPCDataFormat format) {
		return impl_->Upsert(nsName, item, format, ctx_);
	}
	reindexer::Error Delete(std::string_view nsName, reindexer::client::Item& item, reindexer::client::RPCDataFormat format) {
		return impl_->Delete(nsName, item, format, ctx_);
	}
	reindexer::Error Delete(const reindexer::Query& query, reindexer::client::QueryResults& result) {
		return impl_->Delete(query, result, ctx_);
	}
	reindexer::Error Update(const reindexer::Query& query, reindexer::client::QueryResults& result) {
		return impl_->Update(query, result, ctx_);
	}
	reindexer::Error Select(const reindexer::Query& query, reindexer::client::QueryResults& result) {
		return impl_->Select(query, result, ctx_);
	}

private:
	std::shared_ptr<reindexer::client::ReindexerImpl> impl_;
	reindexer::client::InternalRdxContext ctx_;
};

}  // namespace reindexer_tests
