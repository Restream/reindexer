#pragma once

#include "client/reindexerimpl.h"

namespace reindexer {
namespace client {

class [[nodiscard]] RPCTestClient {
public:
	RPCTestClient() : impl_{std::make_shared<ReindexerImpl>(ReindexerConfig(), 1, 1)} {}
	Error Connect(const DSN& dsn, const client::ConnectOpts& opts) { return impl_->Connect(dsn, opts); }
	void Stop() { impl_->Stop(); }
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts) {
		return impl_->OpenNamespace(nsName, ctx_, opts, NsReplicationOpts());
	}
	Error AddIndex(std::string_view nsName, const IndexDef& index) { return impl_->AddIndex(nsName, index, ctx_); }
	Item NewItem(std::string_view nsName) { return impl_->NewItem(nsName, ctx_); }
	Error Insert(std::string_view nsName, client::Item& item, RPCDataFormat format) { return impl_->Insert(nsName, item, format, ctx_); }
	Error Update(std::string_view nsName, client::Item& item, RPCDataFormat format) { return impl_->Update(nsName, item, format, ctx_); }
	Error Upsert(std::string_view nsName, client::Item& item, RPCDataFormat format) { return impl_->Upsert(nsName, item, format, ctx_); }
	Error Delete(std::string_view nsName, client::Item& item, RPCDataFormat format) { return impl_->Delete(nsName, item, format, ctx_); }
	Error Delete(const Query& query, QueryResults& result) { return impl_->Delete(query, result, ctx_); }
	Error Update(const Query& query, QueryResults& result) { return impl_->Update(query, result, ctx_); }
	Error Select(const Query& query, QueryResults& result) { return impl_->Select(query, result, ctx_); }

private:
	std::shared_ptr<ReindexerImpl> impl_;
	InternalRdxContext ctx_;
};

}  // namespace client
}  // namespace reindexer
