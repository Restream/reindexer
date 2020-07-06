#pragma once

#include "rpcclient.h"

namespace reindexer {

namespace client {

class RPCClientMock : public RPCClient {
public:
	RPCClientMock(const ReindexerConfig &config = ReindexerConfig());
	Error Insert(string_view nsName, client::Item &item, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Update(string_view nsName, client::Item &item, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Upsert(string_view nsName, client::Item &item, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Delete(string_view nsName, client::Item &item, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx, int outputFormat = FormatCJson);
	Error Select(string_view query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr,
				 int outputFormat = FormatCJson) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx, outputFormat);
	}
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr,
				 int outputFormat = FormatCJson) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx, outputFormat);
	}

private:
	Error selectImpl(string_view query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout, const InternalRdxContext &ctx,
					 int outputFormat);
	Error selectImpl(const Query &query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout,
					 const InternalRdxContext &ctx, int outputFormat);
	Error modifyItem(string_view nsName, Item &item, int mode, seconds netTimeout, const InternalRdxContext &ctx, int format);
	Error modifyItemAsync(string_view nsName, Item *item, int mode, cproto::ClientConnection *, seconds netTimeout,
						  const InternalRdxContext &ctx, int format);
};

}  // namespace client
}  // namespace reindexer
