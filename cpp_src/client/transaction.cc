#include "client/transaction.h"
#include "client/reindexerimpl.h"
#include "core/cjson/tagsmatcher.h"
#include "tools/logger.h"

namespace reindexer {
namespace client {

static const auto kBadTxStatus = Error(errBadTransaction, "Transaction is free");

Item Transaction::NewItem() noexcept {
	if (!Status().ok()) [[unlikely]] {
		return Item(Status());
	}
	if (!IsFree()) {
		try {
			return rx_->newItemTx(tr_);
		} catch (std::exception& err) {
			return Item(std::move(err));
		}
	}
	return Item(kBadTxStatus);
}

Transaction::~Transaction() {
	if (!IsFree()) {
		auto err = rx_->RollBackTransaction(*this, InternalRdxContext(lsn_t{0}, nullptr, 0));
		(void)err;	// ignore
	}
	tr_.clear();
}

PayloadType Transaction::GetPayloadType() const noexcept { return tr_.GetPayloadType(); }
TagsMatcher Transaction::GetTagsMatcher() const noexcept { return tr_.GetTagsMatcher(); }

int64_t Transaction::GetTransactionId() const noexcept { return tr_.i_.txId_; }

Error Transaction::Modify(Item&& item, ItemModifyMode mode, lsn_t lsn) noexcept {
	try {
		modify(std::move(item), mode, InternalRdxContext(std::move(lsn)));
		return {};
	} catch (std::exception& e) {
		return e;
	}
}
Error Transaction::Modify(Item&& item, ItemModifyMode mode, Completion cmpl, lsn_t lsn) noexcept {
	try {
		modify(std::move(item), mode, InternalRdxContext(std::move(lsn)).WithCompletion(std::move(cmpl)));
		return {};
	} catch (std::exception& e) {
		return e;
	}
}
Error Transaction::PutMeta(std::string_view key, std::string_view value, lsn_t lsn) noexcept {
	try {
		putMeta(key, value, InternalRdxContext(std::move(lsn)));
		return {};
	} catch (std::exception& e) {
		return e;
	}
}
Error Transaction::SetTagsMatcher(TagsMatcher&& tm, lsn_t lsn) noexcept {
	try {
		setTagsMatcher(std::move(tm), InternalRdxContext(std::move(lsn)));
		return {};
	} catch (std::exception& e) {
		return e;
	}
}
Error Transaction::Modify(Query&& query, lsn_t lsn) noexcept {
	try {
		modify(std::move(query), InternalRdxContext(std::move(lsn)));
		return {};
	} catch (std::exception& e) {
		return e;
	}
}

static void safeCallCompletion(const InternalRdxContext& ctx, const Error& err) noexcept {
	if (ctx.cmpl()) {
		try {
			ctx.cmpl()(err);
		} catch (std::exception& e) {
			logFmt(LogError, "Transaction::modify: completion function threw an exception: {}", e.what());
		}
	}
}

void Transaction::modify(Item&& item, ItemModifyMode mode, InternalRdxContext&& ctx) {
	checkStatus(ctx);

	if (!IsFree()) {
		try {
			auto err = rx_->addTxItem(*this, std::move(item), mode, ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
			if (!err.ok()) [[unlikely]] {
				throw err;
			}
			return;
		} catch (std::exception& e) {
			setStatus(std::move(e));
			auto status = Status();
			safeCallCompletion(ctx, status);
			throw status;
		}
	}
	safeCallCompletion(ctx, kBadTxStatus);
	throw kBadTxStatus;
}

void Transaction::modify(Query&& query, InternalRdxContext&& ctx) {
	checkStatus(ctx);

	if (!IsFree()) {
		try {
			auto err = rx_->modifyTx(*this, std::move(query), ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
			if (!err.ok()) [[unlikely]] {
				throw err;
			}
			return;
		} catch (std::exception& e) {
			setStatus(std::move(e));
			auto status = Status();
			safeCallCompletion(ctx, status);
			throw status;
		}
	}
	safeCallCompletion(ctx, kBadTxStatus);
	throw kBadTxStatus;
}

void Transaction::putMeta(std::string_view key, std::string_view value, InternalRdxContext&& ctx) {
	checkStatus(ctx);

	if (!IsFree()) {
		try {
			auto err = rx_->putTxMeta(*this, key, value, ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
			if (!err.ok()) [[unlikely]] {
				throw err;
			}
			return;
		} catch (std::exception& e) {
			setStatus(std::move(e));
			auto status = Status();
			safeCallCompletion(ctx, status);
			throw status;
		}
	}
	safeCallCompletion(ctx, kBadTxStatus);
	throw kBadTxStatus;
}

void Transaction::setTagsMatcher(TagsMatcher&& tm, InternalRdxContext&& ctx) {
	checkStatus(ctx);

	if (!IsFree()) {
		try {
			auto err = rx_->setTxTm(*this, std::move(tm), ctx.WithEmitterServerId(tr_.i_.emitterServerId_));
			if (!err.ok()) [[unlikely]] {
				throw err;
			}
			return;
		} catch (std::exception& e) {
			setStatus(std::move(e));
			auto status = Status();
			safeCallCompletion(ctx, status);
			throw status;
		}
	}
	safeCallCompletion(ctx, kBadTxStatus);
	throw kBadTxStatus;
}

void Transaction::checkStatus(const InternalRdxContext& ctx) {
	if (!Status().ok()) [[unlikely]] {
		auto status = Status();
		safeCallCompletion(ctx, status);
		throw status;
	}
}

}  // namespace client
}  // namespace reindexer
