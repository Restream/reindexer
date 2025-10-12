#pragma once

#include "core/rdxcontext.h"
#include "estl/atomic_unique_ptr.h"

#include <atomic>
#include <thread>

namespace reindexer {

constexpr uint64_t kBitMask = ~(uint64_t(0x7) << (sizeof(uint64_t) * 8 - 3));
constexpr uint64_t kInitFlag = uint64_t(0x4) << (sizeof(uint64_t) * 8 - 3);
constexpr uint64_t kCanceledFlag = uint64_t(0x2) << (sizeof(uint64_t) * 8 - 3);
constexpr uint64_t kCancelingFlag = uint64_t(0x1) << (sizeof(uint64_t) * 8 - 3);

class [[nodiscard]] CancelContextImpl : public IRdxCancelContext {
public:
	CancelContextImpl() : cancelType_(CancelType::None) {}
	CancelContextImpl(const CancelContextImpl& ctx) : cancelType_(ctx.GetCancelType()) {}
	CancelContextImpl& operator=(const CancelContextImpl& ctx) {
		cancelType_ = ctx.GetCancelType();
		return *this;
	}

	bool IsCancelable() const noexcept override final { return true; }
	CancelType GetCancelType() const noexcept override final { return cancelType_.load(std::memory_order_acquire); }
	std::optional<std::chrono::milliseconds> GetRemainingTimeout() const noexcept override final { return std::nullopt; }

	bool IsCancelled() const { return cancelType_.load(std::memory_order_acquire) == CancelType::Explicit; }
	void Cancel(CancelType type = CancelType::Explicit) noexcept { cancelType_.store(type, std::memory_order_release); }
	void Reset() noexcept { cancelType_.store(CancelType::None, std::memory_order_release); }

private:
	std::atomic<CancelType> cancelType_;
};

template <typename ContextT>
class [[nodiscard]] ContextsPoolImpl {
public:
	struct [[nodiscard]] Node {
		Node() = default;

		ContextT ctx;
		std::atomic<uint64_t> ctxID;
		atomic_unique_ptr<Node> next;
	};

	ContextsPoolImpl(size_t baseSize) : contexts_(baseSize) { assertrx(baseSize > 0); }
	~ContextsPoolImpl() {}

	ContextT* getContext(uint64_t id) noexcept {
		uint64_t ctxID = id & kBitMask;
		if (!ctxID) {
			return nullptr;
		}
		auto head = &contexts_[ctxID % contexts_.size()];

		if (ctxIDExists(head, nullptr, ctxID)) {
			return nullptr;
		}
		Node* next = nullptr;
		auto node = head;
		do {
			auto curCtxID = node->ctxID.load(std::memory_order_acquire);
			if (!curCtxID) {
				if (node->ctxID.compare_exchange_strong(curCtxID, ctxID, std::memory_order_acq_rel)) {
					node->ctx = ContextT();
					if (ctxIDExists(head, node, ctxID)) {
						node->ctxID.store(0, std::memory_order_release);
						return nullptr;
					}
					node->ctxID.store(toInitialized(ctxID), std::memory_order_release);
					return &node->ctx;
				}
			}
			next = node->next.get(std::memory_order_acquire);
			if (next) {
				node = next;
			}
		} while (next);

		node = appendNewNode(node, ctxID);
		if (ctxIDExists(head, node, ctxID)) {
			node->ctxID.store(0, std::memory_order_release);
			return nullptr;	 // Already exists
		}
		node->ctxID.store(toInitialized(ctxID), std::memory_order_release);
		return &node->ctx;
	}
	bool cancelContext(uint64_t id, CancelType how) noexcept {
		uint64_t ctxID = id & kBitMask;
		if (!ctxID) {
			return false;
		}
		auto node = &contexts_[ctxID % contexts_.size()];
		bool found = false;
		while (node) {
			auto curCtxID = node->ctxID.load(std::memory_order_acquire);
			while (areEqual(curCtxID, ctxID) && !isInitialized(curCtxID)) {
				found = true;
				std::this_thread::yield();
				curCtxID = node->ctxID.load(std::memory_order_acquire);
			}

			if (areEqual(curCtxID, ctxID) && isCanceled(curCtxID)) {
				return true;
			}

			while (areEqual(curCtxID, ctxID) && isInitialized(curCtxID)) {
				if (node->ctxID.compare_exchange_strong(curCtxID, toCanceling(curCtxID), std::memory_order_acq_rel)) {
					node->ctx.Cancel(how);
					node->ctxID.store(toCanceled(curCtxID), std::memory_order_release);
					return true;
				}
			}
			node = node->next.get(std::memory_order_acquire);
		}
		return found;
	}
	bool removeContext(uint64_t id) noexcept {
		uint64_t ctxID = id & kBitMask;
		if (!ctxID) {
			return false;
		}
		auto node = &contexts_[ctxID % contexts_.size()];
		while (node) {
			auto curCtxID = node->ctxID.load(std::memory_order_acquire);
			while (areEqual(curCtxID, ctxID) && isCanceling(curCtxID)) {
				std::this_thread::yield();
				curCtxID = node->ctxID.load(std::memory_order_acquire);
			}
			while (areEqual(curCtxID, ctxID) && isInitialized(curCtxID)) {
				if (node->ctxID.compare_exchange_strong(curCtxID, 0, std::memory_order_acq_rel)) {
					return true;
				}
			}
			node = node->next.get(std::memory_order_acquire);
		}
		return false;
	}

	static bool isInitialized(uint64_t ctxID) noexcept { return ctxID & kInitFlag; }
	static bool isCanceling(uint64_t ctxID) noexcept { return ctxID & kCancelingFlag; }
	static bool isCanceled(uint64_t ctxID) noexcept { return ctxID & kCanceledFlag; }

	const std::vector<Node>& contexts() const noexcept { return contexts_; }

private:
	static bool areEqual(uint64_t ctxID1, uint64_t ctxID2) noexcept { return ((ctxID1 & kBitMask) == (ctxID2 & kBitMask)); }
	static uint64_t toCanceled(uint64_t ctxID) noexcept { return (ctxID & ~kCancelingFlag) | kCanceledFlag; }
	static uint64_t toCanceling(uint64_t ctxID) noexcept { return (ctxID & ~kCanceledFlag) | kCancelingFlag; }
	static uint64_t toInitialized(uint64_t ctxID) noexcept { return (ctxID & kBitMask) | kInitFlag; }

	bool ctxIDExists(const Node* first, const Node* last, uint64_t ctxID) noexcept {
		auto node = first;
		while (node && node != last) {
			if (areEqual(ctxID, node->ctxID.load(std::memory_order_acquire))) {
				return true;
			}
			node = node->next.get(std::memory_order_acquire);
		}
		return false;
	}
	Node* appendNewNode(Node* here, uint64_t ctxID) {
		Node* node = new Node;
		node->ctxID.store(ctxID, std::memory_order_relaxed);
		for (;;) {
			Node* exp = nullptr;
			if (!here->next && here->next.compare_exchange_strong(exp, node, std::memory_order_acq_rel)) {
				return node;
			}
			here = here->next.get(std::memory_order_acquire);
		}
	}

	std::vector<Node> contexts_;
};

}  // namespace reindexer
