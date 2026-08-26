#pragma once

#include <atomic>
#include <concepts>
#include <memory>
#include <optional>
#include <string_view>

#include "priority_queue.h"
#include "type_consts.h"

namespace hnswlib {

using SearchResultQueue = PriorityQueue<std::pair<float, labeltype>>;

enum class [[nodiscard]] Synchronization { None, OnInsertions };

struct [[nodiscard]] StreamingSearchOptions {
	size_t ef = 0;
};

struct [[nodiscard]] StreamingBatch {
	SearchResultQueue results;
	bool exhausted = false;
};

class [[nodiscard]] StreamingSearchSession {
	struct [[nodiscard]] Impl {
		virtual ~Impl() = default;
	};

public:
	template <std::derived_from<Impl> T>
	explicit StreamingSearchSession(T&& impl) noexcept : impl_(std::make_unique<T>(std::forward<T>(impl))) {}
	StreamingSearchSession(StreamingSearchSession&&) noexcept = default;
	StreamingSearchSession& operator=(StreamingSearchSession&&) noexcept = default;
	StreamingSearchSession(const StreamingSearchSession&) = delete;
	StreamingSearchSession& operator=(const StreamingSearchSession&) = delete;

private:
	template <typename, Synchronization>
	friend class HierarchicalNSWImpl;

	std::unique_ptr<Impl> impl_;
};

class [[nodiscard]] IWriter {
public:
	virtual ~IWriter() = default;

	virtual void PutVarUInt(uint64_t) = 0;
	virtual void PutVarUInt(uint32_t) = 0;
	virtual void PutVarInt(int64_t) = 0;
	virtual void PutVarInt(int32_t) = 0;
	virtual void PutVString(std::string_view) = 0;
	virtual void PutFloat(float) = 0;
	virtual void AppendPKByID(labeltype) = 0;
};

class [[nodiscard]] IReader {
public:
	virtual ~IReader() = default;

	virtual uint64_t GetVarUInt() = 0;
	virtual int64_t GetVarInt() = 0;
	virtual std::string_view GetVString() = 0;
	virtual float GetFloat() = 0;
	virtual labeltype ReadPkEncodedData(float* destBuf) = 0;
	virtual bool WithQuantizer() const = 0;
};

template <Synchronization synchronization>
class [[nodiscard]] HierarchicalNSWInterface {
public:
	virtual ~HierarchicalNSWInterface() = default;

	virtual bool IsQuantized() const noexcept = 0;

	virtual size_t MaxElements() const noexcept = 0;
	virtual size_t CurrentElementCount() const noexcept = 0;
	virtual size_t DeletedCountUnsafe() const noexcept = 0;
	virtual size_t AllocatedMemSize() const noexcept = 0;
	virtual size_t ElementSize() const noexcept = 0;

	virtual labeltype ExternalLabel(tableint internalId) const = 0;
	virtual uint64_t GetHash(labeltype label) const = 0;
	virtual bool IsMarkedDeleted(tableint internalId) const noexcept = 0;
	virtual const float* FloatPtrByExternalLabel(labeltype label) const = 0;

	virtual void MarkDelete(labeltype label) = 0;
	virtual void AddPointNoLock(const float* dataPoint, labeltype label) = 0;
	virtual void AddPointConcurrent(const float* dataPoint, labeltype label) = 0;
	virtual void ResizeIndex(size_t newMaxElements) = 0;
	virtual void SaveIndex(IWriter& writer, const std::atomic_int32_t& cancel) = 0;

	virtual SearchResultQueue SearchKnn(const float* queryDataRaw, std::optional<float> queryDataNorm, size_t k, size_t ef = 0) const = 0;
	virtual SearchResultQueue SearchRange(const float* queryDataRaw, std::optional<float> queryDataNorm, float radius, size_t ef) const = 0;

	// Streaming (batched) KNN. The whole session (Begin + all Continue calls) must run under an external read-lock.
	virtual StreamingSearchSession BeginStreamingSearch(const float* queryDataRaw, std::optional<float> queryDataNorm,
														StreamingSearchOptions opts) const = 0;
	virtual StreamingBatch ContinueStreamingSearch(StreamingSearchSession& session, size_t batchSize) const = 0;
};

}  // namespace hnswlib
