#pragma once

#include "estl/chunk_buf.h"
#include "estl/mutex.h"
#include "events/iexternal_listener.h"
#include "events/serializer.h"

namespace reindexer {

class [[nodiscard]] BufferedUpdateObserver : public IEventsObserver {
public:
	explicit BufferedUpdateObserver(size_t capacity) : queue_(capacity), capacity_(queue_.capacity()) {}

	size_t AvailableEventsSpace() noexcept override final { return queue_.capacity() - queue_.size_atomic(); }
	void SendEvent(uint32_t streamsMask, const EventsSerializationOpts& opts, const EventRecord& rec) override final {
		WrSerializer ser(queue_.get_chunk());
		UpdateSerializer user(ser);
		assertrx_dbg(queue_.capacity() - queue_.size() >= 1);
		queue_.write(user.Serialize(streamsMask, opts, rec));
	}
	std::span<chunk> TryReadUpdates() noexcept { return queue_.tail(); }
	void EraseUpdates(size_t count) noexcept { queue_.erase_chunks(count); }

private:
	chain_buf<mutex> queue_;
	const size_t capacity_;
};

}  // namespace reindexer
