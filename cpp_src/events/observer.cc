#include "observer.h"
#include "events/listener.h"
#include "tools/logger.h"

using namespace std::string_view_literals;

namespace reindexer {

void UpdatesObservers::SendAsyncEventOnly(updates::UpdateRecord&& rec) {
	if (eventsListener_.HasListenersFor(rec.NsName())) {
		EventsContainer events;
		events.emplace_back(std::move(rec));
		auto err = eventsListener_.SendEvents(std::move(events));
		if (!err.ok()) [[unlikely]] {
			logFmt(LogError, "Unable to send update to EventsListener: '{}'", err.what());
		}
	}
}

Error UpdatesObservers::SendUpdate(updates::UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	UpdatesContainer recs;
	recs.emplace_back(std::move(rec));
	return SendUpdates(std::move(recs), std::move(beforeWaitF), ctx);
}

Error UpdatesObservers::SendUpdates(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	if (recs.empty()) {
		return {};
	}

	if (eventsListener_.HasListenersFor(recs[0].NsName())) {
		auto err = eventsListener_.SendEvents(convertUpdatesContainer(recs));
		if (!err.ok()) [[unlikely]] {
			logFmt(LogError, "Unable to send update to EventsListener: '{}'", err.what());
		}
	}
	return replicator_.Replicate(std::move(recs), std::move(beforeWaitF), ctx);
}

Error UpdatesObservers::SendAsyncUpdate(updates::UpdateRecord&& rec, const RdxContext& ctx) {
	UpdatesContainer recs(1);
	recs[0] = std::move(rec);
	return SendAsyncUpdates(std::move(recs), ctx);
}

Error UpdatesObservers::SendAsyncUpdates(UpdatesContainer&& recs, const RdxContext& ctx) {
	if (recs.empty()) {
		return {};
	}

	if (eventsListener_.HasListenersFor(recs[0].NsName())) {
		auto err = eventsListener_.SendEvents(convertUpdatesContainer(recs));
		if (!err.ok()) [[unlikely]] {
			logFmt(LogError, "Unable to send update to EventsListener: '{}'", err.what());
		}
	}
	return replicator_.ReplicateAsync(std::move(recs), ctx);
}

EventsContainer UpdatesObservers::convertUpdatesContainer(const UpdatesContainer& data) {
	EventsContainer copy;
	copy.reserve(data.size());
	for (auto& d : data) {
		copy.emplace_back(d);
	}
	return copy;
}

}  // namespace reindexer
