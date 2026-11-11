#pragma once

#include <optional>
#include <unordered_set>
#include <vector>
#include "activity.h"
#include "estl/mutex.h"

#ifdef RX_LOGACTIVITY
#include "activity_log.h"
#endif

namespace reindexer {

class RdxActivityContext;

class [[nodiscard]] ActivityContainer {
public:
	void Register(const RdxActivityContext*);
	void Unregister(const RdxActivityContext*);
	void Reregister(const RdxActivityContext* oldCtx, const RdxActivityContext* newCtx);
	std::vector<Activity> List(int serverId);
	std::optional<std::string> QueryForIpConnection(int id);

	void Reset();

#ifdef RX_LOGACTIVITY
	void AddOperation(const RdxActivityContext* ctx, Activity::State st, bool start);

private:
	ActivityContainerLog log_;
#endif
private:
	mutex mtx_;
	std::unordered_set<const RdxActivityContext*> cont_;
};

}  // namespace reindexer
