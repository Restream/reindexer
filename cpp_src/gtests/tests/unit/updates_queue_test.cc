#include <algorithm>
#include <array>
#include <future>
#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/rdxcontext.h"
#include "gtest/gtest.h"
#include "tools/catch_and_return.h"
#include "updates/updaterecord.h"
#include "updates/updatesqueue.h"
#include "vendor/fmt/ranges.h"

namespace reindexer_tests {

using namespace reindexer;

namespace {

using updates::EmitterResult;
using updates::ReplicationResult;
using Queue = updates::UpdatesQueue<updates::UpdateRecord, cluster::ReplicationStatsCollector, cluster::Logger>;

constexpr int kEmitterServerId = 1;
constexpr int kNoEmitter = -1;
constexpr auto kResultTimeout = std::chrono::seconds(10);

struct [[nodiscard]] ClusterSize {
	uint32_t consensus;
	uint32_t requiredReplicas;
};
constexpr ClusterSize kThreeNodes{.consensus = 2, .requiredReplicas = 2};
constexpr ClusterSize kFiveNodes{.consensus = 3, .requiredReplicas = 4};

enum class [[nodiscard]] Marker { Emitter, Follower };

Queue::UpdatesContainerT makeUpdate(int emitterServerId, size_t dataSize = 0) {
	Queue::UpdatesContainerT updates;
	updates.emplace_back(updates::URType::SetSchema, NamespaceName("ns"), lsn_t(), lsn_t(), emitterServerId, std::string(dataSize, 'x'));
	return updates;
}

ReplicationResult mark(Queue& queue, uint64_t id, ClusterSize cluster, Marker marker, Error err = Error()) {
	auto chunk = queue.Read(id, std::nullopt);
	if (!chunk || chunk->IsUpdatesDropBlock) {
		ADD_FAILURE() << "Update " << id << " is not in the queue";
		return ReplicationResult::None;
	}
	return chunk->OnUpdateHandled(0, cluster.consensus, cluster.requiredReplicas, uint16_t(id - chunk->ID()), marker == Marker::Emitter,
								  std::move(err));
}

EmitterResult emitterResult(Queue& queue, uint64_t id) {
	auto chunk = queue.Read(id, std::nullopt);
	return chunk->GetUpdate(uint16_t(id - chunk->ID())).GetCounters().GetEmitterResult();
}

bool isDropped(Queue& queue, uint64_t id) {
	auto chunk = queue.Read(id, std::nullopt);
	return chunk && chunk->IsUpdatesDropBlock;
}

// Pushes more updates, than the queue's limit, so its first chunk becomes the candidate for the drop
void overflow(Queue& queue) {
	while (queue.GetNextUpdateID() < Queue::kBatchSize) {
		ASSERT_TRUE(queue.PushAsync<true>(makeUpdate(kNoEmitter)).first.ok());
	}
	ASSERT_TRUE(queue.PushAsync<true>(makeUpdate(kNoEmitter, 2 * Queue::kMinUpdatesBound)).first.ok());
}

// Write, proxied by the emitter, as the leader handles it: PushAndWait awaits the result in a separate thread. The update is in the
// queue, when the constructor returns
class [[nodiscard]] AwaitedUpdate {
public:
	AwaitedUpdate(Queue& queue, int emitterServerId) : queue_(queue), id_(queue.GetNextUpdateID()) {
		std::promise<void> pushed;
		auto pushedFuture = pushed.get_future();
		result_ = std::async(std::launch::async, [&queue, emitterServerId, pushed = std::move(pushed)]() mutable {
			return queue.PushAndWait(makeUpdate(emitterServerId), [&pushed] { pushed.set_value(); }, RdxContext()).first;
		});
		pushedFuture.wait();
	}
	~AwaitedUpdate() {
		if (result_.valid() && result_.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
			queue_.SetWritable(false, Error(errCanceled, "Test is finished"));
		}
	}

	uint64_t ID() const noexcept { return id_; }
	ReplicationResult Mark(ClusterSize cluster, Marker marker, Error err = Error()) {
		return mark(queue_, id_, cluster, marker, std::move(err));
	}
	Error Result() noexcept {
		try {
			if (result_.wait_for(kResultTimeout) != std::future_status::ready) {
				return Error(errTimeout, "The result was not sent");
			}
			return result_.get();
		} CATCH_AND_RETURN
	}

private:
	Queue& queue_;
	const uint64_t id_;
	std::future<Error> result_;
};

}  // namespace

TEST(UpdatesQueue, ResultIsSentOnceWhenEmitterAndConsensusAreKnown) {
	// For any order and outcomes of the marks in the 5 nodes cluster the only result is sent by the mark, which makes both
	// the emitter's outcome and the consensus known. The result is OK, if both the emitter and the majority have approved the update.
	// Otherwise it is the error of the sending mark, or, if that mark is OK, names the missing emitter's approve or majority
	const Error kMarkError(errNetwork, "Connection refused");
	struct [[nodiscard]] NodeMark {
		Marker marker;
		bool ok;
	};
	for (unsigned outcomes = 0; outcomes < 16; ++outcomes) {
		std::array<NodeMark, 4> marks{NodeMark{Marker::Emitter, false}, NodeMark{Marker::Follower, false},
									  NodeMark{Marker::Follower, false}, NodeMark{Marker::Follower, false}};
		for (size_t i = 0; i < marks.size(); ++i) {
			marks[i].ok = (outcomes >> i) & 1;
		}
		std::array<size_t, 4> order{0, 1, 2, 3};
		do {
			SCOPED_TRACE(fmt::format("Outcomes (emitter first): {:04b}, order: {}", outcomes, fmt::join(order, ",")));
			Queue queue(0);
			AwaitedUpdate update(queue, kEmitterServerId);
			uint32_t approves = 1, errors = 0;
			bool isEmitterMarked = false;
			std::optional<size_t> expectedSentAt, sentAt;
			ReplicationResult sentResult = ReplicationResult::None;
			bool isSentMarkOk = false;
			for (size_t i = 0; i < order.size(); ++i) {
				const auto& mark = marks[order[i]];
				if (mark.ok) {
					++approves;
				} else {
					++errors;
				}
				isEmitterMarked = isEmitterMarked || (mark.marker == Marker::Emitter);
				if (!expectedSentAt && isEmitterMarked && (approves >= kFiveNodes.consensus || errors >= kFiveNodes.consensus)) {
					expectedSentAt = i;
				}
				const auto res = update.Mark(kFiveNodes, mark.marker, mark.ok ? Error() : kMarkError);
				if (res != ReplicationResult::None) {
					EXPECT_FALSE(sentAt.has_value()) << "Second result at mark " << i;
					sentAt = i;
					sentResult = res;
					isSentMarkOk = mark.ok;
				}
			}
			const bool isApproved = (approves >= kFiveNodes.consensus);
			ASSERT_EQ(sentAt, expectedSentAt);
			EXPECT_EQ(sentResult, isApproved ? ReplicationResult::Approved : ReplicationResult::Error);
			const auto err = update.Result();
			if (isApproved && marks[0].ok) {
				EXPECT_TRUE(err.ok()) << err.what();
			} else if (!isSentMarkOk) {
				EXPECT_EQ(err.whatStr(), kMarkError.whatStr());
			} else {
				EXPECT_EQ(err.code(), errUpdateReplication) << err.what();
				EXPECT_EQ(err.whatStr(), marks[0].ok ? updates::kNotEnoughReplicasMsg : updates::kNotReplicatedToEmitterMsg);
			}
		} while (std::next_permutation(order.begin(), order.end()));
	}
}

TEST(UpdatesQueue, RepeatedEmitterMarkKeepsEmitterResult) {
	// Async queue awaits no results: the emitter's result is only required to be set before the erasure, and the update may be marked as
	// the emitter's one by several targets. Repeated marks keep the first result, and the update without emitter stays applied
	Queue queue(0);
	ASSERT_TRUE(queue.PushAsync<true>(makeUpdate(kEmitterServerId)).first.ok());
	ASSERT_TRUE(queue.PushAsync<true>(makeUpdate(kNoEmitter)).first.ok());
	constexpr uint64_t kWithEmitterId = 0, kNoEmitterId = 1;
	EXPECT_EQ(emitterResult(queue, kWithEmitterId), EmitterResult::Pending);
	EXPECT_EQ(emitterResult(queue, kNoEmitterId), EmitterResult::Applied);

	std::ignore = mark(queue, kWithEmitterId, kFiveNodes, Marker::Emitter, Error(errVersion, "Old version"));
	std::ignore = mark(queue, kWithEmitterId, kFiveNodes, Marker::Emitter);
	std::ignore = mark(queue, kNoEmitterId, kFiveNodes, Marker::Emitter, Error(errVersion, "Old version"));
	EXPECT_EQ(emitterResult(queue, kWithEmitterId), EmitterResult::Failed);
	EXPECT_EQ(emitterResult(queue, kNoEmitterId), EmitterResult::Applied);
}

TEST(UpdatesQueue, OverflowDropsChunkAwaitingEmitterOnly) {
	// The overflowing queue drops the chunk, whose result awaits the emitter only, and fails the result
	Queue queue(Queue::kMinUpdatesBound);
	AwaitedUpdate update(queue, kEmitterServerId);
	EXPECT_EQ(update.Mark(kThreeNodes, Marker::Follower), ReplicationResult::None);
	overflow(queue);
	EXPECT_TRUE(isDropped(queue, update.ID()));
	const auto err = update.Result();
	EXPECT_EQ(err.code(), errUpdateReplication) << err.what();
	EXPECT_EQ(err.whatStr(), fmt::format("{}: updates queue overflow", updates::kNotReplicatedToEmitterMsg));
}

TEST(UpdatesQueue, OverflowKeepsChunkAwaitingConsensus) {
	// The overflowing queue keeps the chunk, whose result awaits the consensus: the lagging majority is awaited
	Queue queue(Queue::kMinUpdatesBound);
	AwaitedUpdate update(queue, kEmitterServerId);
	overflow(queue);
	EXPECT_FALSE(isDropped(queue, update.ID()));
	EXPECT_EQ(update.Mark(kThreeNodes, Marker::Emitter), ReplicationResult::Approved);
	EXPECT_TRUE(update.Result().ok());
}

TEST(UpdatesQueue, OverflowDropsChunkWithoutAwaiter) {
	// The overflowing queue drops the chunk with the RAFT update, which nobody awaits (PushNowait, e.g. a step of the proxied transaction),
	// after the consensus: only the emitter's mark is pending, and nobody gets the result
	Queue queue(Queue::kMinUpdatesBound);
	ASSERT_TRUE(queue.PushAsync<false>(makeUpdate(kEmitterServerId)).first.ok());
	constexpr uint64_t kId = 0;
	EXPECT_EQ(mark(queue, kId, kThreeNodes, Marker::Follower), ReplicationResult::None);
	overflow(queue);
	EXPECT_TRUE(isDropped(queue, kId));
}

}  // namespace reindexer_tests
