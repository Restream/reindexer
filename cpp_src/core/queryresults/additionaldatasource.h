#pragma once

#include "core/cjson/baseencoder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/rank_t.h"

namespace reindexer {

template <typename Builder>
class [[nodiscard]] AdditionalDatasource final : public IAdditionalDatasource<Builder> {
public:
	AdditionalDatasource(RankT r, IEncoderDatasourceWithJoins<Builder>* jds) noexcept : joinsDs_(jds), withRank_(true), rank_(r) {}
	AdditionalDatasource(IEncoderDatasourceWithJoins<Builder>* jds) noexcept : joinsDs_(jds) {}
	void PutAdditionalFields(Builder& builder) const override {
		if (withRank_) {
			builder.Put("rank()", rank_.Value());
		}
	}
	IEncoderDatasourceWithJoins<Builder>* GetJoinsDatasource() noexcept override { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins<Builder>* joinsDs_ = nullptr;
	bool withRank_ = false;
	RankT rank_{};
};

template <typename Builder>
class [[nodiscard]] AdditionalDatasourceShardId final : public IAdditionalDatasource<Builder> {
public:
	AdditionalDatasourceShardId(int shardId) noexcept : shardId_(shardId) {}
	void PutAdditionalFields(Builder& builder) const override { builder.Put("#shard_id", shardId_); }
	IEncoderDatasourceWithJoins<Builder>* GetJoinsDatasource() noexcept override { return nullptr; }
	int GetShardId() const noexcept { return shardId_; }

private:
	int shardId_;
};

class [[nodiscard]] AdditionalDatasourceCSV final : public IAdditionalDatasource<CsvBuilder> {
public:
	AdditionalDatasourceCSV(IEncoderDatasourceWithJoins<CsvBuilder>* jds) noexcept : joinsDs_(jds) {}
	void PutAdditionalFields(CsvBuilder&) const override {}
	IEncoderDatasourceWithJoins<CsvBuilder>* GetJoinsDatasource() noexcept override { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins<CsvBuilder>* joinsDs_;
};

}  // namespace reindexer
