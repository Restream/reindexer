#pragma once

#include "core/cjson/baseencoder.h"

namespace reindexer {

class AdditionalDatasource : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalDatasource(double r, IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(true), rank_(r) {}
	AdditionalDatasource(IEncoderDatasourceWithJoins *jds) : joinsDs_(jds), withRank_(false), rank_(0.0) {}
	void PutAdditionalFields(JsonBuilder &builder) const final {
		if (withRank_) builder.Put("rank()", rank_);
	}
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins *joinsDs_ = nullptr;
	bool withRank_ = false;
	double rank_ = 0.0;
};

class AdditionalDatasourceShardId : public IAdditionalDatasource<JsonBuilder> {
public:
	AdditionalDatasourceShardId(int shardId) : shardId_(shardId) {}
	void PutAdditionalFields(JsonBuilder &builder) const final { builder.Put("#shard_id", shardId_); }
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return nullptr; }

private:
	int shardId_;
};

class AdditionalDatasourceCSV : public IAdditionalDatasource<CsvBuilder> {
public:
	AdditionalDatasourceCSV(IEncoderDatasourceWithJoins *jds) : joinsDs_(jds) {}
	void PutAdditionalFields(CsvBuilder &) const final {}
	IEncoderDatasourceWithJoins *GetJoinsDatasource() final { return joinsDs_; }

private:
	IEncoderDatasourceWithJoins *joinsDs_;
};

}  // namespace reindexer
