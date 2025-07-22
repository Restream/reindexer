package reindexer

import (
	"fmt"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

type rawResultItemParams struct {
	id      int
	version LsnT
	nsid    int
	rank    float32
	shardid int
	cptr    uintptr
	data    []byte
}

type nsTagsMap map[int][]int64

type rawResultQueryParams struct {
	flags                 int
	totalcount            int
	qcount                int
	count                 int
	aggResults            [][]byte
	explainResults        []byte
	nsIncarnationTags     nsTagsMap
	shardingConfigVersion int64
	shardId               int
	rankFormat            *uint64
}

type resultSerializer struct {
	cjson.Serializer
	flags      int
	rankFormat *uint64
}

type updatePayloadTypeFunc func(nsid int)

func newSerializer(buf []byte) resultSerializer {
	return resultSerializer{
		Serializer: cjson.NewSerializer(buf),
	}
}
func (s *resultSerializer) readRawtItemParams(shardId int) (v rawResultItemParams) {

	if (s.flags & bindings.ResultsWithItemID) != 0 {
		v.id = int(s.GetVarUInt())
		v.version = bindings.CreateLSNFromInt64(int64(s.GetVarUInt()))
	}

	if (s.flags & bindings.ResultsWithNsID) != 0 {
		v.nsid = int(s.GetVarUInt())
	}

	if (s.flags & bindings.ResultsWithRank) != 0 {
		if s.rankFormat != nil {
			v.rank = s.GetFloat32()
		} else {
			v.rank = float32(s.GetVarUInt())
		}

	}

	if (s.flags & bindings.ResultsWithShardId) != 0 {
		if shardId != bindings.ShardingNotSet {
			v.shardid = shardId
		} else {
			v.shardid = int(s.GetVarUInt())
		}
	} else {
		v.shardid = bindings.ShardingProxyOff
	}

	switch s.flags & bindings.ResultsFormatMask {
	case bindings.ResultsPure:
	case bindings.ResultsPtrs:
		v.cptr = uintptr(s.GetUInt64())
	case bindings.ResultsJson, bindings.ResultsCJson:
		v.data = s.GetBytes()
	}
	return v
}

func (s *resultSerializer) readRawQueryParamsKeepExtras(v *rawResultQueryParams, updatePayloadType ...updatePayloadTypeFunc) {

	v.flags = int(s.GetVarUInt())
	v.totalcount = int(s.GetVarUInt())
	v.qcount = int(s.GetVarUInt())
	v.count = int(s.GetVarUInt())

	if (v.flags & bindings.ResultsWithPayloadTypes) != 0 {
		ptCount := int(s.GetVarUInt())
		for i := 0; i < ptCount; i++ {
			nsid := int(s.GetVarUInt())
			nsname := s.GetVString()
			_ = nsname
			if (len(updatePayloadType)) != 1 {
				panic(fmt.Errorf("Internal error: Got payload types from raw query params, but there are no updatePayloadType"))
			}
			updatePayloadType[0](nsid)
		}
	}
	s.readExtraResults(v)
	s.flags = v.flags
	s.rankFormat = v.rankFormat
}

func (s *resultSerializer) readRawQueryParams(updatePayloadType ...updatePayloadTypeFunc) (v rawResultQueryParams) {
	s.readRawQueryParamsKeepExtras(&v, updatePayloadType...)
	return v
}

func (s *resultSerializer) readExtraResults(v *rawResultQueryParams) {
	firstAgg := true
	v.shardingConfigVersion = -1
	v.shardId = bindings.ShardingNotSet
	for {
		tag := s.GetVarUInt()
		if tag == bindings.QueryResultEnd {
			break
		}
		if firstAgg && (tag == bindings.QueryResultAggregation || tag == bindings.QueryResultExplain) {
			v.aggResults = v.aggResults[:0]
			v.explainResults = v.explainResults[:0]
			firstAgg = false
		}

		switch tag {
		case bindings.QueryResultExplain:
			v.explainResults = s.GetBytes()
		case bindings.QueryResultAggregation:
			v.aggResults = append(v.aggResults, s.GetBytes())
		case bindings.QueryResultShardingVersion:
			v.shardingConfigVersion = s.GetVarInt()
		case bindings.QueryResultShardId:
			v.shardId = int(s.GetVarUInt())
		case bindings.QueryResultIncarnationTags:
			shardsCnt := uint(s.GetVarUInt())
			v.nsIncarnationTags = make(nsTagsMap)
			for i := uint(0); i < shardsCnt; i++ {
				shardID := int(s.GetVarInt())
				nsCnt := uint(s.GetVarUInt())
				if nsCnt > 0 {
					sl := make([]int64, nsCnt)
					for j := uint(0); j < nsCnt; j++ {
						sl[j] = s.GetVarInt()
					}
					v.nsIncarnationTags[shardID] = sl
				}
			}
		case bindings.QueryResultRankFormat:
			format := s.GetVarUInt()
			if format != bindings.RankFormatSingleFloat {
				panic(fmt.Sprintf("unexpected rank format value: %d - only supported format is 0 (single float rank)", format))
			}
			v.rankFormat = &format
		}
	}
}
