package reindexer

import (
	"fmt"

	"github.com/restream/reindexer/bindings"
	"github.com/restream/reindexer/cjson"
)

type rawResultItemParams struct {
	id      int
	version LsnT
	nsid    int
	proc    int
	shardid int
	cptr    uintptr
	data    []byte
}

type rawResultsExtraParam struct {
	Tag  int
	Name string
	Data []byte
}

type rawResultQueryParams struct {
	flags                 int
	totalcount            int
	qcount                int
	count                 int
	aggResults            [][]byte
	explainResults        []byte
	shardingConfigVersion int64
	shardId               int
}

type resultSerializer struct {
	cjson.Serializer
	flags int
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
		v.version = CreateLSNFromInt64(int64(s.GetVarUInt()))
	}

	if (s.flags & bindings.ResultsWithNsID) != 0 {
		v.nsid = int(s.GetVarUInt())
	}

	if (s.flags & bindings.ResultsWithPercents) != 0 {
		v.proc = int(s.GetVarUInt())
	}

	if (s.flags & bindings.ResultsWithShardId) != 0 {
		if shardId != bindings.ShardingProxyOff {
			v.shardid = shardId
		} else {
			v.shardid = int(s.GetVarUInt())
		}
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

func (s *resultSerializer) readRawQueryParams(updatePayloadType ...updatePayloadTypeFunc) (v rawResultQueryParams) {

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
	s.readExtraResults(&v)
	s.flags = v.flags

	return v
}

func (s *resultSerializer) readExtraResults(v *rawResultQueryParams) {

	v.shardingConfigVersion = -1
	v.shardId = bindings.ShardingProxyOff
	for {
		tag := s.GetVarUInt()
		if tag == bindings.QueryResultEnd {
			break
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
		}
	}
}
