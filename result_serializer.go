package reindexer

import (
	"fmt"

	"git.itv.restr.im/itv-backend/reindexer/bindings"
	"git.itv.restr.im/itv-backend/reindexer/cjson"
)

type rawResultItemParams struct {
	id      int
	version int
	nsid    int
	proc    int
	cptr    uintptr
	data    []byte
}

type rawResultQueryParams struct {
	totalcount  int
	qcount      int
	count       int
	haveProcent bool
	nsCount     int
	aggResults  []float64
}

type resultSerializer struct {
	cjson.Serializer
	haveCPtr bool
}

type updatePayloadTypeFunc func(nsid int)

func newSerializer(buf []byte) resultSerializer {
	return resultSerializer{
		Serializer: cjson.NewSerializer(buf),
		haveCPtr:   false,
	}
}
func (s *resultSerializer) readRawtItemParams() (v rawResultItemParams) {
	v.id = int(s.GetVarUInt())
	v.version = int(s.GetVarUInt())
	v.nsid = int(s.GetVarUInt())
	v.proc = int(s.GetVarUInt())
	format := int(s.GetVarUInt())

	switch format {
	case bindings.ResultsPure:
	case bindings.ResultsWithPtrs:
		v.cptr = uintptr(s.GetUInt64())
	case bindings.ResultsWithJson, bindings.ResultsWithCJson:
		v.data = s.GetBytes()
	}
	return v
}
func (s *resultSerializer) readSimpleRawItemParams() (v rawResultItemParams) {
	v.id = int(s.GetVarUInt())
	v.version = int(s.GetVarUInt())
	return v
}
func (s *resultSerializer) readRawQueryParams(updatePayloadType ...updatePayloadTypeFunc) (v rawResultQueryParams) {
	s.haveCPtr = s.GetUInt64() != 0
	v.totalcount = int(s.GetVarUInt())
	v.qcount = int(s.GetVarUInt())
	v.count = int(s.GetVarUInt())
	v.haveProcent = (s.GetVarUInt() != 0)
	v.nsCount = int(s.GetVarUInt())

	ptCount := int(s.GetVarUInt())
	for i := 0; i < ptCount; i++ {
		nsid := int(s.GetVarUInt())
		if (len(updatePayloadType)) != 1 {
			panic(fmt.Errorf("Internal error: Got payload types from raw query params, but there are no updatePayloadType"))
		}
		updatePayloadType[0](nsid)
	}

	v.aggResults = s.readAggregationResults()

	return v
}

func (s *resultSerializer) readAggregationResults() (aggResults []float64) {
	aggResCount := int(s.GetVarUInt())
	if aggResCount == 0 {
		return nil
	}

	aggResults = make([]float64, aggResCount)

	for i := 0; i < aggResCount; i++ {
		aggResults[i] = s.GetDouble()
	}
	return
}
