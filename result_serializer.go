package reindexer

import "github.com/restream/reindexer/cjson"

type rawResultItemParams struct {
	id      int
	version int
	nsid    int
	proc    int
	cptr    uintptr
}

type rawResultQueryParams struct {
	totalcount  int
	count       int
	haveProcent bool
	nsCount     int
	nsVersions  [16]int
	aggResults  []float64
}

type resultSerializer struct {
	cjson.Serializer
}

func newSerializer(buf []byte) resultSerializer {
	return resultSerializer{Serializer: cjson.NewSerializer(buf)}
}
func (s *resultSerializer) readRawtItemParams() (v rawResultItemParams) {
	v.id = s.GetCInt()
	v.version = s.GetCInt16()
	v.nsid = s.GetCUInt8()
	v.proc = s.GetCUInt8()
	v.cptr = uintptr(s.GetUInt64())
	return v
}
func (s *resultSerializer) readSimpleRawItemParams() (v rawResultItemParams) {
	v.id = s.GetCInt()
	v.version = s.GetCInt()
	return v
}
func (s *resultSerializer) readRawQueryParams() (v rawResultQueryParams) {
	_ = s.GetUInt64()
	v.totalcount = s.GetCInt()
	v.count = s.GetCInt()
	v.haveProcent = (s.GetCInt() != 0)
	v.nsCount = s.GetCInt()
	for i := 0; i < v.nsCount; i++ {
		v.nsVersions[i] = s.GetCInt()
	}
	v.aggResults = s.readAggregationResults()

	return v
}

func (s *resultSerializer) readAggregationResults() (aggResults []float64) {
	aggResCount := s.GetCInt()
	if aggResCount == 0 {
		return nil
	}

	aggResults = make([]float64, aggResCount)

	for i := 0; i < aggResCount; i++ {
		aggResults[i] = s.GetDouble()
	}
	return
}
