//go:build tiny_vectors

package reindexer

const (
	kTestFloatVectorDimension       = 32
	kTestHNSWFloatVectorMaxElements = 2500
	kTestIVFFloatVectorMaxElements  = 10000
	kTestBFloatVectorMaxElements    = 10000
	kBenchKnnNsSize                 = 2000
	kBenchKnnTxSize                 = 500
	kBenchFloatVectorDimension      = 8
	kBenchKnnK                      = 30
)

const (
	kTestFloatVectorArraySize            = 8
	kTestFloatVectorArrayDimension       = 8
	kTestHNSWFloatVectorArrayMaxElements = 250
)
