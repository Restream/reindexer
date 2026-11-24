//go:build !tiny_vectors

package reindexer

const (
	kTestFloatVectorDimension       = 512
	kTestHNSWFloatVectorMaxElements = 10000
	kTestIVFFloatVectorMaxElements  = 10000
	kTestBFloatVectorMaxElements    = 10000
	kBenchKnnNsSize                 = 30000
	kBenchKnnTxSize                 = 3000
	kBenchFloatVectorDimension      = 256
	kBenchKnnK                      = 1000
)

const (
	kTestFloatVectorArraySize            = 64
	kTestFloatVectorArrayDimension       = 64
	kTestHNSWFloatVectorArrayMaxElements = 1000
)
