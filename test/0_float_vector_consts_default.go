//go:build !tiny_vectors

package reindexer

const kTestFloatVectorDimension       = 512
const kTestHNSWFloatVectorMaxElements = 10000
const kTestIVFFloatVectorMaxElements  = 10000
const kTestBFloatVectorMaxElements    = 10000
const kBenchKnnNsSize                 = 30000
const kBenchKnnTxSize                 = 3000
const kBenchFloatVectorDimension      = 256
const kBenchKnnK                      = 1000
