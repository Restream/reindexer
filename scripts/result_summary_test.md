# Serializer benchmark summary

## Compared
- New: current serializer implementation
- Old: baseline serializer implementation

## Table (lower ns/op is better)

| Benchmark | New ns/op | Old ns/op | Old/New speedup |
|---|---:|---:|---:|
| BenchmarkSerializerEncodeTypicalDocument-12 | 329,0 | 1480,0 | 4,50x |
| BenchmarkSerializerPutFloatVectorBatch-12 | 1499,0 | 8199,0 | 5,47x |
| BenchmarkSerializerWriteInts16Batch-12 | 259,0 | 6916,0 | 26,70x |
| BenchmarkSerializerWriteIntsBatch-12 | 653,0 | 14707,0 | 22,52x |
