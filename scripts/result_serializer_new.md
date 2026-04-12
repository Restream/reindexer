# Serializer benchmark (new)

Command:
```bash
go test ./cjson -bench BenchmarkSerializer -benchmem -run ^$
```

Raw output:
```
goos: linux
goarch: amd64
pkg: github.com/restream/reindexer/v5/cjson
cpu: Intel(R) Core(TM) i5-10400 CPU @ 2.90GHz
BenchmarkSerializerEncodeTypicalDocument-12    	 3388335	       329.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkSerializerWriteIntsBatch-12           	 1843077	       653.0 ns/op	50183.78 MB/s	       0 B/op	       0 allocs/op
BenchmarkSerializerWriteInts16Batch-12         	 4841936	       259.3 ns/op	63181.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkSerializerPutFloatVectorBatch-12      	  744631	      1499 ns/op	4098.37 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/restream/reindexer/v5/cjson	5.985s
```
