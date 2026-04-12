# Serializer benchmark (old)

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
BenchmarkSerializerEncodeTypicalDocument-12    	  765309	      1480 ns/op	       0 B/op	       0 allocs/op
BenchmarkSerializerWriteIntsBatch-12           	   80754	     14707 ns/op	2228.02 MB/s	       0 B/op	       0 allocs/op
BenchmarkSerializerWriteInts16Batch-12         	  172694	      6916 ns/op	2368.85 MB/s	       0 B/op	       0 allocs/op
BenchmarkSerializerPutFloatVectorBatch-12      	  142226	      8199 ns/op	 749.40 MB/s	       0 B/op	       0 allocs/op
PASS
ok  	github.com/restream/reindexer/v5/cjson	5.013s
```
