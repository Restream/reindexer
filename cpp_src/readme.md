# Reindexer

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.
Reindexer's goal is to provide fast search with complex queries.

# Dependencies

Reindexer's core is written in C++11 and uses LevelDB as the storage backend, so the Cmake, C++11 toolchain and LevelDB must be installed before installing Reindexer.  To build Reindexer, g++ 4.8+ or clang 3.3+ is required.  
Dependencies can be installed automatically by this script:

```bash
curl https://github.com/restream/reindexer/raw/master/dependencies.sh | bash -s
```

# Building from sources

The typical steps for building and configuring the reindexer looks like this

````bash
git clone github.com/restream/reindexer
cd reindexer/cpp_src
mkdir -p build && cd build
cmake ..
make -j4
# optional: step for build swagger documentation
make swagger
# optional: step for build web pages of Reindexer's face
make face
````
# Using reindexer server

- Start server
```
cpp_src/cmd/reindexer_server/reindexer_server
```
- open in web browser http://127.0.0.1:9088/swagger  to see reindexer REST API interactive documentation

- open in web browser http://127.0.0.1:9088/face to see reindexer web interface


# Optional dependencies

- `Doxygen` package is also required for building a documentation of the project.
- `gtest`,`gbenchmark` for run C++ tests and benchmarks
- `gperftools` for memory and performance profiling
