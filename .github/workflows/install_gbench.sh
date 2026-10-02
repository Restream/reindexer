#!/usr/bin/env bash

git clone --branch v1.9.5 https://github.com/google/benchmark.git
git clone --branch v1.17.0 https://github.com/google/googletest.git benchmark/googletest
cd benchmark
cmake -E make_directory "build"
cmake -E chdir "build" cmake -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_TESTING=OFF ../
cmake --build "build" --config Release
sudo cmake --build "build" --config Release --target install
cd ..
