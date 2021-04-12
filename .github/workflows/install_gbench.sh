#!/usr/bin/env bash

git clone https://github.com/google/benchmark.git
git clone https://github.com/google/googletest.git benchmark/googletest
cd benchmark
cmake -E make_directory "build"
cmake -E chdir "build" cmake -DCMAKE_BUILD_TYPE=Release ../
cmake --build "build" --config Release
sudo cmake --build "build" --config Release --target install
cd ..
