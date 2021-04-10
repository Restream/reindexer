#!/usr/bin/env bash

pwd
#cd ..
git clone https://github.com/grpc/grpc
cd grpc
git submodule update --init
mkdir -p cmake/build
cd cmake/build
cmake -D CMAKE_CXX_FLAGS="-lrt" -DCMAKE_BUILD_TYPE=Release DgRPC_INSTALL=ON ../..
make -j4
make install
cd ..
rm -rf grpc
