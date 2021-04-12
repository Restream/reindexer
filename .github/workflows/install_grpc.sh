#!/usr/bin/env bash

git clone https://github.com/grpc/grpc
cd grpc
git submodule update --init
mkdir -p cmake/build
cd cmake/build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DgRPC_INSTALL=ON ../..
cmake --build --config RelWithDebInfo
sudo cmake --build --config RelWithDebInfo --target install
cd ../../..
