#!/usr/bin/env bash

git clone --branch v1.38.0 https://github.com/grpc/grpc
cd grpc
git submodule update --init
mkdir -p cmake/build
cd cmake/build
cmake -DCMAKE_BUILD_TYPE=Release DgRPC_INSTALL=ON ../..
make -j4
sudo make install
sudo ldconfig
cd ../../..
