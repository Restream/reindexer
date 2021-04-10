#!/usr/bin/env bash

git clone https://github.com/abseil/abseil-cpp.git
cd abseil-cpp
mkdir build && cd build
cmake ..
cmake --build . --target all
cd ..
rm -rf abseil-cpp

git clone https://github.com/grpc/grpc
cd grpc
git submodule update --init
mkdir -p cmake/build
cd cmake/build
cmake -DCMAKE_BUILD_TYPE=Release -DgRPC_INSTALL=ON ../..
make -j4
sudo make install
cd ..
rm -rf grpc
