#!/usr/bin/env bash

git clone --branch release-1.10.0 https://github.com/google/googletest.git
cd googletest
mkdir build
cd build
cmake ..
make -j4
sudo make install
cd ..
