#!/usr/bin/env bash

git clone --branch release-1.12.1 https://github.com/google/googletest.git
cd googletest
mkdir build
cd build
cmake -DBUILD_GMOCK=ON -DBUILD_GTEST=ON ..
make -j4
sudo make install
cd ..
