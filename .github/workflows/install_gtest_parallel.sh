#!/usr/bin/env bash

git clone https://github.com/google/gtest-parallel
sudo cp gtest-parallel/gtest-parallel gtest-parallel/gtest_parallel.py /usr/local/bin
rm -rf gtest-parallel
sudo chmod +x /usr/local/bin/gtest-parallel
