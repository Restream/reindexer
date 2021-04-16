#!/usr/bin/env bash

cd $RX_DIR
for f in reindexer*.deb; do
    sudo dpkg -i $f
    sudo apt-get install -f
done;
