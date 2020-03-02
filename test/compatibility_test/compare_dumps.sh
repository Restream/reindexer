#!/bin/bash

if [ ! -f "$1" ] || [ ! -f "$2" ]; then
	exit -1
fi
if [ "$(head -4 $1)" != "$(head -4 $2)" ]; then
	exit -1
fi
sort -s -o "$1_sorted" "$1"
sort -s -o "$2_sorted" "$2"
cmp "$1_sorted" "$2_sorted"
exit_code=$?
rm -f "$1_sorted" "$2_sorted"
exit ${exit_code}

