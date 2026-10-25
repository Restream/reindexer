#!/bin/sh

# npm install --save -g openapi-to-md
# npm install --save -g markdown-toc

openapi-to-md server.yml server.md
sed -e s'/<a name=.*><\/a\>//' -e s'/\*\*true\*\*/\*\*on\*\*/' -e 's/# Reindexer REST API/# Reindexer REST API <!-- toc -->/g' \
    -e s'/^>$//' \
    -e '/\[#\/components\/schemas\/.*\](#.*)/{
s/\[#\/components\/schemas\/\(.*\)\](#\(.*\))/[\1](#\1)/
h
s/.*(#\(.*\)).*/\1/
y/ABCDEFGHIJKLMNOPQRSTUVWXYZ/abcdefghijklmnopqrstuvwxyz/
G
s/\(.*\)\n\(.*\)(#.*)\(.*\)/\2(#\1)\3/
}' \
    -e '/\[#\/components\/responses\/.*\](#.*)/{
s/\[#\/components\/responses\/\(.*\)\](#\(.*\))/[\1](#\1)/
h
s/.*(#\(.*\)).*/\1/
y/ABCDEFGHIJKLMNOPQRSTUVWXYZ/abcdefghijklmnopqrstuvwxyz/
G
s/\(.*\)\n\(.*\)(#.*)\(.*\)/\2(#\1)\3/
}' \
    -e s'/#\/components\/schemas\///g' \
    -e s'/#\/components\/responses\///g' \
    -e s'/\*\*Reindexer/## Overview\n\n\*\*\*\n\n\*\*Reindexer/' \
    -e s'/^- Description *$//' \
    -e '/### \[.*\]\/.*/{N
N
N
N
s/### \(.*\)\n\n- Summary  \n\(.*\)/### \2\n```\n\1\n```\n/
}' \
    -i_ server.md && rm -f server.md_
markdown-toc -i server.md --maxdepth 3
