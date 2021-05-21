#!/bin/sh

# brew install swagger2markup-cli
# npm install --save -g markdown-toc

swagger2markup convert -c swagger2markup.properties -i server.yml -f server
sed -e s'/<a name=.*><\/a\>//' -e s'/\*\*true\*\*/\*\*on\*\*/' -e 's/# Reindexer REST API/# Reindexer REST API \\n <!-- toc -->/g' \
    -e s'/^>$//' -i_ server.md && rm -f server.md_
markdown-toc -i server.md --maxdepth 3
