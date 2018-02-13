FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y g++ libsnappy-dev make libleveldb-dev curl

RUN curl -L https://github.com/Restream/reindexer/archive/master.tar.gz | tar xzv && \
    cd reindexer-master && \
    sed -i_ "s/-lev//" cpp_src/cmd/reindexer_server/Makefile && \
    sed -i_ "s/-ltcmalloc//" cpp_src/cmd/reindexer_server/Makefile && \
    make -C cpp_src/cmd/reindexer_server -j4 && \
    make -C cpp_src/cmd/reindexer_server swagger && \
    cp cpp_src/cmd/reindexer_server/reindexer_server /usr/bin && \
    mkdir -p /usr/share/reindexer && \
    cp -a cpp_src/cmd/reindexer_server/doc /usr/share/reindexer && \
    cd .. && rm -rf  reindexer-master

EXPOSE 9088
CMD reindexer_server --db /var/lib/reindexer --port 9088 --webroot /usr/share/reindexer/
