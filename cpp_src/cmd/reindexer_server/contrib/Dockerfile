FROM alpine:latest

RUN mkdir /src && \
    cd /src && \
    apk update && \
    apk add git curl autoconf automake libtool linux-headers g++ make libunwind-dev && \ 
    git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && \
    echo "noinst_PROGRAMS =" >> Makefile.am && \
    ./autogen.sh &&  ./configure && make -j8 && make install && \
    cd .. && \
    git clone https://github.com/restream/reindexer && \
    cd reindexer && \
    git checkout develop && \
    ./dependencies.sh && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j8 reindexer_server reindexer_tool && \
    make face swagger && \
    make install -C cpp_src/cmd/reindexer_server && \
    make install -C cpp_src/cmd/reindexer_tool && \
    cd / && \
    rm -rf /src && \
    apk del g++ cmake make git curl autoconf automake libtool linux-headers && \
    rm -rf /var/cache/apk/* /usr/local/lib/*.a /usr/local/include/* /usr/local/lib/libtcmalloc_debug* /usr/local/lib/libtcmalloc_minimal* \
    /usr/local/lib/libprofiler* /usr/local/lib/libtcmalloc.*

ENV RX_DATABASE /db
ENV RX_CORELOG stdout
ENV RX_HTTPLOG stdout
ENV RX_RPCLOG stdout
ENV RX_SERVERLOG stdout
ENV RX_LOGLEVEL info

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9088 6534
ENTRYPOINT ["/entrypoint.sh"]
CMD [""]
