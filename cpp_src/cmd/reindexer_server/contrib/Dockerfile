FROM alpine:latest AS build

RUN cd /tmp && \
    apk update && \
    apk add git curl autoconf automake libtool linux-headers g++ make libunwind-dev && \ 
    git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && \
    echo "noinst_PROGRAMS =" >> Makefile.am && \
    ./autogen.sh &&  ./configure && make -j8 && make install && \
    cd .. 
ADD . /src

WORKDIR /src
RUN ./dependencies.sh && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j8 reindexer_server reindexer_tool && \
    make face swagger && \
    make install -C cpp_src/cmd/reindexer_server && \
    make install -C cpp_src/cmd/reindexer_tool && \
    cp ../cpp_src/cmd/reindexer_server/contrib/entrypoint.sh /entrypoint.sh && \
    rm -rf /usr/local/lib/*.a /usr/local/include /usr/local/lib/libtcmalloc_debug* /usr/local/lib/libtcmalloc_minimal* \
    /usr/local/lib/libprofiler* /usr/local/lib/libtcmalloc.* /usr/local/share/doc /usr/local/share/man /usr/local/share/perl5  /usr/local/bin/pprof*

FROM alpine:latest

COPY --from=build /usr/local /usr/local
COPY --from=build /entrypoint.sh /entrypoint.sh
RUN apk update && apk add libstdc++ libunwind snappy libexecinfo leveldb && rm -rf /var/cache/apk/*

ENV RX_DATABASE /db
ENV RX_CORELOG stdout
ENV RX_HTTPLOG stdout
ENV RX_RPCLOG stdout
ENV RX_SERVERLOG stdout
ENV RX_LOGLEVEL info

RUN chmod +x /entrypoint.sh

EXPOSE 9088 6534
ENTRYPOINT ["/entrypoint.sh"]
CMD [""]
