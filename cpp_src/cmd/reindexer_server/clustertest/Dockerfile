FROM ubuntu:20.04 AS build

RUN cd /tmp && \
    apt-get update && \
    apt-get install -y git curl autoconf automake libtool g++ make libunwind-dev libunwind-dev libprotobuf-dev libc-ares-dev && \ 
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
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
    make -j8 reindexer_server reindexer_tool && \
    make install -C cpp_src/cmd/reindexer_server && \
    make install -C cpp_src/cmd/reindexer_tool && \
    cp ../cpp_src/cmd/reindexer_server/clustertest/entrypoint.sh /entrypoint.sh && \
    rm -rf /usr/local/lib/*.a /usr/local/include /usr/local/lib/libtcmalloc_debug* /usr/local/lib/libtcmalloc_minimal* \
    /usr/local/lib/libprofiler* /usr/local/lib/libtcmalloc.* /usr/local/share/doc /usr/local/share/man /usr/local/share/perl5  /usr/local/bin/pprof*

FROM ubuntu:20.04

COPY --from=build /usr/local /usr/local
COPY --from=build /entrypoint.sh /entrypoint.sh
RUN apt-get update && apt-get install -y libstdc++-9-dev libsnappy-dev libunwind-dev libleveldb-dev libc-ares-dev libprotobuf-dev xz-utils && rm -rf /var/cache/apt/*

ENV RX_DATABASE /db
ENV RX_CORELOG stdout
ENV RX_HTTPLOG stdout
ENV RX_RPCLOG stdout
ENV RX_SERVERLOG stdout
ENV RX_LOGLEVEL info

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD [""]
