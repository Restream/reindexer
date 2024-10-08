FROM alpine:3.20 AS build

RUN cd /tmp && apk update && \
    apk add git curl autoconf automake libtool linux-headers g++ make libunwind-dev grpc-dev protobuf-dev c-ares-dev patch && \
    git clone https://github.com/gperftools/gperftools.git && \
    cd gperftools && git checkout gperftools-2.15 && \
    sed -i s/_sigev_un\._tid/sigev_notify_thread_id/ src/profile-handler.cc && \
    ./autogen.sh && ./configure --disable-dependency-tracking && make -j6 && make install

ADD . /src

WORKDIR /src

RUN ./dependencies.sh && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. -DENABLE_GRPC=On -DGRPC_PACKAGE_PROVIDER="" && \
    make -j6 reindexer_server reindexer_tool && \
    make install -C cpp_src/cmd/reindexer_server && \
    make install -C cpp_src/cmd/reindexer_tool && \
    make install -C cpp_src/server/grpc && \
    cp ../cpp_src/cmd/reindexer_server/contrib/entrypoint.sh /entrypoint.sh && \
    rm -rf /usr/local/lib/*.a /usr/local/include /usr/local/lib/libtcmalloc_debug* /usr/local/lib/libtcmalloc_minimal* \
    /usr/local/lib/libprofiler* /usr/local/lib/libtcmalloc.* /usr/local/share/doc /usr/local/share/man /usr/local/share/perl5 /usr/local/bin/pprof*

FROM alpine:3.20

COPY --from=build /usr/local /usr/local
COPY --from=build /entrypoint.sh /entrypoint.sh
RUN apk update && apk add libstdc++ libunwind snappy leveldb c-ares libprotobuf xz-libs grpc-cpp && rm -rf /var/cache/apk/*

ENV RX_DATABASE=/db
ENV RX_CORELOG=stdout
ENV RX_HTTPLOG=stdout
ENV RX_RPCLOG=stdout
ENV RX_SERVERLOG=stdout
ENV RX_LOGLEVEL=info

RUN chmod +x /entrypoint.sh

EXPOSE 9088 6534 16534
ENTRYPOINT ["/entrypoint.sh"]
CMD [""]
