FROM alpine:latest

RUN mkdir /src && \
    cd /src && \
    apk update && \
    apk add git curl && \ 
    git clone https://github.com/restream/reindexer && \
    cd reindexer && \
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
    apk del g++ cmake make git curl && \
    rm -rf /var/cache/apk/* /usr/local/lib/*.a /usr/local/include/*

ENV RX_DATABASE /db
ENV RX_CORELOG stdout
ENV RX_HTTPLOG stdout
ENV RX_RPCLOG stdout
ENV RX_SERVERLOG stdout
ENV RX_LOGLEVEL info

EXPOSE 9088 6534
CMD ["sh", "-c", "reindexer_server --db $RX_DATABASE --httpaddr 0:9088 --rpcaddr 0:6534 --webroot /usr/local/share/reindexer/web --corelog $RX_CORELOG --serverlog $RX_SERVERLOG --httplog $RX_HTTPLOG --rpclog $RX_RPCLOG --loglevel $RX_LOGLEVEL"]
