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
    make -j8 reindexer_server && \
    make face swagger && \
    make install -C cpp_src/cmd/reindexer_server && \
    cd / && \
    rm -rf /src && \
    apk del g++ cmake make git curl && \
    rm -rf /var/cache/apk/* /usr/local/lib/*.a /usr/local/include/*

EXPOSE 9088 6534
CMD reindexer_server --db /db --httpaddr 0:9088 --rpcaddr 0:6534 --webroot /usr/local/share/reindexer/web
