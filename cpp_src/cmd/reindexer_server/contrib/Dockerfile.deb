FROM debian:stable-slim

ADD . /src

RUN apt update -y && apt install -y  libunwind-dev build-essential libsnappy-dev libleveldb-dev make curl unzip git cmake libjemalloc-dev && \
    cd /src && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j8 reindexer_server reindexer_tool && \
    make face swagger && \
    make install -C cpp_src/cmd/reindexer_server && \
    make install -C cpp_src/cmd/reindexer_tool && \
    cd / && \
    rm -rf /src && \
    apt remove --purge -y  build-essential cmake make curl unzip git && \
    apt autoremove -y && \
    rm -rf /var/lib/apt /usr/lib/x86_64-linux-gnu/*.a /usr/local/lib/*.a

ENV RX_DATABASE /db
ENV RX_CORELOG stdout
ENV RX_HTTPLOG stdout
ENV RX_RPCLOG stdout
ENV RX_SERVERLOG stdout
ENV RX_LOGLEVEL info

COPY cpp_src/cmd/reindexer_server/contrib/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9088 6534
ENTRYPOINT ["/entrypoint.sh"]
CMD [""]
