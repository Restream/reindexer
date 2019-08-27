FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y build-essential g++ make cmake curl git psmisc libgoogle-perftools-dev libsnappy-dev libleveldb-dev

# Install golang and go tools
RUN curl -L https://storage.googleapis.com/golang/go1.9.2.linux-amd64.tar.gz | tar xzv -C /usr/local
ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$PATH:$GOROOT/bin:$GOPATH/bin

# Install Google Test framework
RUN git clone https://github.com/google/googletest.git && \
    cd googletest && cmake -DBUILD_GMOCK=OFF -DBUILD_GTEST=ON . && \
    make -j4 && make install && \
    cd .. && rm -rf googletest

# Install Google Benchmark framework
RUN git clone https://github.com/google/benchmark.git && \
    cd benchmark && cmake -DBENCHMARK_ENABLE_TESTING=OFF cmake -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_LTO=true . && \
    make -j4 && make install && \
    cd .. && rm -rf benchmark

RUN go get github.com/onsi/ginkgo/ginkgo && \
    go get github.com/onsi/gomega && \
    go get github.com/cheggaaa/deepcopy

ENV LANG=en_US.UTF-8
RUN go get github.com/buaazp/fasthttprouter && \
    go get github.com/valyala/fasthttp

RUN git clone https://github.com/wg/wrk.git && cd wrk && \
    make -j8 && cp wrk /usr/bin && \
    cd .. && rm -rf wrk

# Install Benchmark's golang dependencies
#########################################
RUN go get -insecure github.com/restream/reindexer && \
    go get github.com/tarantool/go-tarantool && \
    go get github.com/yunge/sphinx && \
    go get gopkg.in/mgo.v2 && \
    go get gopkg.in/olivere/elastic.v5 && \
    go get github.com/go-redis/redis && \
    go get github.com/go-sql-driver/mysql && \
    go get github.com/jmoiron/sqlx && \
    go get github.com/mailru/easyjson/...
RUN go get github.com/boltdb/bolt && \
    go get github.com/kshvakov/clickhouse && \
    go get -tags fts5 github.com/mattn/go-sqlite3 && \
    go get github.com/labstack/echo

# Build Reindexer
RUN make -C /go/src/github.com/restream/reindexer/cpp_src -j8 EXTRA_CFLAGS=-DNDEBUG

# Install elastic search
########################
RUN curl  https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add - && \
    apt-get -y install apt-transport-https && \
    echo "deb https://artifacts.elastic.co/packages/6.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-6.x.list && \
    apt-get -y update && \
    apt-get -y install elasticsearch openjdk-8-jre-headless

# Install Redis
###############
RUN apt-get install -y redis-server

# Install MongoDB
#################
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 && \
    echo "deb https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list && \
    apt-get -y update && \
    apt-get install -y mongodb-org-server

# Install MySQL
###############
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install mysql-server

# Install Tarantool
###################
RUN curl http://download.tarantool.org/tarantool/1.7/gpgkey | apt-key add - && \
    echo "deb http://download.tarantool.org/tarantool/1.7/ubuntu/ xenial main" > /etc/apt/sources.list.d/tarantool_1_7.list && \
    apt-get update && \
    apt-get -y install tarantool 

RUN echo "space = box.space.items" >>/etc/tarantool/instances.enabled/example.lua && \
    echo "if not space then">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space = box.schema.create_space('items')">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space:create_index('primary', { type = 'hash', parts = {1, 'int'} })">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space:create_index('name', { type = 'tree', unique = false, parts = {2, 'string'} })">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space:create_index('year', { type = 'tree', unique = false, parts = {3, 'int'} })">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space:create_index('name_year', { type = 'tree', unique = false, parts = {{2, 'string'},{3,'int'}} })">>/etc/tarantool/instances.enabled/example.lua && \
    echo "  space:create_index('description', { type = 'tree', unique = false, parts = {4, 'string'}} )">>/etc/tarantool/instances.enabled/example.lua && \
    echo "end">>/etc/tarantool/instances.enabled/example.lua 

# Install Sphinx search
#######################
RUN apt-get -y install sphinxsearch
RUN echo "source src1" >/etc/sphinxsearch/sphinx.conf && \
    echo "{" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  type          = mysql" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_host      = localhost" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_user      = root" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_pass      =" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_db        = test" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_port      = 3306" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  sql_query     = SELECT id, description FROM items" >>/etc/sphinxsearch/sphinx.conf && \
    echo "}" >>/etc/sphinxsearch/sphinx.conf && \
    echo "index test1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "{" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  source            = src1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  path              = /var/lib/sphinxsearch/data/test1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  docinfo           = extern" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  min_infix_len = 2" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  morphology = libstemmer_en,libstemmer_ru" >>/etc/sphinxsearch/sphinx.conf && \
    echo "}" >>/etc/sphinxsearch/sphinx.conf && \
    echo "searchd" >>/etc/sphinxsearch/sphinx.conf && \
    echo "{" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  listen            = localhost" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  read_timeout      = 5" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  max_children      = 30" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  pid_file          = /var/run/sphinxsearch/searchd.pid" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  seamless_rotate   = 1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  preopen_indexes   = 1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  unlink_old        = 1" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  binlog_path       = /var/lib/sphinxsearch/" >>/etc/sphinxsearch/sphinx.conf && \
    echo "  persistent_connections_limit = 20" >>/etc/sphinxsearch/sphinx.conf && \
    echo "}" >>/etc/sphinxsearch/sphinx.conf 

# Install Clickhouse
####################
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 && \
    echo "deb http://repo.yandex.ru/clickhouse/trusty stable main" > /etc/apt/sources.list.d/clickhouse.list && \
    echo '#!/bin/sh' >/bin/systemctl && \
    chmod +x /bin/systemctl && \
    apt-get update && \
    apt-get -y install clickhouse-client clickhouse-server-common
RUN sed -i_ "s/<listen_host>::1<\/listen_host>//" /etc/clickhouse-server/config.xml

# Install arangoDB 
RUN curl -L https://download.arangodb.com/arangodb33/xUbuntu_16.04/Release.key | apt-key add - && \
    echo 'deb https://download.arangodb.com/arangodb33/xUbuntu_16.04/ /' | tee /etc/apt/sources.list.d/arangodb.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y arangodb3=3.3.3

RUN go get github.com/arangodb/go-driver

# Install rethink DB
RUN echo "deb http://download.rethinkdb.com/apt xenial main" | tee /etc/apt/sources.list.d/rethinkdb.list && \
    curl -L https://download.rethinkdb.com/apt/pubkey.gpg | apt-key add - && \
    apt-get -y update && \
    apt-get -y install rethinkdb

RUN cp /etc/rethinkdb/default.conf.sample /etc/rethinkdb/instances.d/instance1.conf
RUN go get gopkg.in/gorethink/gorethink.v4

# Create entrypoint
###################
RUN echo "#!/bin/sh" >/entrypoint.sh&& \
    echo "/etc/init.d/elasticsearch start" >>/entrypoint.sh && \
    echo "/etc/init.d/redis-server start" >>/entrypoint.sh && \
    echo "/etc/init.d/mysql start" >>/entrypoint.sh && \
    echo "echo 'create database test CHARACTER SET utf8 ' | mysql -h127.0.0.1 -P3306 -uroot" >>/entrypoint.sh && \
    echo "mongod --config /etc/mongod.conf --fork" >>/entrypoint.sh && \
    echo "/etc/init.d/tarantool start" >>/entrypoint.sh && \
    echo "/etc/init.d/clickhouse-server start" >>/entrypoint.sh && \
    echo "/etc/init.d/rethinkdb start" >>/entrypoint.sh && \
    echo "/etc/init.d/arangodb3 start &" >>/entrypoint.sh && \
    echo "exec \"\$@\"" >>/entrypoint.sh && \
    chmod +x /entrypoint.sh 

ENTRYPOINT ["/entrypoint.sh"]

CMD ["bash"]

