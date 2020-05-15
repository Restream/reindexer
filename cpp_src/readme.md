# Reindexer

**Reindexer** is an embeddable, in-memory, document-oriented database with a high-level Query builder interface.
Reindexer's goal is to provide fast search with complex queries.

The Reindexer is compact and fast. It has not heavy dependencies. Complete reindexer docker image with all libraries and web interface size is just 15MB.
Reindexer is fast. Up to 5x times faster, than mongodb, and 10x times than elastic search. See [benchmaks section](../benchmarks) for details.

# Installation

## Docker image

The simplest way to get reindexer, is pulling & run docker image from [dockerhub](https://hub.docker.com/r/reindexer/reindexer/)

````bash
docker run -p9088:9088 -p6534:6534 -it reindexer/reindexer
````

## OSX brew

````bash
brew tap restream/reindexer
brew install reindexer
````

## Linux

## RHEL/Centos/Fefora
````bash
yum install -y epel-release yum-utils
rpm --import http://repo.reindexer.org/RX-KEY.GPG
yum-config-manager --add-repo  http://repo.reindexer.org/<distro>/x86_64/
yum update
yum install reindexer-server
````

Available distros: `centos-6`,`centos-7`,`centos-8`,`fedora-30`,`fedora-31`,


## Ubuntu/Debian

````bash
curl http://repo.reindexer.org/RX-KEY.GPG | apt-key add
echo "deb http://repo.reindexer.org/<distro> /" >> /etc/apt/sources.list
apt update
apt install reindexer-server
````

Available distros: `debian-buster`, `debian-stretch`, `ubuntu-bionic`, `ubuntu-xenial`, `ubuntu-focal`


## Windows

Download and install [64 bit](http://www.reindexer.org/dist/reindexer_server-win64.exe) or [32 bit](http://www.reindexer.org/dist/reindexer_server-win32.exe) 

## Installing from sources

### Dependencies

Reindexer's core is written in C++11 and uses LevelDB as the storage backend, so the Cmake, C++11 toolchain and LevelDB must be installed before installing Reindexer.  To build Reindexer, g++ 4.8+, clang 3.3+ or MSVC 2015+ is required.  
Dependencies can be installed automatically by this script:

```bash
curl -L https://github.com/Restream/reindexer/raw/master/dependencies.sh | bash -s
```

### Build & install

The typical steps for building and configuring the reindexer looks like this

````bash
git clone https://github.com/Restream/reindexer
cd reindexer
mkdir -p build && cd build
cmake ..
make -j4
# install to system
sudo make install
````

## Using reindexer server

- Start server
```
service start reindexer
```
- open in web browser http://127.0.0.1:9088/swagger  to see reindexer REST API interactive documentation

- open in web browser http://127.0.0.1:9088/face to see reindexer web interface


# Optional dependencies

- `Doxygen` package is also required for building a documentation of the project.
- `gtest`,`gbenchmark` for run C++ tests and benchmarks
- `gperftools` for memory and performance profiling
