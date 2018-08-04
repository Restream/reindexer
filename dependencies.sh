#!/bin/sh

# NO COLOR
NC='\033[0m'

# COLORS
RED='\033[0;31m'
GREEN='\033[0;32m'
LBLUE='\033[01;34m'

# BOLD COLORS
RED_BOLD='\033[0;31;1m'
GREEN_BOLD='\033[0;32;1m'
LBLUE_BOLD='\033[01;34;1m'
YELLOW_BOLD='\033[0;33;1m'

success_msg() {
	printf "${GREEN_BOLD}[ OK ] ${NC}$1\n"
}

info_msg() {
    printf "${LBLUE_BOLD}[ INFO ] ${NC}$1\n"
}

warning_msg() {
    printf "${YELLOW_BOLD}[ WARNING ] ${NC}$1\n"
}

error_msg() {
    printf "${RED_BOLD}[ ERROR ] ${NC}$1\n"
}

# declare dependencies arrays for systems
osx_deps="gperftools leveldb snappy cmake git"
centos7_debs="gcc-c++ cmake make snappy-devel gperftools-devel findutils curl tar unzip rpm-build rpmdevtools git"
centos6_debs="centos-release-scl devtoolset-7-gcc devtoolset-7-gcc-c++ make snappy-devel gperftools-devel findutils curl tar cmake unzip rpm-build git"
debian_debs="build-essential g++ libgoogle-perftools-dev libsnappy-dev libleveldb-dev make curl cmake unzip git"
alpine_apks="g++ snappy-dev libexecinfo-dev make curl cmake unzip git"

install_leveldb() {
    info_msg "Installing 'leveldb' package ....."
    curl -L -s https://github.com/google/leveldb/archive/v1.20.tar.gz | tar xz -C /tmp
    if [ $? -ne 0 ]; then
        error_msg "Could not download 'leveldb'." && return 1
    fi

    cd /tmp/leveldb-1.20 && make -j4 > /dev/null 2>&1 && mv out-static/libleveldb.* /usr/local/lib
    if [ $? -ne 0 ]; then
        error_msg "Could not install 'leveldb' libraries." && return 1
    fi

    mkdir -p /usr/local/include
    cd include && cp -R leveldb /usr/local/include  >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        error_msg "Could not copy 'leveldb' header files." && return 1
    fi

    ldconfig

    # delete temporary files
    rm -rf /tmp/leveldb-1.20

    success_msg "Package 'leveldb' was installed successfully." && return
}

install_osx() {
    for pkg in $osx_deps
    do
        if brew list -1 | grep -q "^${pkg}\$"; then
            info_msg "Package $pkg already installed. Skip ....."
        else
            info_msg "Installing $pkg package ....."
            brew install ${pkg} 2>&1 | grep -i -E "error|warning" | tr '[:upper:]' '[:lower:]' >/tmp/.status
            IFS=":" read STATUS MESSAGE < /tmp/.status
            if [ -n "$STATUS" ]; then
                print_result="${STATUS}_msg \"$MESSAGE\""
                eval "${print_result}"
                return 1
            else
                success_msg "Package '$pkg' was installed successfully."
            fi
        fi
    done
    return
}

install_centos7() {
    yum install -y epel-release >/dev/null 2>&1 || true
    for pkg in ${centos7_debs}
    do
        if rpm -qa | grep -qw ${pkg} ; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            yum install -y ${pkg} > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'yum update && yum install $pkg'" && return 1
            fi
        fi
    done
    install_leveldb
    return $?
}

install_centos6() {
    yum install -y epel-release >/dev/null 2>&1 || true
    for pkg in ${centos6_debs}
    do
        if rpm -qa | grep -qw ${pkg}; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            yum install -y ${pkg}  >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'yum update && yum install $pkg'" && return 1
            fi
        fi
    done
    source scl_source enable devtoolset-7
    install_leveldb
    return $?
}

install_debian() {
    info_msg "Updating packages....."
    apt-get -y update >/dev/null 2>&1
    for pkg in ${debian_debs}
    do
        dpkg -s ${pkg} >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            apt-get install -y ${pkg} >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'apt-get update && apt-get install $pkg'" && return 1
            fi
        fi
    done
    return $?
}

install_alpine() {
    info_msg "Updating packages....."
    apk update >/dev/null 2>&1
    for pkg in ${alpine_apks}
    do
        local info=`apk info | grep ${pkg}`
        if [ _"$info" != _"" ]; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            apk add ${pkg} >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'apt-get update && apt-get install $pkg'" && return 1
            fi
        fi
    done
    install_leveldb
    return $?
}

detect_installer() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        # It is "ubuntu/debian" ?
        local OS=$(echo ${ID} | tr '[:upper:]' '[:lower:]')
        if [ "$OS" = "ubuntu" -o "$OS" = "debian" -o "$OS" = "linuxmint" ]; then
            OS_TYPE="debian" && return
        elif [ "$OS" = "centos" -o "$OS" = "fedora" -o "$OS" = "rhel" ]; then
            OS_TYPE="centos7" && return
        elif [ "$OS" = "alpine" ]; then
            OS_TYPE="alpine" && return
        else
            return 1
        fi
    elif [ -f /etc/centos-release ]; then
        if [ $(cat /etc/centos-release | cut -d" " -f3 | cut -d "." -f1) -eq 6 ]; then
            OS_TYPE="centos6" && return
        else
            return 1
        fi
    elif [ "$(uname)" == "Darwin" ]; then
        OS_TYPE="osx" && return
    else
        return 1
    fi
}

detect_installer
if [ $? -eq 0 ]; then
    INSTALL="install_${OS_TYPE}"
    eval "$INSTALL"
    if [ $? -eq 0 ]; then
        success_msg "All dependencies installed."; exit 0
    else
        error_msg "Dependencies installation was failed."; exit 1
    fi
else
    error_msg "Unsupported OS type."
fi