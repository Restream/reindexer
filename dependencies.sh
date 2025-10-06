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

centos_openssl_msg() {
    message="The package manager of this OS does not contain the required version of the openssl library.
To use the extended authorization and authentication functionality in RX,
you can build and install openssl from source, following the instructions below:
    yum -y install perl-IPC-Cmd perl-Test-Simple && \\
    cd /usr/src  && \\
    wget https://www.openssl.org/source/openssl-3.2.1.tar.gz  && \\
    tar -zxf openssl-3.2.1.tar.gz  && \\
    rm openssl-3.2.1.tar.gz && \\
    cd /usr/src/openssl-3.2.1 && \\
    ./config no-shared no-module zlib-dynamic && \\
    make -j8 && \\
    make test && \\
    make install"

    printf "${YELLOW_BOLD}[ ATTENTION ]\n${NC}$message\n"
}

# declare dependencies arrays for systems
osx_deps="gperftools leveldb snappy cmake git libomp"
almalinux9_rpms="gcc-c++ make snappy-devel leveldb-devel gperftools-devel findutils curl tar unzip rpm-build rpmdevtools git"
fedora_rpms=" gcc-c++ make cmake snappy-devel leveldb-devel gperftools-devel findutils curl tar unzip rpm-build rpmdevtools git openblas-devel openssl-devel"
centos7_rpms="centos-release-scl devtoolset-10-gcc devtoolset-10-gcc-c++ make snappy-devel leveldb-devel gperftools-devel findutils curl tar unzip rpm-build rpmdevtools git openblas-devel"
debian_debs="build-essential g++ make cmake libunwind-dev libgoogle-perftools-dev libsnappy-dev libleveldb-dev make curl unzip git libopenblas-pthread-dev libssl-dev"
alpine_apks="g++ snappy-dev leveldb-dev libunwind-dev lapack-dev make curl cmake unzip git openssl-dev"
arch_pkgs="gcc snappy leveldb make curl cmake unzip git"
redos_rpms="gcc gcc-c++ make snappy-devel leveldb-devel gperftools-devel findutils curl tar unzip git cmake rpm-build python-srpm-macros openblas-devel openssl-devel"
altlinux_rpms="gcc gcc-c++ make libsnappy-devel libleveldb-devel libgperftools-devel curl unzip git cmake ctest rpm-build rpmdevtools libgomp-devel libopenblas-devel liblapack-devel openssl-devel"

cmake_installed () {
    info_msg "Check for installed cmake ..... "
    cmake_version=$(cmake --version  2>/dev/null | grep -oE '3\.[0-9]+\.[0-9]+')
    if [ -n "$cmake_version" ]; then
        info_msg "Cmake with comaptable version $cmake_version found"
        return
    fi
    return 1
}

install_cmake_linux () {
    info_msg "Installing 'cmake' package ....."
    case `uname -m` in
        x86_64)
            curl -L https://github.com/Kitware/CMake/releases/download/v3.31.8/cmake-3.31.8-Linux-x86_64.tar.gz 2>/dev/null | tar xzv --strip-components=1 -C /usr/local/ >/dev/null 2>&1
            ;;
        *)
            warning_msg "Fallback to system 'cmake' package. Be sure, cmake version must be at least 3.18.x"
            apt-get -y install cmake >/dev/null 2>&1
            ;;
    esac

    if [ $? -ne 0 ]; then
        error_msg "Error install 'cmake'" && return 1
    fi

    success_msg "Package 'cmake' was installed successfully." && return
}

install_osx() {
    for pkg in $osx_deps
    do
        if brew list -1 | grep -E -q "^${pkg}\$|^${pkg}@[0-9.]+\$"; then
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

install_almalinux9() {
    yum install -y epel-release >/dev/null 2>&1 || true
    sed -i 's/enabled=0/enabled=1/g' /etc/yum.repos.d/almalinux-crb.repo || true
    for pkg in ${almalinux9_rpms}
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
    cmake_installed || install_cmake_linux
    return $?
}

install_centos7() {
    yum install -y epel-release >/dev/null 2>&1 || true
    for pkg in ${centos7_rpms}
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

    centos_openssl_msg

    source scl_source enable devtoolset-10
    cmake_installed || install_cmake_linux
    return $?
}

install_fedora() {
    yum install -y epel-release >/dev/null 2>&1 || true
    for pkg in ${fedora_rpms}
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
    cmake_installed || install_cmake_linux
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
    cmake_installed || install_cmake_linux
    return $?
}

install_arch() {
    info_msg "Updating packages....."
    pacman -Sy >/dev/null 2>&1
    for pkg in ${arch_pkgs}
    do
        pacman -Q ${pkg} >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            pacman -S --noconfirm ${pkg} >/dev/null 2>&1
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
                error_msg "Could not install '$pkg' package. Try 'apk update && apk add $pkg'" && return 1
            fi
        fi
    done
    return $?
}

install_redos() {
    for pkg in ${redos_rpms}
    do
        if dnf list --installed | grep -e ^${pkg}\\. > /dev/null ; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            dnf install -y ${pkg} > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'dnf update && dnf install $pkg'" && return 1
            fi
        fi
    done
    return $?
}

install_altlinux() {
	apt-get -y update >/dev/null 2>&1
    for pkg in ${altlinux_rpms}
    do
        if rpm -qa | grep -qw ${pkg} ; then
            info_msg "Package '$pkg' already installed. Skip ....."
        else
            info_msg "Installing '$pkg' package ....."
            apt-get install -y ${pkg} > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                success_msg "Package '$pkg' was installed successfully."
            else
                error_msg "Could not install '$pkg' package. Try 'apt-get update && apt-get install $pkg'" && return 1
            fi
        fi
    done
    return $?
}



detect_installer() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        # It is "ubuntu/debian" ?
        local OS=$(echo ${ID} | tr '[:upper:]' '[:lower:]')
        if [ "$OS" = "ubuntu" -o "$OS" = "debian" -o "$OS" = "linuxmint" ]; then
            OS_TYPE="debian" && return
        elif [ "$OS" = "almalinux" -a "$(echo ${ALMALINUX_MANTISBT_PROJECT} | tr '[:upper:]' '[:lower:]')" = "almalinux-9" ]; then
            OS_TYPE="almalinux9" && return
        elif [ "$OS" = "centos" -o "$OS" = "rhel" ]; then
            if [ "$VERSION_ID" = "9" ]; then
                return 1
            elif [ "$VERSION_ID" = "8" ]; then
                OS_TYPE="centos8"
            else
                OS_TYPE="centos7"
            fi
            return
        elif [ "$OS" = "fedora" ]; then
            OS_TYPE="fedora"
            return
        elif [ "$OS" = "alpine" ]; then
            OS_TYPE="alpine" && return
        elif [ "$OS" = "arch" ]; then
            OS_TYPE="arch" && return
        elif [ "$OS" = "redos" ]; then
            OS_TYPE="redos" && return
        elif [ "$OS" = "altlinux" ]; then
            OS_TYPE="altlinux" && return
        else
            return 1
        fi
    elif [ -f /etc/centos-release ]; then
        return 1
    elif [ "$(uname)" == "Darwin" ]; then
        OS_TYPE="osx" && return
    else
        return 1
    fi
}

if [ -z "$@" ]; then
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
else
  $@
fi
