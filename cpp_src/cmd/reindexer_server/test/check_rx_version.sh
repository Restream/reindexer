#!/bin/bash

PACKAGE=$1

if [ "$PACKAGE" == "deb" ]; then
  RX_SERVER_REQUIRED_VERSION="$(basename build/reindexer-server*.deb .deb)"
  RX_SERVER_REQUIRED_VERSION=$(echo "$RX_SERVER_REQUIRED_VERSION" | cut -d'_' -f 2)
  RX_SERVER_INSTALLED_VERSION="$(dpkg -s reindexer-server | grep Version)"
  RX_SERVER_INSTALLED_VERSION="${RX_SERVER_INSTALLED_VERSION#*: }"
elif [ "$PACKAGE" == "rpm" ]; then
  RX_SERVER_REQUIRED_VERSION="$(basename build/reindexer-server*.rpm .rpm)"
  OS=$(echo ${ID} | tr '[:upper:]' '[:lower:]')
  if [ "$OS" = "redos" ]; then
    RX_SERVER_INSTALLED_VERSION="$(dnf list installed  \"reindexer-server\" | tail -n 1 | awk \'{print $$2}\')"
    echo RX_SERVER_INSTALLED_VERSION=$RX_SERVER_INSTALLED_VERSION
    echo "Installed!!!"
    dnf list installed  \"reindexer-server\"
    echo "More!!!"
    dnf list installed  \"reindexer-server\" | tail -n 1
  else
    RX_SERVER_INSTALLED_VERSION="$(rpm -q reindexer-server)"
  fi
else
  echo "Unknown package extension"
fi

if [[ $RX_SERVER_REQUIRED_VERSION == "$RX_SERVER_INSTALLED_VERSION" ]]; then
  echo "Reindexer package was upgraded"
else
  echo "Reindexer package was not upgraded" && exit 1
fi
