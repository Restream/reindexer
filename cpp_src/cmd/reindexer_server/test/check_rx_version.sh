#!/bin/bash

PACKAGE=$1

if [ "$PACKAGE" == "deb" ]; then
  RX_SERVER_REQUIRED_VERSION="$(basename build/reindexer-server*.deb .deb)"
  RX_SERVER_REQUIRED_VERSION=$(echo "$RX_SERVER_REQUIRED_VERSION" | cut -d'_' -f 2)
  RX_SERVER_INSTALLED_VERSION="$(dpkg -s reindexer-server | grep Version)"
  RX_SERVER_INSTALLED_VERSION="${RX_SERVER_INSTALLED_VERSION#*: }"
elif [ "$PACKAGE" == "rpm" ]; then
  RX_SERVER_REQUIRED_VERSION="$(basename build/reindexer-server*.rpm .rpm)"
  RX_SERVER_INSTALLED_VERSION="$(rpm -q reindexer-server)"
else
  echo "Unknown package extension"
fi

if [[ $RX_SERVER_REQUIRED_VERSION == "$RX_SERVER_INSTALLED_VERSION" ]]; then
  echo "Reindexer package was upgraded"
else
  echo "Reindexer package was not upgraded" && exit 1
fi