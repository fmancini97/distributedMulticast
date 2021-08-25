#!/usr/bin/env bash

if [ $# -ne 3 ]; then
  echo "usage $0 address api-path multicast-id"
  echo "example: $0 localhost:8080 /multicast/api/v1 test-multicast"
  exit 0
fi

curl --location --request DELETE "$1""$2"/groups/"$3"
