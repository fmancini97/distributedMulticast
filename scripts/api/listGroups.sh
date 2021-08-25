#!/usr/bin/env bash

if [ $# -ne 2 ]; then
  echo "usage $0 address api-path"
  echo "example: $0 localhost:8080 /multicast/api/v1"
  exit 0
fi

curl --location --request GET "$1""$2"/groups