#!/usr/bin/env bash

if [ $# -ne 4 ]; then
  echo "usage $0 address api-path multicast-id message_pos"
  echo "example: $0 localhost:8080 /multicast/api/v1 test-multicast 5"
  exit 0
fi

curl --location --request GET "$1""$2"/messages/"$3"/"$4"