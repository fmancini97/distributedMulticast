#!/usr/bin/env bash

if [ $# -ne 4 ]; then
  echo "usage $0 address api_path multicast_id multicast_type"
  echo "example: $0 localhost:8080 /multicast/api/v1 test-multicast TOCOMULTICAST"
  exit 0
fi

curl --location --request PUT "$1""$2"/groups/"$3" \
--header 'Content-Type: application/json' \
--data-raw "{
    \"multicast_type\": \"$4\"
}"
