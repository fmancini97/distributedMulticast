#!/usr/bin/env bash

if [ $# -ne 4 ]; then
  echo "usage $0 address api_path multicast_id payload"
  echo "example: $0 localhost:8080 /multicast/api/v1 test-multicast testMessage"
  exit 0
fi

payload=$(echo -n "$4" | base64)

curl --location --request PUT "$1""$2"/messages/"$3" \
--header 'Content-Type: application/json' \
--data-raw "{
    \"payload\": \"$payload\"
}"
