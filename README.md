# Distributed Multicast

Distributed application to multicast messages between different nodes. The application allows you to instantiate 
multiple concurrent multicast groups and set different requirements on the message delivery order. A registry server 
manages the creation of multicast groups and the discovery of available ones. Each node exposes an HTTP Rest interface 
to access the services offered by the application.

Available multicast algorithms are:

- ```BMULTICAST``` Basic multicast implementation. Does not offer any guarantee on message delivery order.
- ```FIFOMULTICAST``` Fifo ordering multicast. Ensures that messages sent by one node are delivered to others 
  in the same order.
- ```TOMULTICASTSEQ``` Total ordering multicast. All messages are delivered in the same order to all nodes. 
  Centralized implementation using a sequencer.
- ```TOMULTICASTSC``` Total ordering multicast. All messages are delivered in the same order to all nodes.
  Decentralized implementation using scalar timestamps.
- ```COMULTICAST``` Causal ordering multicast. Messages are delivered by maintaining cause-and-effect relationships. 
  Furthermore, it guarantees Fifo ordering. Decentralized implementation using vector timestamps.
- ```TOCOMULTICAST``` Total and Causal ordering multicast. Messages are delivered in the same order to all nodes and 
  by maintaining cause-and-effect relationships.
  

## Requirements
- [Golang v1.16](https://golang.org/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

## Deployment
### Local 
Running the application locally using the following commands:
```shell
go run gitlab.com/f_mancini/distributedMulticast/cmd/distributedMulticast
```

The application can be configured using the following flags:
- ```-appMulticast``` start distributed multicast node
- ```-registryMulticast``` start multicast registry
- ```-debugMode``` execute distributed multicast node in debug mode
- ```-delay uint``` delay for sending operations (ms)
- ```-grpcPort uint``` port number of the grpc server (default 90)
- ```-logLevel string``` log level: ```panic```, ```fatal```, ```error```, ```warning```, ```info```, ```debug```, 
  ```trace``` (default "info")
- ```-numThreads uint``` number of threads used to multicast messages (default 1)
- ```-registryAddr string``` multicast registry address (default ":90")
- ```-restPath string``` path of the rest api (default "/multicast/api/v1")
- ```-restPort uint``` port number of the rest server (default 80)

### Docker
The application can be run in a docker container using the dedicated [Dockerfile](Dockerfile). 
```shell
docker build . -t fmancini/distributedMulticast
```

[docker-compose.yml](docker-compose.yml) file deploys an application demo.
```shell
docker-compose up
```

The container can be configured through the following environment variables:
- ```APP=``` start distributed multicast node
- ```REGISTRY=``` start multicast registry
- ```DEBUG=``` execute distributed multicast node in debug mode
- ```DELAY=uint``` delay for sending operations (ms)
- ```GRPC_PORT=uint``` port number of the grpc server (default 90)
- ```LOG_LEVEL=string``` log level: ```panic```, ```fatal```, ```error```, ```warning```, ```info```, ```debug```,
  ```trace``` (default "info")
- ```NUM_THREADS=uint``` number of threads used to multicast messages (default 1)
- ```REGISTRY_ADDR=string``` multicast registry address (default ":90")
- ```REST_PATH=string``` path of the rest api (default "/multicast/api/v1")
- ```REST_PORT=uint``` port number of the rest server (default 80)

## REST API

### API Resources
  - [GET /registry/groups](#get-registrygroups)
  - [GET /groups](#get-groups)
  - [GET /groups/[multicastId]](#get-groupsmulticastid)
  - [PUT /groups/[multicastId]](#put-groupsmulticastid)
  - [POST /groups/[multicastId]](#post-groupsmulticastid)
  - [DELETE /groups/[multicastId]](#delete-groupsmulticastid)
  - [GET /messages/[multicastId]](#get-messagesmulticastid)
  - [PUT /messages/[multicastId]](#put-messagesmulticastid)
  - [GET /messages/[multicastId]/[messagePos]](#get-messagesmulticastidmessagepos)


### GET /registry/groups
It returns all groups in the registry. 

### GET /groups
Returns the groups the node is joining.

Response body:
```json
[
  {
    "multicast_id": "test",
    "multicast_type": "TOCOMULTICAST",
    "received_messages": 772,
    "status": "ACTIVE",
    "members": {
        "127.0.0.1:9090": {
            "member_id": "127.0.0.1:9090",
            "address": "127.0.0.1:9090",
            "ready": true
        },
        "127.0.0.1:9091": {
            "member_id": "127.0.0.1:9091",
            "address": "127.0.0.1:9091",
            "ready": true
        },
        "127.0.0.1:9092": {
            "member_id": "127.0.0.1:9092",
            "address": "127.0.0.1:9092",
            "ready": true
        }
    },
    "error": {}
  }
]
```

### GET /groups/[multicastId]
It returns the information of the group identified by ```multicastId```.

Response body:
```json
{
  "multicast_id": "test",
  "multicast_type": "TOCOMULTICAST",
  "received_messages": 3,
  "status": "ACTIVE",
  "members": {
    "127.0.0.1:9090": {
      "member_id": "127.0.0.1:9090",
      "address": "127.0.0.1:9090",
      "ready": true
    },
    "127.0.0.1:9091": {
      "member_id": "127.0.0.1:9091",
      "address": "127.0.0.1:9091",
      "ready": true
    },
    "127.0.0.1:9092": {
      "member_id": "127.0.0.1:9092",
      "address": "127.0.0.1:9092",
      "ready": true
    }
  },
  "error": {}
}
```

### PUT /groups/[multicastId]
It initializes a new multicast group. The operation fails if multicast communication has started in the group or if the 
required multicast type does not match the one set in the group

Request body:
```json
{
  "multicast_type": "TOCOMULTICAST" 
}
```

### POST /groups/[multicastId]
It starts communication in the multicast group. 

Request body is empty

### DELETE /groups/[multicastId]
It interrupts communication in the multicast group of the node. A closed group can no longer be used to multicast messages but
received messages can still be read.

Request body is empty

### GET /messages/[multicastId]
It returns all messages received by a multicast group of the node

Response body:
```json
[
  {
    "metadata": {
      "seq-multicast:messageId": "127.0.0.1:9090-1",
      "seq-multicast:messageType": "seqData",
      "seq-multicast:seqOrder": "1",
      "vc-multicast:clientId": "127.0.0.1:9090",
      "vc-multicast:timestamp": "Dv+BBAEC/4IAAQwBBgAANP+CAAMOMTI3LjAuMC4xOjkwOTIADjEyNy4wLjAuMTo5MDkxAA4xMjcuMC4wLjE6OTA5MAE="
    },
    "payload": "ZGVhZGJlZWY="
  },
  {
    "metadata": {
      "seq-multicast:messageId": "127.0.0.1:9092-1",
      "seq-multicast:messageType": "seqData",
      "seq-multicast:seqOrder": "2",
      "vc-multicast:clientId": "127.0.0.1:9092",
      "vc-multicast:timestamp": "Dv+BBAEC/4IAAQwBBgAANP+CAAMOMTI3LjAuMC4xOjkwOTIBDjEyNy4wLjAuMTo5MDkwAg4xMjcuMC4wLjE6OTA5MQA="
    },
    "payload": "ZGVhZGJlZWY="
  },
  {
    "metadata": {
      "seq-multicast:messageId": "127.0.0.1:9091-1",
      "seq-multicast:messageType": "seqData",
      "seq-multicast:seqOrder": "3",
      "vc-multicast:clientId": "127.0.0.1:9091",
      "vc-multicast:timestamp": "Dv+BBAEC/4IAAQwBBgAANP+CAAMOMTI3LjAuMC4xOjkwOTADDjEyNy4wLjAuMTo5MDkyAQ4xMjcuMC4wLjE6OTA5MQE="
    },
    "payload": "ZGVhZGJlZWY="
  }
]
```

### PUT /messages/[multicastId]
It sends a message in an active multicast group of the node. 

The metadata field can contain pairs of strings. The payload field must be a base64 encoded string.

Request body:
```json
{
  "metadata": {
    "field1": "value1",
    "field2": "value2"
  },
  "payload": "ZGVhZGJlZWY=" 
}
```
### GET /messages/[multicastId]/[messagePos]
It returns the message at messagePos position of a multicast group in the node

Response body:
```json
{
  "metadata": {
    "seq-multicast:messageId": "127.0.0.1:9092-1",
    "seq-multicast:messageType": "seqData",
    "seq-multicast:seqOrder": "2",
    "vc-multicast:clientId": "127.0.0.1:9092",
    "vc-multicast:timestamp": "Dv+BBAEC/4IAAQwBBgAANP+CAAMOMTI3LjAuMC4xOjkwOTIBDjEyNy4wLjAuMTo5MDkwAg4xMjcuMC4wLjE6OTA5MQA="
  },
  "payload": "ZGVhZGJlZWY="
}
```

## Test

Tests can be run using the following command:
```shell
sh ./scripts/test.sh
```