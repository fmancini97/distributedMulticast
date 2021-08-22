FROM golang

WORKDIR /go/src/distributedmulticast
COPY . .


RUN go get -d -v ./...
RUN go install -v gitlab.com/f_mancini/distributedMulticast/cmd/distributedMulticast

CMD ["distributedMulticast"]

