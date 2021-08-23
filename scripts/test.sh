#!/usr/bin/env sh

echo testing gitlab.com/f_mancini/distributedMulticast/internal/priorityqueue
go test -v gitlab.com/f_mancini/distributedMulticast/internal/priorityqueue

echo testing gitlab.com/f_mancini/distributedMulticast/pkg/multicast
go test -v gitlab.com/f_mancini/distributedMulticast/pkg/multicast