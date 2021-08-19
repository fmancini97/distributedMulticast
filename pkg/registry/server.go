package registry

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/registry/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

var (
	groups      map[string]*registrygrpc.GroupInfo
	groupsMu    sync.RWMutex
	registryLog *log.Entry
)

func init() {
	groups = make(map[string]*registrygrpc.GroupInfo)
	registryLog = registryLog.WithField("app", "registry")
}

type server struct {
	registrygrpc.UnimplementedRegistryServer
}

func (s *server) Register(ctx context.Context, in *registrygrpc.RegistrationInfo) (*registrygrpc.GroupInfo, error) {

	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	srcAddr := src[:strings.IndexByte(src, ':')]

	srcAddr = fmt.Sprintf("%s:%d", srcAddr, in.ClientPort)

	multicastId := in.GroupId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId})
	logger.WithField("registration_info", in).Debug("New registration")

	groupsMu.Lock()
	defer groupsMu.Unlock()

	group, exists := groups[multicastId]

	if exists {
		if group.MulticastType != in.MulticastType {
			return group, status.Errorf(codes.InvalidArgument, "MulticastType")
		}

		if group.Status != registrygrpc.Status_ACTIVE {
			return group, status.Errorf(codes.InvalidArgument, "Status")
		}

		if _, ok := group.Members[srcAddr]; ok {
			return group, status.Errorf(codes.AlreadyExists, "Already Registered")
		}

	} else {

		group = &registrygrpc.GroupInfo{
			GroupId:       multicastId,
			MulticastType: in.MulticastType,
			Status:        registrygrpc.Status_STARTING,
			Members:       make(map[string]*registrygrpc.MemberInfo),
		}

		groups[multicastId] = group
	}

	memberInfo := new(registrygrpc.MemberInfo)
	memberInfo.Id = srcAddr
	memberInfo.Address = srcAddr

	group.Members[srcAddr] = memberInfo

	return group, nil
}

func (s *server) GetStatus(ctx context.Context, in *registrygrpc.Group) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.GroupId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId})
	logger.WithField("group", in).Debug("Retrieving status")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := groups[in.GroupId]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	return group, nil

}

func (s *server) startGroup(ctx context.Context, in *registrygrpc.Group) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.GroupId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId})
	logger.WithField("group", in).Debug("Retrieving status")

	groupsMu.Lock()
	defer groupsMu.Unlock()

	group, ok := groups[in.GroupId]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	if group.Status == registrygrpc.Status_CLOSED {
		return nil, status.Errorf(codes.Canceled, "Group closed")
	}

	group.Status = registrygrpc.Status_ACTIVE

	return group, nil

}
