package multicastregistry

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

var (
	groups      map[string]*group
	groupsMu    sync.RWMutex
	registryLog *log.Entry
)

func init() {
	groups = make(map[string]*group)
	registryLog = log.WithField("app", "registry")
}

type server struct {
	registrygrpc.UnimplementedRegistryServer
}

type group struct {
	mu        sync.RWMutex
	groupInfo *registrygrpc.GroupInfo
}

func (s *server) Register(ctx context.Context, in *registrygrpc.RegistrationInfo) (*registrygrpc.RegistrationAnswer, error) {

	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	srcAddr := src[:strings.LastIndexByte(src, ':')]

	srcAddr = fmt.Sprintf("%s:%d", srcAddr, in.ClientPort)

	multicastId := in.GroupId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId})
	logger.WithField("registration_info", in).Debug("New registration")

	groupsMu.Lock()
	defer groupsMu.Unlock()

	multicastGroup, exists := groups[multicastId]

	if exists {
		if multicastGroup.groupInfo.MulticastType != in.MulticastType {
			return nil, status.Errorf(codes.InvalidArgument, "MulticastType")
		}

		if multicastGroup.groupInfo.Status != registrygrpc.Status_OPENING {
			return nil, status.Errorf(codes.PermissionDenied, "ServiceStarted")
		}

		if _, ok := multicastGroup.groupInfo.Members[srcAddr]; ok {
			return nil, status.Errorf(codes.AlreadyExists, "Already Registered")
		}

	} else {

		multicastGroup = &group{groupInfo: &registrygrpc.GroupInfo{
			GroupId:       multicastId,
			MulticastType: in.MulticastType,
			Status:        registrygrpc.Status_OPENING,
			Members:       make(map[string]*registrygrpc.MemberInfo),
		}}

		groups[multicastId] = multicastGroup
	}

	memberInfo := new(registrygrpc.MemberInfo)
	memberInfo.Id = srcAddr
	memberInfo.Address = srcAddr
	memberInfo.Ready = false

	multicastGroup.groupInfo.Members[srcAddr] = memberInfo

	return &registrygrpc.RegistrationAnswer{
		ClientId:  srcAddr,
		GroupInfo: multicastGroup.groupInfo,
	}, nil
}

func (s *server) GetStatus(ctx context.Context, in *registrygrpc.GroupId) (*registrygrpc.GroupInfo, error) {
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

	mGroup, ok := groups[in.GroupId]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}
	mGroup.mu.RLock()
	defer mGroup.mu.RUnlock()

	return mGroup.groupInfo, nil

}

func (s *server) StartGroup(ctx context.Context, in *registrygrpc.RequestData) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.GroupId
	clientId := in.ClientId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId, "client_id": clientId})
	logger.WithField("group", in).Debug("Retrieving status")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[multicastId]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	mGroup.mu.Lock()
	defer mGroup.mu.Unlock()

	if mGroup.groupInfo.Status != registrygrpc.Status_OPENING {
		return nil, status.Errorf(codes.Canceled, "Cannot start group")
	}

	if _, ok = mGroup.groupInfo.Members[clientId]; !ok {
		return nil, status.Errorf(codes.PermissionDenied, "Not registered to the group")
	}

	mGroup.groupInfo.Status = registrygrpc.Status_STARTING

	return mGroup.groupInfo, nil
}

func (s *server) Ready(ctx context.Context, in *registrygrpc.RequestData) (*registrygrpc.GroupInfo, error) {

	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.GroupId
	clientId := in.ClientId

	logger := registryLog.WithFields(log.Fields{"user_ip": src, "multicast_id": multicastId, "client_id": clientId})
	logger.WithField("group", in).Debug("Retrieving status")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[multicastId]

	if !ok {
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	mGroup.mu.Lock()
	defer mGroup.mu.Unlock()

	if mGroup.groupInfo.Status != registrygrpc.Status_STARTING {
		return nil, status.Errorf(codes.Canceled, "Cannot start group")
	}

	var member *registrygrpc.MemberInfo

	if member, ok = mGroup.groupInfo.Members[clientId]; !ok {
		return nil, status.Errorf(codes.PermissionDenied, "Not registered to the group")
	}

	if !member.Ready {
		member.Ready = true
		mGroup.groupInfo.ReadyMembers++
	}

	if mGroup.groupInfo.ReadyMembers == uint64(len(mGroup.groupInfo.Members)) {
		mGroup.groupInfo.Status = registrygrpc.Status_ACTIVE
	}

	return mGroup.groupInfo, nil

}
