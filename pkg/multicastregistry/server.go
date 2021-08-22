package multicastregistry

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry/registrygrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

var (
	groups   map[string]*group
	groupsMu sync.RWMutex
)

func init() {
	groups = make(map[string]*group)
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

	multicastId := in.MulticastId

	logger := registryServerLog.WithFields(log.Fields{
		"method":      "Register",
		"clientAddr":  src,
		"multicastId": multicastId,
	})

	logger.WithField("registrationInfo", in).Info("New registration")

	groupsMu.Lock()
	defer groupsMu.Unlock()

	multicastGroup, exists := groups[multicastId]

	if exists {
		logger.WithField("multicastGroup", multicastGroup).Info("Multicast group already exists")

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
		logger.WithField("multicastGroup", multicastGroup).Info("Missing multicast group... creating")

		multicastGroup = &group{groupInfo: &registrygrpc.GroupInfo{
			MulticastId:   multicastId,
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

	logger.Info("Member registered successfully")

	return &registrygrpc.RegistrationAnswer{
		ClientId:  srcAddr,
		GroupInfo: multicastGroup.groupInfo,
	}, nil
}

func (s *server) GetStatus(ctx context.Context, in *registrygrpc.MulticastId) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.MulticastId

	logger := registryServerLog.WithFields(log.Fields{
		"method":      "GetStatus",
		"clientAddr":  src,
		"multicastId": multicastId,
	})

	logger.WithField("group", in).Info("Retrieving status")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[in.MulticastId]

	if !ok {
		logger.Warn("Missing group")

		return nil, status.Errorf(codes.NotFound, "Group not found")
	}
	mGroup.mu.RLock()
	defer mGroup.mu.RUnlock()

	logger.Info("Status retrieved correctly")

	return mGroup.groupInfo, nil

}

func (s *server) StartGroup(ctx context.Context, in *registrygrpc.RequestData) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.MulticastId
	clientId := in.ClientId

	logger := registryServerLog.WithFields(log.Fields{
		"method":      "StartGroup",
		"clientAddr":  src,
		"multicastId": multicastId,
		"clientId":    clientId,
	})

	logger.Info("Starting group")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[multicastId]

	if !ok {
		logger.Warn("Missing group")
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	mGroup.mu.Lock()
	defer mGroup.mu.Unlock()

	if mGroup.groupInfo.Status != registrygrpc.Status_OPENING {
		logger.WithField("status", registrygrpc.Status_name[int32(mGroup.groupInfo.Status)]).Warn("Cannot start the group")
		return nil, status.Errorf(codes.Canceled, "Cannot start group")
	}

	if _, ok = mGroup.groupInfo.Members[clientId]; !ok {
		logger.Warn("Client does not belong to the group")
		return nil, status.Errorf(codes.PermissionDenied, "Not registered to the group")
	}

	mGroup.groupInfo.Status = registrygrpc.Status_STARTING

	logger.Info("Group started correctly")

	return mGroup.groupInfo, nil
}

func (s *server) CloseGroup(ctx context.Context, in *registrygrpc.RequestData) (*registrygrpc.GroupInfo, error) {
	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.MulticastId
	clientId := in.ClientId

	logger := registryServerLog.WithFields(log.Fields{
		"method":      "CloseGroup",
		"clientAddr":  src,
		"multicastId": multicastId,
		"clientId":    clientId,
	})

	logger.Info("Starting closing procedure")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[multicastId]

	if !ok {
		logger.Warn("Missing group")
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	mGroup.mu.Lock()
	defer mGroup.mu.Unlock()

	if mGroup.groupInfo.Status != registrygrpc.Status_ACTIVE && mGroup.groupInfo.Status != registrygrpc.Status_CLOSING {
		logger.WithField("status", registrygrpc.Status_name[int32(mGroup.groupInfo.Status)]).Warn("Cannot close the group")
		return nil, status.Errorf(codes.Canceled, "Cannot start group")
	}

	member, ok := mGroup.groupInfo.Members[clientId]

	if !ok {
		logger.Warn("Client does not belong to the group")
		return nil, status.Errorf(codes.PermissionDenied, "Not registered to the group")
	}

	mGroup.groupInfo.Status = registrygrpc.Status_CLOSING

	if member.Ready {
		member.Ready = false
		mGroup.groupInfo.ReadyMembers--
		if mGroup.groupInfo.ReadyMembers == 0 {
			logger.Info("Closing group")
			mGroup.groupInfo.Status = registrygrpc.Status_CLOSED
		}
	}

	logger.Info("Closing procedure started correctly")

	return mGroup.groupInfo, nil
}

func (s *server) Ready(ctx context.Context, in *registrygrpc.RequestData) (*registrygrpc.GroupInfo, error) {

	source, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Missing source address")
	}

	src := source.Addr.String()

	multicastId := in.MulticastId
	clientId := in.ClientId

	logger := registryServerLog.WithFields(log.Fields{
		"method":      "Ready",
		"sourceAddr":  src,
		"multicastId": multicastId,
		"clientId":    clientId,
	})

	logger.Info("New ready node")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	mGroup, ok := groups[multicastId]

	if !ok {
		logger.Warn("Missing group")
		return nil, status.Errorf(codes.NotFound, "Group not found")
	}

	mGroup.mu.Lock()
	defer mGroup.mu.Unlock()

	if mGroup.groupInfo.Status != registrygrpc.Status_STARTING {
		logger.WithField("status", registrygrpc.Status_name[int32(mGroup.groupInfo.Status)]).Warn("Cannot register ready node")
		return nil, status.Errorf(codes.Canceled, "Cannot start group")
	}

	var member *registrygrpc.MemberInfo

	if member, ok = mGroup.groupInfo.Members[clientId]; !ok {
		logger.Warn("Client does not belong to the group")
		return nil, status.Errorf(codes.PermissionDenied, "Not registered to the group")
	}

	if !member.Ready {
		member.Ready = true
		mGroup.groupInfo.ReadyMembers++
	}

	if mGroup.groupInfo.ReadyMembers == uint64(len(mGroup.groupInfo.Members)) {
		mGroup.groupInfo.Status = registrygrpc.Status_ACTIVE
	}

	logger.Info("Procedure completed correctly")

	return mGroup.groupInfo, nil

}

func (s *server) List(_ *registrygrpc.Empty, stream registrygrpc.Registry_ListServer) error {
	source, ok := peer.FromContext(stream.Context())

	var src string

	if ok {
		src = source.Addr.String()
	}

	logger := registryServerLog.WithFields(log.Fields{
		"method":     "List",
		"sourceAddr": src,
	})

	logger.Info("Getting groups info")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	for _, mGroup := range groups {
		mGroup.mu.RLock()
		err := stream.Send(mGroup.groupInfo)
		mGroup.mu.RUnlock()
		if err != nil {
			logger.WithField("error", err).Error("Error while sending results")

			return err
		}
	}

	logger.Info("Groups sent correctly")

	return nil
}
