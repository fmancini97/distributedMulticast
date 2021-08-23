// Package distribuitedmulticast provides functions to run a Http REST server running a multicast node
package distribuitedmulticast

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	multicasttypemapper "gitlab.com/f_mancini/distributedMulticast/internal/multicastmapper"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distributedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicast"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry/registrygrpc"
	"sync"
	"time"
)

const appHeader = "distributed-multicast"

var (
	multicastGroups map[string]*MulticastGroup
	groupsMu        sync.RWMutex
	registryClient  registrygrpc.RegistryClient
	grpcPort        uint
	delay           uint
	dmLog           *log.Entry
)

func init() {
	multicastGroups = make(map[string]*MulticastGroup)
	dmLog = log.WithField("app", "distribute_multicast")
}

// MulticastGroup manages the metadata associated with a group in which the node is registered
type MulticastGroup struct {
	clientId    string
	group       *MulticastGroupInfo
	groupMu     sync.RWMutex
	multicaster multicast.Multicast
	messages    []communication.Message
	messageMu   sync.RWMutex
}

type MulticastGroupInfo struct {
	MulticastId      string            `json:"multicast_id"`
	MulticastType    string            `json:"multicast_type"`
	ReceivedMessages int               `json:"received_messages"`
	Status           string            `json:"status"`
	Members          map[string]Member `json:"members"`
	Error            error             `json:"error"`
}

type RegistryGroupInfo struct {
	MulticastId   string            `json:"multicast_id"`
	MulticastType string            `json:"multicast_type"`
	Status        string            `json:"status"`
	Members       map[string]Member `json:"members"`
}

type Member struct {
	MemberId string `json:"member_id"`
	Address  string `json:"address"`
	Ready    bool   `json:"ready"`
}

type GroupConfig struct {
	MulticastType string `json:"multicast_type"`
}

// AppCmd represents a command that can be used between multicast nodes
type AppCmd int

const (
	CLOSE = iota
)

var (
	AppCmdValue = map[string]AppCmd{
		"CLOSE": CLOSE,
	}
	AppCmdName = map[AppCmd]string{
		CLOSE: "CLOSE",
	}

	AppCmds = map[AppCmd]func(multicastGroup *MulticastGroup){
		CLOSE: closeMulticastGroup,
	}
)

type GroupDeliverer struct {
	group *MulticastGroup
}

func (gDlv *GroupDeliverer) Deliver(message communication.Message) error {
	if gDlv.group == nil {
		return errors.New("not initialized")
	}

	gDlv.group.messageMu.Lock()
	defer gDlv.group.messageMu.Unlock()

	cmdName, ok := message.Metadata[fmt.Sprintf("%s:%s", appHeader, "cmd")]

	if ok {
		cmd, ok := AppCmdValue[cmdName]
		if ok {
			cmdFunc, ok := AppCmds[cmd]
			if ok {
				go cmdFunc(gDlv.group)
			}
		}
	}

	gDlv.group.messages = append(gDlv.group.messages, message)
	gDlv.group.group.ReceivedMessages++

	return nil
}

// Run initializes and executes the Rest HTTP server and the multicast node
func Run(grpcP, restPort uint, registryAddr, relativePath string, numThreads, dl uint, debug bool) error {
	grpcPort = grpcP
	delay = dl
	var err error

	dmLog.WithFields(log.Fields{
		"grpcPort":     grpcP,
		"restPort":     restPort,
		"registryAddr": registryAddr,
		"relativePath": relativePath,
		"numthreads":   numThreads,
		"debug":        debug,
	}).Debug("Starting distributed multicast server")

	registryClient, err = multicastregistry.Connect(registryAddr)

	if err != nil {
		return err
	}

	if debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	err = multicast.ConfigPool(int(numThreads))
	if err != nil {
		return err
	}

	router := gin.Default()

	routerGroup := router.Group(relativePath)

	routerMap, err := GetRouterMethods(routerGroup)

	for path, methods := range GetRestAPI() {
		for methodType, method := range methods {
			routerMethod, ok := routerMap[methodType]

			if !ok {
				return errors.New(fmt.Sprintf("missing routing method: %s", HttpMethodName[methodType]))
			}

			routerMethod(path, method)

		}
	}

	err = router.Run(fmt.Sprintf(":%d", restPort))
	if err != nil {
		return err
	}

	return nil
}

// closeMulticastGroup closes a multicast group
func closeMulticastGroup(multicastGroup *MulticastGroup) {
	multicastGroup.groupMu.Lock()

	// Checking if the group can be closed
	if registrygrpc.Status(registrygrpc.Status_value[multicastGroup.group.Status]) != registrygrpc.Status_ACTIVE {
		multicastGroup.groupMu.Unlock()
		return
	}

	// Changing group status
	multicastGroup.group.Status = registrygrpc.Status_name[int32(registrygrpc.Status_CLOSING)]

	multicastGroup.groupMu.Unlock()

	// Closing group in the registry
	groupInfo, err := registryClient.CloseGroup(context.Background(), &registrygrpc.RequestData{MulticastId: multicastGroup.group.MulticastId, ClientId: multicastGroup.clientId})
	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	// Waiting that all member have closed the group
	updateMulticastGroup(groupInfo, multicastGroup)
	groupInfo, err = waitStatusChange(groupInfo, multicastGroup, registrygrpc.Status_CLOSING)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	multicastGroup.groupMu.Lock()

	// Removing structures associated to the multicast group
	err = e2e.RemoveDeliverer(multicastGroup.group.MulticastId)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	err = multicastGroup.multicaster.Close()

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	multicastGroup.multicaster = nil

	multicastGroup.groupMu.Unlock()

}

// initializeGroup initializes a multicast group
func initializeGroup(groupInfo *registrygrpc.GroupInfo, multicastGroup *MulticastGroup, isMaster bool) {

	logger := dmLog.WithField("multicastId", multicastGroup.group.MulticastId)

	logger.Info("Waiting for the group to be ready")

	// Waiting that the group is ready
	updateMulticastGroup(groupInfo, multicastGroup)
	groupInfo, err := waitStatusChange(groupInfo, multicastGroup, registrygrpc.Status_OPENING)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	logger.Debug("Group ready, initializing multicast")

	// Initializing multicast data structures
	err = initializeMulticaster(multicastGroup, isMaster)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	logger.Info("Notify the registry that the multicaster is ready")

	// Communicating to the registry that the node is ready
	groupInfo, err = registryClient.Ready(context.Background(), &registrygrpc.RequestData{MulticastId: multicastGroup.group.MulticastId, ClientId: multicastGroup.clientId})

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	logger.Info("Waiting for the other nodes")

	// Waiting tha all other nodes are ready
	updateMulticastGroup(groupInfo, multicastGroup)
	groupInfo, err = waitStatusChange(groupInfo, multicastGroup, registrygrpc.Status_STARTING)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	logger.Info("Ready to multicast")

}

// initializeMulticaster allocates the data structures and connections necessary for multicasting
func initializeMulticaster(multicastGroup *MulticastGroup, isMaster bool) error {
	mtType, ok := multicasttypemapper.Type[registrygrpc.MulticastType(registrygrpc.MulticastType_value[multicastGroup.group.MulticastType])][isMaster]

	if !ok {
		return errors.New("unknown multicast type")
	}

	logger := dmLog.WithField("multicastId", multicastGroup.group.MulticastId)

	dlv := &GroupDeliverer{group: multicastGroup}

	// Initializing connections to other members
	e2eConnections := make([]*multicast.E2EMember, 0)

	logger.Info("Connecting to multicast members")

	for memberId, member := range multicastGroup.group.Members {
		if memberId != multicastGroup.clientId {
			logger.WithField("address", member.Address).Info("Connecting")

			connection, err := e2e.InitializeConnection(member.MemberId, member.Address, delay)
			if err != nil {
				return err
			}
			e2eConnections = append(e2eConnections, connection)
		}
	}

	logger.Info("Initializing multicaster")

	// Initializing multicaster
	multicaster, err := multicast.Initializers[mtType](multicastGroup.group.MulticastId, multicastGroup.clientId, dlv, e2eConnections)

	if err != nil {
		return err

	}

	logger.Info("Registering multicaster to e2e server")

	err = e2e.RegisterDeliverer(multicastGroup.group.MulticastId, multicaster)
	if err != nil {
		return err
	}

	multicastGroup.multicaster = multicaster

	logger.Info("Multicaster initialized")

	return nil
}

// waitStatusChange waits until the status of the group in the registry changes
func waitStatusChange(groupInfo *registrygrpc.GroupInfo, multicastGroup *MulticastGroup, status registrygrpc.Status) (*registrygrpc.GroupInfo, error) {
	var err error

	logger := dmLog.WithField("multicastId", multicastGroup.group.MulticastId)

	for groupInfo.Status == status {
		time.Sleep(time.Second * 5)
		groupInfo, err = registryClient.GetStatus(context.Background(), &registrygrpc.MulticastId{MulticastId: groupInfo.MulticastId})
		if err != nil {
			return nil, err
		}

		updateMulticastGroup(groupInfo, multicastGroup)
		logger.WithFields(log.Fields{
			"multicastId":   groupInfo.MulticastId,
			"multicastType": groupInfo.MulticastType,
			"readyMembers":  groupInfo.ReadyMembers,
			"status":        registrygrpc.Status_name[int32(groupInfo.Status)],
		}).Debug("New status")
	}

	if groupInfo.Status == registrygrpc.Status_CLOSED || groupInfo.Status == registrygrpc.Status_CLOSING {
		return nil, errors.New("multicast group is closed")
	}

	logger.Info("Group status changed")

	return groupInfo, nil
}

// updateMulticastGroup updates local group infos with ones received from the registry
func updateMulticastGroup(groupInfo *registrygrpc.GroupInfo, multicastGroup *MulticastGroup) {
	multicastGroup.groupMu.Lock()
	defer multicastGroup.groupMu.Unlock()

	multicastGroup.group.Status = registrygrpc.Status_name[int32(groupInfo.Status)]

	for clientId, member := range groupInfo.Members {
		m, ok := multicastGroup.group.Members[clientId]

		if !ok {
			m = Member{
				MemberId: member.Id,
				Address:  member.Address,
				Ready:    member.Ready,
			}

			multicastGroup.group.Members[clientId] = m
		}

		if m.Ready != member.Ready {
			m.Ready = member.Ready
			multicastGroup.group.Members[clientId] = m
		}

	}
}
