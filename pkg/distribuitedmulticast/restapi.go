package distribuitedmulticast

import (
	"context"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry/registrygrpc"
	"io"
	"net/http"
	"strconv"
	"sync"
)

type HttpMethod int

const (
	HttpMethodGET HttpMethod = iota
	HttpMethodPOST
	HttpMethodPUT
	HttpMethodDELETE
	HttpMethodHEAD
)

var (
	HttpMethodName = map[HttpMethod]string{
		HttpMethodGET:    "GET",
		HttpMethodPOST:   "POST",
		HttpMethodPUT:    "PUT",
		HttpMethodDELETE: "DELETE",
		HttpMethodHEAD:   "HEAD",
	}
	MulticastTypeValue = map[string]HttpMethod{
		"GET":    HttpMethodGET,
		"POST":   HttpMethodPOST,
		"PUT":    HttpMethodPUT,
		"DELETE": HttpMethodDELETE,
		"HEAD":   HttpMethodHEAD,
	}
)

var restAPI = map[string]map[HttpMethod]func(ctx *gin.Context){
	"/groups": {
		HttpMethodGET: GetGroups,
	},
	"/registry/groups": {
		HttpMethodGET: GetAllGroups,
	},
	"/groups/:multicastId": {
		HttpMethodGET:    GetGroupById,
		HttpMethodPUT:    CreateGroup,
		HttpMethodPOST:   StartGroup,
		HttpMethodDELETE: CloseGroup,
	},
	"/messages/:multicastId": {
		HttpMethodGET: GetMessages,
		HttpMethodPUT: SendMessage,
	},
	"/messages/:multicastId/:messagePos": {
		HttpMethodGET: GetMessageByPos,
	},
}

func GetRestAPI() map[string]map[HttpMethod]func(ctx *gin.Context) {
	api := make(map[string]map[HttpMethod]func(ctx *gin.Context))

	for key, value := range restAPI {
		api[key] = value
	}

	return api
}

func GetRouterMethods(router *gin.RouterGroup) (map[HttpMethod]func(path string, handlers ...gin.HandlerFunc) gin.IRoutes, error) {
	routerMap := make(map[HttpMethod]func(path string, handlers ...gin.HandlerFunc) gin.IRoutes)

	routerMap[HttpMethodGET] = router.GET
	routerMap[HttpMethodPOST] = router.POST
	routerMap[HttpMethodPUT] = router.PUT
	routerMap[HttpMethodDELETE] = router.DELETE
	routerMap[HttpMethodHEAD] = router.HEAD

	return routerMap, nil

}

// GetGroups returns the groups to which the node is registered
func GetGroups(ctx *gin.Context) {

	groups := make([]*MulticastGroupInfo, 0)

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	for _, group := range multicastGroups {
		group.groupMu.RLock()
		groups = append(groups, group.group)
		group.groupMu.RUnlock()
	}

	ctx.IndentedJSON(http.StatusOK, groups)
}

// GetAllGroups returns all groups of the node registry
func GetAllGroups(ctx *gin.Context) {

	logger := dmLog.WithField("method", "GetAllGroups")

	groups := make([]*RegistryGroupInfo, 0)

	logger.Debug("Retrieving group infos from registry")

	stream, err := registryClient.List(context.Background(), &registrygrpc.Empty{})

	if err != nil {
		logger.WithField("error", err).Error("Error while communicating with registry")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	groupInfo, err := stream.Recv()

	for ; err == nil; groupInfo, err = stream.Recv() {

		members := make(map[string]Member, 0)

		for memberId, member := range groupInfo.Members {
			members[memberId] = Member{
				MemberId: member.Id,
				Address:  member.Address,
				Ready:    member.Ready,
			}
		}

		groups = append(groups, &RegistryGroupInfo{
			MulticastId:   groupInfo.MulticastId,
			MulticastType: registrygrpc.MulticastType_name[int32(groupInfo.MulticastType)],
			Status:        registrygrpc.Status_name[int32(groupInfo.Status)],
			Members:       members,
		})
	}

	if err != io.EOF {
		logger.WithField("error", err).Error("Error while retrieving group info from registry")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	logger.Info("Returning groups")

	ctx.IndentedJSON(http.StatusOK, groups)
}

// GetGroupById returns group information based on input id
func GetGroupById(ctx *gin.Context) {
	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "GetGroupById", "multicastId": multicastId})

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	logger.Info("Returning group")

	ctx.IndentedJSON(http.StatusOK, group.group)
}

// CreateGroup initializes a new multicast group.
func CreateGroup(ctx *gin.Context) {
	var config GroupConfig

	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "CreateGroup", "multicastId": multicastId})

	err := ctx.BindJSON(&config)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing request")
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	multicastType, ok := multicastregistry.MulticastType[config.MulticastType]

	if !ok {
		logger.WithField("multicastType", multicastType).Warn("MulticastType not supported")
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": "multicast_type not supported", "supported": multicastregistry.MulticastTypes})
		return
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	group, ok := multicastGroups[multicastId]

	if ok {
		logger.Warn("Group already exists")
		ctx.IndentedJSON(http.StatusConflict, gin.H{"error": "group already exists"})
		return
	}

	logger.Info("Registering group to multicast registry")

	registrationAns, err := registryClient.Register(context.Background(), &registrygrpc.RegistrationInfo{
		MulticastId:   multicastId,
		MulticastType: multicastType,
		ClientPort:    uint32(grpcPort),
	})

	if err != nil {
		logger.WithField("error", err).Error("Error while registering group")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	members := make(map[string]Member, 0)

	for memberId, member := range registrationAns.GroupInfo.Members {
		members[memberId] = Member{
			MemberId: member.Id,
			Address:  member.Address,
			Ready:    member.Ready,
		}
	}

	group = &MulticastGroup{
		clientId: registrationAns.ClientId,
		group: &MulticastGroupInfo{
			MulticastId:      registrationAns.GroupInfo.MulticastId,
			MulticastType:    registrygrpc.MulticastType_name[int32(registrationAns.GroupInfo.MulticastType)],
			ReceivedMessages: 0,
			Status:           registrygrpc.Status_name[int32(registrationAns.GroupInfo.Status)],
			Members:          members,
		},
		multicaster: nil,
		messages:    make([]communication.Message, 0),
		groupMu:     sync.RWMutex{},
	}

	multicastGroups[registrationAns.GroupInfo.MulticastId] = group

	logger.Info("Staring group initialization")

	go initializeGroup(registrationAns.GroupInfo, group, len(registrationAns.GroupInfo.Members) == 1)

	ctx.Status(http.StatusOK)
}

// StartGroup completes the initialization phase of a multicast group.
// The aforementioned group can now be used to perform multicast operations
func StartGroup(ctx *gin.Context) {

	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "StartGroup", "multicastId": multicastId})

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	logger.Info("Starting multicast group")
	_, err := registryClient.StartGroup(context.Background(), &registrygrpc.RequestData{MulticastId: group.group.MulticastId, ClientId: group.clientId})
	if err != nil {
		logger.WithField("error", err).Error("Error while staring multicast group")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	logger.Info("Group started")

	ctx.Status(http.StatusOK)
}

// CloseGroup closes a multicast group.
// A closed group can no longer be used to multicast messages but received messages can still be read.
func CloseGroup(ctx *gin.Context) {

	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "StartGroup", "multicastId": multicastId})

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	if registrygrpc.Status(registrygrpc.Status_value[group.group.Status]) != registrygrpc.Status_ACTIVE {
		logger.Warn("Group not active")
		ctx.IndentedJSON(http.StatusServiceUnavailable, gin.H{"error": "group not active"})
		return
	}

	logger.Info("Sending closing command")

	err := group.multicaster.SendToAll(communication.Message{
		Metadata: map[string]string{
			fmt.Sprintf("%s:cmd", appHeader): AppCmdName[CLOSE],
		},
		Payload: nil,
	})
	if err != nil {
		logger.WithField("error", err).Error("Error while sending closing message")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	logger.Info("Closing procedure started")

	ctx.Status(http.StatusOK)
}

// SendMessage sends a message in an active multicast group
func SendMessage(ctx *gin.Context) {
	var message communication.Message
	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "SendMessage", "multicastId": multicastId})

	err := ctx.BindJSON(&message)

	if err != nil {
		logger.Warn("Error while parsing message")
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if message.Metadata == nil {
		message.Metadata = make(map[string]string)
	}

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	if registrygrpc.Status(registrygrpc.Status_value[group.group.Status]) != registrygrpc.Status_ACTIVE {
		logger.Warn("Group not active")
		ctx.IndentedJSON(http.StatusTooEarly, gin.H{"error": "group not ready"})
		return
	}

	logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload}).Info("Sending Message")

	err = group.multicaster.SendToAll(message)
	if err != nil {
		logger.WithField("error", err).Error("Error while sending message")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	logger.Info("Message sent")

	ctx.Status(http.StatusCreated)
}

// GetMessages returns all messages received by a multicast group
func GetMessages(ctx *gin.Context) {
	multicastId := ctx.Param("multicastId")

	logger := dmLog.WithFields(log.Fields{"method": "GetMessages", "multicastId": multicastId})

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	logger.Info("Retrieving messages")

	group.messageMu.RLock()
	defer group.messageMu.RUnlock()

	logger.Info("Returning messages")

	ctx.IndentedJSON(http.StatusOK, group.messages)
}

// GetMessageByPos returns the message at messagePos position of a multicast group
func GetMessageByPos(ctx *gin.Context) {
	multicastId := ctx.Param("multicastId")

	messagePosString := ctx.Param("messagePos")

	logger := dmLog.WithFields(log.Fields{"method": "GetMessageByPos", "multicastId": multicastId, "messagePos": messagePosString})

	messagePos, err := strconv.Atoi(messagePosString)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing input parameters")
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if messagePos < 0 {
		logger.WithField("error", errors.New("negative position")).Warn("Error while parsing input parameters")
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": "negative position"})
		return
	}

	logger.Info("Retrieving message")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[multicastId]

	if !ok {
		logger.Warn("Group not found")
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.messageMu.RLock()
	defer group.messageMu.RUnlock()

	var message *communication.Message

	if messagePos < len(group.messages) {
		message = &group.messages[messagePos]
	}

	logger.Info("Returning message")

	ctx.IndentedJSON(http.StatusOK, message)
}
