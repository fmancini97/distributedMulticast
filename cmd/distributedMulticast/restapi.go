package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/registryrpc"
	"io"
	"net/http"
	"strconv"
	"sync"
)

type HttpMethod int

const (
	GET HttpMethod = iota
	POST
	PUT
	DELETE
	HEAD
)

var (
	HttpMethod_name = map[HttpMethod]string{
		GET:    "GET",
		POST:   "POST",
		PUT:    "PUT",
		DELETE: "DELETE",
		HEAD:   "HEAD",
	}
	MulticastType_value = map[string]HttpMethod{
		"GET":    GET,
		"POST":   POST,
		"PUT":    PUT,
		"DELETE": DELETE,
		"HEAD":   HEAD,
	}
)

func getRouterMethods(router *gin.RouterGroup) (map[HttpMethod]func(path string, handlers ...gin.HandlerFunc) gin.IRoutes, error) {
	routerMap := make(map[HttpMethod]func(path string, handlers ...gin.HandlerFunc) gin.IRoutes)

	routerMap[GET] = router.GET
	routerMap[POST] = router.POST
	routerMap[PUT] = router.PUT
	routerMap[DELETE] = router.DELETE
	routerMap[HEAD] = router.HEAD

	return routerMap, nil

}

var restAPI = map[string]map[HttpMethod]func(ctx *gin.Context){
	"/groups": {
		GET: getGroups,
	},
	"/registry/groups": {
		GET: getAllGroups,
	},
	"/groups/:id": {
		GET:    getGroupById,
		PUT:    createGroup,
		POST:   startGroup,
		DELETE: closeGroup,
	},
	"/messages/:groupId": {
		GET: getMessages,
		PUT: sendMessage,
	},
	"/messages/:groupId/:messagePos": {
		GET: getMessageByPos,
	},
}

// GET /groups
func getGroups(ctx *gin.Context) {
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

// GET /registry/groups
func getAllGroups(ctx *gin.Context) {
	groups := make([]*MulticastGroupInfo, 0)

	stream, err := registryClient.List(context.Background(), nil)

	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
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

		groups = append(groups, &MulticastGroupInfo{
			MulticastId:      groupInfo.GroupId,
			MulticastType:    registrygrpc.MulticastType_name[int32(groupInfo.MulticastType)],
			ReceivedMessages: -1,
			Status:           registrygrpc.Status_name[int32(groupInfo.Status)],
			Members:          members,
		})
	}

	if err != io.EOF {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ctx.IndentedJSON(http.StatusOK, groups)
}

// GET /groups/:id
func getGroupById(ctx *gin.Context) {
	id := ctx.Param("id")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[id]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	ctx.IndentedJSON(http.StatusOK, group.group)
}

// PUT /groups/:id
func createGroup(ctx *gin.Context) {
	var config GroupConfig

	groupId := ctx.Param("id")

	err := ctx.BindJSON(&config)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"message": err})
		return
	}

	multicastType, ok := multicastregistry.MulticastType[config.MulticastType]

	if !ok {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"message": "MulticastType not supported", "supported": multicastregistry.MulticastTypes})
		return
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	group, ok := multicastGroups[groupId]

	if ok {
		ctx.IndentedJSON(http.StatusConflict, gin.H{"message": "group already exists"})
		return
	}

	registrationAns, err := registryClient.Register(context.Background(), &registrygrpc.RegistrationInfo{
		GroupId:       groupId,
		MulticastType: multicastType,
		ClientPort:    grpcPort,
	})

	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
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
			MulticastId:      registrationAns.GroupInfo.GroupId,
			MulticastType:    registrygrpc.MulticastType_name[int32(registrationAns.GroupInfo.MulticastType)],
			ReceivedMessages: 0,
			Status:           registrygrpc.Status_name[int32(registrationAns.GroupInfo.Status)],
			Members:          members,
		},
		multicaster: nil,
		messages:    make([]communication.Message, 0),
		groupMu:     sync.RWMutex{},
	}

	multicastGroups[registrationAns.GroupInfo.GroupId] = group

	go initializeGroup(registrationAns.GroupInfo, group, len(registrationAns.GroupInfo.Members) == 1)

	ctx.Status(http.StatusOK)
}

// POST /groups/:id
func startGroup(ctx *gin.Context) {

	groupId := ctx.Param("id")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	//logger.Info("Starting multicast group")
	_, err := registryClient.StartGroup(context.Background(), &registrygrpc.RequestData{GroupId: group.group.MulticastId, ClientId: group.clientId})
	if err != nil {
		//logger.WithField("error", err).Fatal("Error while starting Communication")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ctx.Status(http.StatusOK)
}

// DELETE /groups/:id
func closeGroup(ctx *gin.Context) {

	groupId := ctx.Param("id")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	if registrygrpc.Status(registrygrpc.Status_value[group.group.Status]) != registrygrpc.Status_ACTIVE {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not active"})
		return
	}

	err := group.multicaster.SendToAll(communication.Message{
		Metadata: map[string]string{
			fmt.Sprintf("%s:cmd", appHeader): AppCmd_Name[CLOSE],
		},
		Payload: nil,
	})
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ctx.Status(http.StatusOK)
}

// GET /messages/:groupId
func getMessages(ctx *gin.Context) {
	groupId := ctx.Param("groupId")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not found"})
		return
	}

	group.messageMu.RLock()
	defer group.messageMu.RUnlock()

	ctx.IndentedJSON(http.StatusOK, group.messages)
}

type JSONMessage struct {
	Payload string `json:"payload"`
}

// PUT /messages/:groupId
func sendMessage(ctx *gin.Context) {
	var message communication.Message
	groupId := ctx.Param("groupId")

	err := ctx.BindJSON(&message)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if message.Metadata == nil {
		message.Metadata = make(map[string]string)
	}

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	if registrygrpc.Status(registrygrpc.Status_value[group.group.Status]) != registrygrpc.Status_ACTIVE {
		ctx.IndentedJSON(http.StatusTooEarly, gin.H{"message": "group not ready"})
		return
	}

	err = group.multicaster.SendToAll(message)
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	ctx.Status(http.StatusCreated)
}

// GET /messages/:groupId/:messagePos
func getMessageByPos(ctx *gin.Context) {
	groupId := ctx.Param("groupId")

	messagePosString := ctx.Param("messagePos")

	messagePos, err := strconv.Atoi(messagePosString)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if messagePos < 0 {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": "negative position"})
		return
	}

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"message": "group not found"})
		return
	}

	group.messageMu.RLock()
	defer group.messageMu.RUnlock()

	var message *communication.Message

	if messagePos < len(group.messages) {
		message = &group.messages[messagePos]
	}

	ctx.IndentedJSON(http.StatusOK, message)
}
