package distribuitedmulticast

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
	"/groups/:id": {
		HttpMethodGET:    GetGroupById,
		HttpMethodPUT:    CreateGroup,
		HttpMethodPOST:   StartGroup,
		HttpMethodDELETE: CloseGroup,
	},
	"/messages/:groupId": {
		HttpMethodGET: GetMessages,
		HttpMethodPUT: SendMessage,
	},
	"/messages/:groupId/:messagePos": {
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

// GET /groups
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

// GET /registry/groups
func GetAllGroups(ctx *gin.Context) {
	groups := make([]*MulticastGroupInfo, 0)

	stream, err := registryClient.List(context.Background(), &registrygrpc.Empty{})

	if err != nil {
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

		groups = append(groups, &MulticastGroupInfo{
			MulticastId:      groupInfo.GroupId,
			MulticastType:    registrygrpc.MulticastType_name[int32(groupInfo.MulticastType)],
			ReceivedMessages: -1,
			Status:           registrygrpc.Status_name[int32(groupInfo.Status)],
			Members:          members,
		})
	}

	if err != io.EOF {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.IndentedJSON(http.StatusOK, groups)
}

// GET /groups/:id
func GetGroupById(ctx *gin.Context) {
	id := ctx.Param("id")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[id]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	ctx.IndentedJSON(http.StatusOK, group.group)
}

// PUT /groups/:id
func CreateGroup(ctx *gin.Context) {
	var config GroupConfig

	groupId := ctx.Param("id")

	err := ctx.BindJSON(&config)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	multicastType, ok := multicastregistry.MulticastType[config.MulticastType]

	if !ok {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": "MulticastType not supported", "supported": multicastregistry.MulticastTypes})
		return
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	group, ok := multicastGroups[groupId]

	if ok {
		ctx.IndentedJSON(http.StatusConflict, gin.H{"error": "group already exists"})
		return
	}

	registrationAns, err := registryClient.Register(context.Background(), &registrygrpc.RegistrationInfo{
		GroupId:       groupId,
		MulticastType: multicastType,
		ClientPort:    grpcPort,
	})

	if err != nil {
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
func StartGroup(ctx *gin.Context) {

	groupId := ctx.Param("id")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.groupMu.RLock()
	defer group.groupMu.RUnlock()

	//logger.Info("Starting multicast group")
	_, err := registryClient.StartGroup(context.Background(), &registrygrpc.RequestData{GroupId: group.group.MulticastId, ClientId: group.clientId})
	if err != nil {
		//logger.WithField("error", err).Fatal("Error while starting Communication")
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.Status(http.StatusOK)
}

// DELETE /groups/:id
func CloseGroup(ctx *gin.Context) {

	groupId := ctx.Param("id")

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
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not active"})
		return
	}

	err := group.multicaster.SendToAll(communication.Message{
		Metadata: map[string]string{
			fmt.Sprintf("%s:cmd", appHeader): AppCmdName[CLOSE],
		},
		Payload: nil,
	})
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.Status(http.StatusOK)
}

// GET /messages/:groupId
func GetMessages(ctx *gin.Context) {
	groupId := ctx.Param("groupId")

	groupsMu.RLock()
	defer groupsMu.RUnlock()

	group, ok := multicastGroups[groupId]

	if !ok {
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	group.messageMu.RLock()
	defer group.messageMu.RUnlock()

	ctx.IndentedJSON(http.StatusOK, group.messages)
}

// PUT /messages/:groupId
func SendMessage(ctx *gin.Context) {
	var message communication.Message
	groupId := ctx.Param("groupId")

	err := ctx.BindJSON(&message)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
		ctx.IndentedJSON(http.StatusTooEarly, gin.H{"error": "group not ready"})
		return
	}

	err = group.multicaster.SendToAll(message)
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.Status(http.StatusCreated)
}

// GET /messages/:groupId/:messagePos
func GetMessageByPos(ctx *gin.Context) {
	groupId := ctx.Param("groupId")

	messagePosString := ctx.Param("messagePos")

	messagePos, err := strconv.Atoi(messagePosString)

	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
		ctx.IndentedJSON(http.StatusNotFound, gin.H{"error": "group not found"})
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
