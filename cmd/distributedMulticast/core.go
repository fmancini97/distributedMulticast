package main

import (
	"context"
	"errors"
	"fmt"
	logger "github.com/sirupsen/logrus"
	multicasttypemapper "gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/multicastmapper"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/registryrpc"
	"time"
)

const appHeader = "distributed-multicast"

type AppCmd int

const (
	CLOSE = iota
)

var (
	AppCmd_Value = map[string]AppCmd{
		"CLOSE": CLOSE,
	}
	AppCmd_Name = map[AppCmd]string{
		CLOSE: "CLOSE",
	}

	AppCmds = map[AppCmd]func(multicastGroup *MulticastGroup){
		CLOSE: close,
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
		cmd, ok := AppCmd_Value[cmdName]
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

func close(multicastGroup *MulticastGroup) {
	multicastGroup.groupMu.Lock()

	if registrygrpc.Status(registrygrpc.Status_value[multicastGroup.group.Status]) != registrygrpc.Status_ACTIVE {
		multicastGroup.groupMu.Unlock()

	}

	multicastGroup.group.Status = registrygrpc.Status_name[int32(registrygrpc.Status_CLOSING)]

	multicastGroup.groupMu.Unlock()

	groupInfo, err := registryClient.CloseGroup(context.Background(), &registrygrpc.RequestData{GroupId: multicastGroup.group.MulticastId, ClientId: multicastGroup.clientId})
	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	updateMulticastGroup(groupInfo, multicastGroup)
	groupInfo, err = waitStatusChange(groupInfo, multicastGroup, registrygrpc.Status_CLOSING)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	multicastGroup.groupMu.Lock()

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

func initializeGroup(groupInfo *registrygrpc.GroupInfo, multicastGroup *MulticastGroup, isMaster bool) {

	updateMulticastGroup(groupInfo, multicastGroup)
	groupInfo, err := waitStatusChange(groupInfo, multicastGroup, registrygrpc.Status_OPENING)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return

	}

	logger.WithField("group", groupInfo).Info("Initializing Multicast")

	err = initializeMulticaster(multicastGroup, isMaster)

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

	groupInfo, err = registryClient.Ready(context.Background(), &registrygrpc.RequestData{GroupId: multicastGroup.group.MulticastId, ClientId: multicastGroup.clientId})

	if err != nil {
		multicastGroup.groupMu.Lock()
		multicastGroup.group.Error = err
		multicastGroup.groupMu.Unlock()
		return
	}

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

func initializeMulticaster(multicastGroup *MulticastGroup, isMaster bool) error {
	mtType, ok := multicasttypemapper.Type[registrygrpc.MulticastType(registrygrpc.MulticastType_value[multicastGroup.group.MulticastType])][isMaster]

	if !ok {
		return errors.New("unknown multicast type")
	}

	dlv := &GroupDeliverer{group: multicastGroup}

	e2eConnections := make([]*multicast.E2EMember, 0)

	for memberId, member := range multicastGroup.group.Members {
		if memberId != multicastGroup.clientId {
			logger.Infof("Connecting to: %s", member.Address)

			connection, err := e2e.InitializeConnection(member.MemberId, member.Address, 2000)
			if err != nil {
				return err
			}
			e2eConnections = append(e2eConnections, connection)
		}
	}

	multicaster, err := multicast.MulticastInitializers[mtType](multicastGroup.group.MulticastId, multicastGroup.clientId, dlv, e2eConnections)

	if err != nil {
		return err

	}

	err = e2e.AddDeliverer(multicastGroup.group.MulticastId, multicaster)
	if err != nil {
		return err
	}

	multicastGroup.multicaster = multicaster
	return nil
}

func waitStatusChange(groupInfo *registrygrpc.GroupInfo, multicastGroup *MulticastGroup, status registrygrpc.Status) (*registrygrpc.GroupInfo, error) {
	var err error

	for groupInfo.Status == status {
		time.Sleep(time.Second * 5)
		groupInfo, err = registryClient.GetStatus(context.Background(), &registrygrpc.GroupId{GroupId: groupInfo.GroupId})
		if err != nil {
			return nil, err
		}

		updateMulticastGroup(groupInfo, multicastGroup)

	}

	if groupInfo.Status == registrygrpc.Status_CLOSED || groupInfo.Status == registrygrpc.Status_CLOSING {
		return nil, errors.New("multicast group is closed")
	}

	return groupInfo, nil
}

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
