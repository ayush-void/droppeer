package signaling

import (
	"fmt"
	"sync"
)

type Hub struct {
	rooms map[string]*Room
	rwm   sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		rooms: make(map[string]*Room),
	}
}

func (h *Hub) CreateRoom(code string, peerA *Peer) *Room {
	room := &Room{
		Code:  code,
		PeerA: peerA,
	}
	h.rwm.Lock()
	h.rooms[code] = room
	h.rwm.Unlock()
	return room
}

func (h *Hub) JoinRoom(code string, peerB *Peer) (*Room, error) {
	var room *Room
	h.rwm.Lock()
	defer h.rwm.Unlock()
	room = h.rooms[code]
	if room == nil {
		return nil, fmt.Errorf("room is not created")
	}
	if room.PeerB != nil {
		return nil, fmt.Errorf("room is filled")
	}
	room.PeerB = peerB
	return room, nil
}
