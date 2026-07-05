package signaling

import (
	"errors"
	"sync"
)

var (
	ErrRoomNotFound = errors.New("room is not created")
	ErrRoomFilled   = errors.New("room is filled")
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
		return nil, ErrRoomNotFound
	}
	if room.PeerB != nil {
		return nil, ErrRoomFilled
	}
	room.mw.Lock()
	room.PeerB = peerB
	room.mw.Unlock()
	return room, nil
}
