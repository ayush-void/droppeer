package signaling

import (
	"errors"
	"sync"

	"github.com/ayush-void/droppeer/internal/types"
)

var (
	ErrReceiverNotPresent = errors.New("the receiver is not logged in")
	ErrWrongPeer          = errors.New("wrong peer is present in the room")
)

type Peer struct {
	ID   string
	Send chan types.Message
}

type Room struct {
	Code  string
	mw    sync.RWMutex
	PeerA *Peer
	PeerB *Peer
}

func (r *Room) Relay(from *Peer, msg types.Message) error {
	r.mw.RLock()
	defer r.mw.RUnlock()
	switch from {
	case r.PeerA:
		if r.PeerB == nil {
			return ErrReceiverNotPresent
		}
		r.PeerB.Send <- msg
	case r.PeerB:
		if r.PeerA == nil {
			return ErrReceiverNotPresent
		}
		r.PeerA.Send <- msg
	default:
		return ErrWrongPeer
	}
	return nil
}
