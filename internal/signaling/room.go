package signaling

import "fmt"

type Message struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type Peer struct {
	ID   string
	Send chan Message
}

type Room struct {
	Code  string
	PeerA *Peer
	PeerB *Peer
}

func (r *Room) Relay(from *Peer, msg Message) error {
	switch from {
	case r.PeerA:
		if r.PeerB == nil {
			return fmt.Errorf("the receiver is not logged in")
		}
		r.PeerB.Send <- msg
	case r.PeerB:
		if r.PeerA == nil {
			return fmt.Errorf("the receiver is not logged in")
		}
		r.PeerA.Send <- msg
	default:
		return fmt.Errorf("wrong peer is present in the room")
	}
	return nil
}
