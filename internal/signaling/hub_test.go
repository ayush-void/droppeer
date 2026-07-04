package signaling

import "testing"

func TestCreateRoom(t *testing.T) {
	hub := NewHub()
	peerA := &Peer{
		ID:   "Aysuh",
		Send: make(chan Message, 256),
	}
	room := hub.CreateRoom("04040", peerA)
	if room == nil {
		t.Errorf("room not created")
		return
	}
	if room.Code != "04040" {
		t.Errorf("wrong Code for the room")
		return
	}
	if room.PeerA == nil {
		t.Errorf("no peerB in the room")
		return
	}
	if room.PeerA != peerA {
		t.Errorf("wrong peerA in the room")
		return
	}
	if _, ok := hub.rooms["04040"]; !ok {
		t.Errorf("Room not in Hub")
		return
	}
}

func TestJoinRoom(t *testing.T) {
	hub := NewHub()
	peerA := &Peer{
		ID:   "Aysuh",
		Send: make(chan Message, 256),
	}
	peerB := &Peer{
		ID:   "Omkar",
		Send: make(chan Message, 256),
	}
	room := hub.CreateRoom("04040", peerA)
	room, err := hub.JoinRoom("04040", peerB)
	if err != nil {
		t.Errorf("Error while joining")
		return
	}
	if room.PeerB == nil {
		t.Errorf("no peerB in the room")
		return
	}
	if room.PeerB != peerB {
		t.Errorf("wrong peerB in the room")
		return
	}
	room, err = hub.JoinRoom("04041", peerB)
	if err == nil {
		t.Errorf("Added peer in empty room")
	}
	room, err = hub.JoinRoom("04040", peerB)
	if err == nil {
		t.Errorf("Added peer in filled room")
	}
}

func TestRelay(t *testing.T) {
	msg := Message{
		Type:    "Test",
		Payload: "ppogeligsnls",
	}
	Troom := &Room{
		Code: "9304",
	}

	peerA := &Peer{
		ID:   "Aysuh",
		Send: make(chan Message, 256),
	}
	if err := Troom.Relay(peerA, msg); err == nil {
		t.Errorf("msg was passed from nil")
	}

	Troom.PeerA = peerA
	if err := Troom.Relay(peerA, msg); err == nil {
		t.Errorf("msg was delivered to nil")
	}
	peerB := &Peer{
		ID:   "Omkar",
		Send: make(chan Message, 256),
	}

	Troom.PeerB = peerB
	err := Troom.Relay(peerA, msg)
	if err != nil {
		t.Errorf("Error while relaying")
	}
	select {
	case x := <-Troom.PeerB.Send:
		if x != msg {
			t.Errorf("wrong message was received")
		}
	default:
		t.Errorf("no message passed")
	}
}
