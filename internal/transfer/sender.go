package transfer

import (
	"encoding/json"
	"fmt"

	"github.com/ayush-void/droppeer/internal/types"
	"github.com/gorilla/websocket"
)

func Sender(filePath string, signalingURL string) error {
	var msg types.Message
	conn, _, err := websocket.DefaultDialer.Dial(signalingURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"create-room","payload":""}`)); err != nil {
		return err
	}
	_, p, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(p, &msg); err != nil {
		return err
	}
	if msg.Type == "room-created" {
		fmt.Println("Room code:", msg.Payload, "— waiting for receiver...")
	} else {
		return fmt.Errorf("unexpected Message received while sending file")
	}
	_, p, err = conn.ReadMessage()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(p, &msg); err != nil {
		return err
	}
	if msg.Type == "peer-joined" {
		fmt.Println("Peer Joined")
	} else {
		return fmt.Errorf("unexpected message type: %s", msg.Type)
	}

	return nil
}
