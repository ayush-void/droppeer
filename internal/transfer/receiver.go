package transfer

import (
	"encoding/json"
	"fmt"

	"github.com/ayush-void/droppeer/internal/types"
	"github.com/gorilla/websocket"
)

func Receiver(code string, signalingURL string) error {
	conn, _, err := websocket.DefaultDialer.Dial(signalingURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	p, err := json.Marshal(types.Message{Type: "join-room", Payload: code})
	if err != nil {
		return err
	}
	if err = conn.WriteMessage(websocket.TextMessage, p); err != nil {
		return err
	}
	msg := types.Message{}
	_, p, err = conn.ReadMessage()
	if err != nil {
		return err
	}
	if err = json.Unmarshal(p, &msg); err != nil {
		return err
	}
	if msg.Type != "peer-joined" {
		return fmt.Errorf("unexpected message type: %s", msg.Type)
	}

	return nil
}
