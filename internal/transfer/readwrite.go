package transfer

import (
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/ayush-void/droppeer/internal/types"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

func writeMsg(conn *websocket.Conn, msgWriter chan types.Message) {
	for msg := range msgWriter {
		p, err := json.Marshal(msg)
		if err != nil {
			slog.Error("failed to marshal Message", "error", err)
			continue
		}
		if err := conn.WriteMessage(websocket.TextMessage, p); err != nil {
			slog.Error("failed to write msgICECand", "error", err)
			return
		}
	}
}

func readMsg(conn *websocket.Conn, pc *webrtc.PeerConnection, msgReader chan types.Message, expectedType string) {
	for {
		_, p, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				return // clean close
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			slog.Error("failed to read message", "error", err)
			return
		}
		msg := types.Message{}
		if err := json.Unmarshal(p, &msg); err != nil {
			slog.Error("failed to unmarshal Message", "error", err)
			continue
		}
		switch msg.Type {
		case expectedType, "checksum":
			msgReader <- msg
		case "ice-candidate":
			payloadICECand := webrtc.ICECandidateInit{}
			if err := json.Unmarshal([]byte(msg.Payload), &payloadICECand); err != nil {
				slog.Error("failed to Unmarshal ICECandidate", "error", err)
				continue
			}
			if err := pc.AddICECandidate(payloadICECand); err != nil {
				slog.Error("failed to add ICE candidate", "error", err)
				continue
			}
		default:
			slog.Error("wrong message type", "type", msg.Type)
			continue
		}
	}
}
