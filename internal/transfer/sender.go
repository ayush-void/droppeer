package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/ayush-void/droppeer/internal/types"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

func Sender(filePath string, signalingURL string) error {
	fileStats, err := os.Stat(filePath)
	if err != nil {
		return err
	}
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
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return err
	}
	defer pc.Close()
	msgWriter := make(chan types.Message, 16)
	msgReader := make(chan types.Message, 2)
	go writeMsg(conn, msgWriter)
	go readMsg(conn, pc, msgReader, "answer")
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil {
			return
		}
		payloadICECand, err := json.Marshal(cand.ToJSON())
		if err != nil {
			slog.Error("failed to marshal ICE candidate init", "error", err)
			return
		}
		msgWriter <- types.Message{
			Type:    "ice-candidate",
			Payload: string(payloadICECand),
		}
	})
	dc, err := pc.CreateDataChannel("file", nil)
	if err != nil {
		return err
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	offerMsg, err := json.Marshal(offer)
	if err != nil {
		return err
	}

	msgWriter <- types.Message{Type: "offer", Payload: string(offerMsg)}
	answerMsg := <-msgReader

	answer := webrtc.SessionDescription{}
	if err := json.Unmarshal([]byte(answerMsg.Payload), &answer); err != nil {
		return err
	}
	doneCh := make(chan struct{})
	h := sha256.New()
	var checksum string
	dc.OnOpen(func() {
		defer close(doneCh)
		fileMetaData := types.Metadata{
			Filename: fileStats.Name(),
			Filesize: fileStats.Size(),
		}
		sendMetaData, err := json.Marshal(fileMetaData)
		if err != nil {
			slog.Error("error while marshal", "error", err)
			return
		}
		if err := dc.Send(sendMetaData); err != nil {
			slog.Error("error while sending metadata", "error", err)
			return
		}
		if fileMetaData.Filesize == 0 {
			slog.Error("File Size = 0", "error", fmt.Errorf("File is empty"))
			return
		}
		file, err := os.Open(filePath)
		if err != nil {
			slog.Error("error while opening file", "error", err)
			return
		}
		defer file.Close()
		buffer := make([]byte, types.ChunkSize)
		for {
			n, err := file.Read(buffer)
			if err == io.EOF {
				checksum = hex.EncodeToString(h.Sum(nil))
				return
			} else if err != nil {
				slog.Error("error while reading file", "error", err)
				return
			}
			h.Write(buffer[:n])
			if err := dc.Send(buffer[:n]); err != nil {
				slog.Error("error while sending metadata", "error", err)
				return
			}
		}
	})
	if err := pc.SetRemoteDescription(answer); err != nil {
		return err
	}
	<-doneCh
	if checksum != "" {
		msgWriter <- types.Message{Type: "checksum", Payload: checksum}
	}
	return nil
}
