package transfer

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/ayush-void/droppeer/internal/types"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
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
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return err
	}
	defer pc.Close()
	msgWriter := make(chan types.Message, 16)
	msgReader := make(chan types.Message, 1)
	go writeMsg(conn, msgWriter)
	go readMsg(conn, pc, msgReader)
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil {
			return
		}
		payloadICECand, err := json.Marshal(cand.ToJSON())
		if err != nil {
			slog.Error("Error while Marshalling ICECandidateInit", "error", err)
			return
		}
		msgWriter <- types.Message{Type: "ice-candidate", Payload: string(payloadICECand)}
	})

	doneCh := make(chan struct{})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc == nil {
			return
		}
		dc.OnOpen(func() {
			fmt.Println("DataChannel Open waiting for file...")
		})
		var fileMetaData *types.Metadata
		var file *os.File
		var byteReceived int64 = 0
		var byteToBeReceived int64
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if fileMetaData == nil {
				fileMetaData = &types.Metadata{}
				if err = json.Unmarshal(msg.Data, fileMetaData); err != nil {
					slog.Error("error while unmarshal", "error", err)
					return
				}
				file, err = os.Create(fileMetaData.Filename)
				if err != nil {
					slog.Error("error while creating file", "error", err)
					return
				}
				byteToBeReceived = fileMetaData.Filesize
				return
			}
			byteWritten, err := file.Write(msg.Data)
			if err != nil {
				slog.Error("error while writing chunk", "error", err)
			}
			byteReceived += int64(byteWritten)
			if byteReceived == byteToBeReceived {
				fmt.Println("File Transfer Complete")
				close(doneCh)
				return
			}
		})
	})

	offerMsg := <-msgReader
	offer := webrtc.SessionDescription{}
	if err := json.Unmarshal([]byte(offerMsg.Payload), &offer); err != nil {
		return err
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		return err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	answerMsg, err := json.Marshal(answer)
	if err != nil {
		return err
	}
	msgWriter <- types.Message{Type: "answer", Payload: string(answerMsg)}
	<-doneCh
	return nil
}
