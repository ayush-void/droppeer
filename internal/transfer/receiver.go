package transfer

import (
	"crypto/sha256"
	"encoding/hex"
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
	msgReader := make(chan types.Message, 2)
	go writeMsg(conn, msgWriter)
	go readMsg(conn, pc, msgReader, "offer")
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
	f := false
	h := sha256.New()
	doneCh := make(chan struct{})
	var fileMetaData *types.Metadata
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			fmt.Println("DataChannel Open waiting for file...")
		})
		var file *os.File
		var byteReceived int64
		var byteToBeReceived int64
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if fileMetaData == nil {
				fileMetaData = &types.Metadata{}
				if err := json.Unmarshal(msg.Data, fileMetaData); err != nil {
					slog.Error("error while unmarshal", "error", err)
					return
				}
				if fileMetaData.Filesize == 0 {
					slog.Error("File Size = 0", "error", fmt.Errorf("File is empty"))
					return
				}
				var ferr error
				fileMetaData.Filename = "received_" + fileMetaData.Filename
				file, ferr = os.Create(fileMetaData.Filename)
				if ferr != nil {
					slog.Error("error while creating file", "error", ferr)
					return
				}
				byteToBeReceived = fileMetaData.Filesize
				return
			}
			h.Write(msg.Data)
			byteWritten, err := file.Write(msg.Data)
			if err != nil {
				slog.Error("error while writing chunk", "error", err)
			}
			byteReceived += int64(byteWritten)
			if byteReceived == byteToBeReceived {
				f = true
				file.Close()
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
	if !f {
		return nil
	}
	checksumReceiver := hex.EncodeToString(h.Sum(nil))
	checksumMsg := <-msgReader
	if checksumMsg.Type == "checksum" {
		if checksumMsg.Payload == checksumReceiver {
			fmt.Println("File Transfer Complete")
		} else {
			fmt.Println("File Corrupted while transfering")
			os.Remove(fileMetaData.Filename)
		}
	} else {
		return fmt.Errorf("wrong message read instead of checksum")
	}
	return nil
}
