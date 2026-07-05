package signaling

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/websocket"
)

type Server struct {
	hub    *Hub
	server *http.Server
}

func writeError(w http.ResponseWriter, status int, msg string) {
	http.Error(w, msg, status)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Server is up"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot perform response write")
		return
	}
}

var upgrader = &websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func generateCode() (string, error) {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	code := make([]byte, 6)
	_, err := rand.Read(code)
	if err != nil {
		return "", err
	}
	for i := range code {
		code[i] = charset[code[i]%byte(len(charset))]
	}
	return string(code), nil
}

func (s *Server) handleWs(w http.ResponseWriter, r *http.Request) {
	id, err := generateCode()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "id cannot be generated")
		return
	}
	peer := &Peer{
		ID:   id,
		Send: make(chan Message, 1024),
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot upgrade connection")
		return
	}
	defer conn.Close()
	msg := Message{}
	var room *Room
	for {
		_, p, err := conn.ReadMessage()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			return
		} else if err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"cannot read message"}`))
			return
		}
		if err := json.Unmarshal(p, &msg); err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"Unmarshal unsuccessful"}`))
			return
		}

		if msg.Type == "join-room" {
			//p-> payload: {type: "join-room", payload: "<code>"}
			room, err = s.hub.JoinRoom(msg.Payload, peer)
			if err != nil {
				if errors.Is(err, ErrRoomFilled) {
					conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"room filled"}`))
					continue
				} else if errors.Is(err, ErrRoomNotFound) {
					conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"room not found"}`))
					return
				}
				return
			}
			msg.Type = "peer-joined"
			msg.Payload = ""
			p, err = json.Marshal(msg)
			if err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"marshall unsuccessful"}`))
				return
			}
			if err = conn.WriteMessage(websocket.TextMessage, p); err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"write unsuccessful"}`))
				return
			}
			room.PeerA.Send <- Message{Type: "peer-joined", Payload: ""}
			break
		} else if msg.Type == "create-room" {
			code, err := generateCode()
			if err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"code not generated"}`))
				return
			}
			room = s.hub.CreateRoom(code, peer)
			msg.Type = "room-created"
			msg.Payload = code
			p, err = json.Marshal(msg)
			if err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"marshall unsuccessful"}`))
				return
			}
			if err = conn.WriteMessage(websocket.TextMessage, p); err != nil {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"write unsuccessful"}`))
				return
			}
			break
		} else {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"expected create-room or join-room"}`))
			continue
		}
	}

	go writerLoop(conn, peer.Send)
	readerLoop(conn, room, peer)
}

func writerLoop(conn *websocket.Conn, send chan Message) {
	var err error
	var p []byte
	for msg := range send {
		p, err = json.Marshal(msg)
		if err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"marshall unsuccessful"}`))
			return
		}
		if err = conn.WriteMessage(websocket.TextMessage, p); err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"write unsuccessful"}`))
			return
		}
	}
}

func readerLoop(conn *websocket.Conn, room *Room, peer *Peer) {
	defer close(peer.Send)
	var msg Message
	for {
		_, p, err := conn.ReadMessage()
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
			return
		} else if err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"cannot read message"}`))
			return
		}
		if err := json.Unmarshal(p, &msg); err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"Unmarshal unsuccessful"}`))
			continue
		}
		err = room.Relay(peer, msg)
		if err != nil {
			if errors.Is(err, ErrReceiverNotPresent) {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"Receiver not Present"}`))
				continue
			} else if errors.Is(err, ErrWrongPeer) {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","payload":"Wrong Peer present in room"}`))
				return
			}
		}
	}
}

func NewServer(addr string) *Server {
	s := Server{
		hub: NewHub(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ws", s.handleWs)
	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return &s
}

func (s *Server) Run() error {
	return s.server.ListenAndServe()
}
