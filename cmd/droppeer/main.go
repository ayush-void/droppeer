package main

import (
	"log"
	"log/slog"

	"github.com/ayush-void/droppeer/internal/signaling"
)

func main() {
	server := signaling.NewServer(":8080")
	slog.Info("starting signaling server", "addr", ":8080")
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}
