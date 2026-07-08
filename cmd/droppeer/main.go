package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/ayush-void/droppeer/internal/signaling"
	"github.com/ayush-void/droppeer/internal/transfer"
)

func main() {
	sendCmd := flag.NewFlagSet("send", flag.ExitOnError)
	receiveCmd := flag.NewFlagSet("receive", flag.ExitOnError)
	filePath := sendCmd.String("file", "", "Send the file to the other Peer")
	code := receiveCmd.String("code", "", "Connect to the room and receive the file")
	if len(os.Args) < 2 {
		fmt.Println("usage: droppeer <serve|send|receive>")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		server := signaling.NewServer(":8080")
		slog.Info("starting signaling server", "addr", ":8080")
		if err := server.Run(); err != nil {
			log.Fatal(err)
		}
	case "send":
		sendCmd.Parse(os.Args[2:])
		if *filePath == "" {
			fmt.Println("Give \"--file={file_Path}\"")
			return
		}
		if err := transfer.Sender(*filePath, "ws://localhost:8080/ws"); err != nil {
			log.Fatal(err)
		}
	case "receive":
		receiveCmd.Parse(os.Args[2:])
		if len(*code) != 6 {
			fmt.Println("Give Proper Code")
			return
		}
		if err := transfer.Receiver(*code, "ws://localhost:8080/ws"); err != nil {
			log.Fatal(err)
		}
	default:
		fmt.Println("Wrong command passed")
		return
	}
}
