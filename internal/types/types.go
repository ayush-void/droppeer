package types

type Message struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

type Metadata struct {
	Filename string `json:"filename"`
	Filesize int64  `json:"filesize"`
}

const ChunkSize int = 16 * 1024
