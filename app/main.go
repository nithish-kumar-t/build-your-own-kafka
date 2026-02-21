package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// // Read the length prefix (4 bytes)
	// lenBuf := make([]byte, 4)
	// if _, err := io.ReadFull(conn, lenBuf); err != nil {
	// 	if err != io.EOF && err != io.ErrUnexpectedEOF {
	// 		fmt.Println("error reading message size:", err)
	// 	}
	// 	return
	// }
	// reqMessageSize := int(binary.BigEndian.Uint32(lenBuf))
	// if reqMessageSize <= 0 {
	// 	return // or continue
	// }

	// // Read the rest of the request based on the length prefix
	// buffer := make([]byte, reqMessageSize)
	// if _, err := io.ReadFull(conn, buffer); err != nil {
	// 	fmt.Println("error reading message body:", err)
	// 	return
	// }

	// Read the length prefix (4 bytes)
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fmt.Println("error reading message size:", err)
			}
			return
		}
		msgLen := int(binary.BigEndian.Uint32(lenBuf))
		if msgLen <= 0 {
			continue
		}

		// Read the rest of the request based on the length prefix
		req := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, req); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fmt.Println("error reading message body:", err)
			}
			return
		}

		// Extract API version and correlation id from the request header
		// Request header (flexible v2): ApiKey int16, ApiVersion int16, CorrelationId int32
		if len(req) < 8 {
			continue
		}
		version := binary.BigEndian.Uint16(req[2:4])
		corr := req[4:8]

		if version > 4 {
			errBody := []byte{0, 35}
			msgSize := make([]byte, 4)
			// ApiVersions uses flexible body but header remains v0 (no tagged fields).
			binary.BigEndian.PutUint32(msgSize, uint32(len(corr)+len(errBody)))
			conn.Write(append(append(msgSize, corr...), errBody...))
			continue
		}

		// ApiVersions response entries: ApiVersions (18) and DescribeTopicPartitions (75).
		apiKeys := [][]byte{
			{0, 18, 0, 0, 0, 4}, // ApiVersions v0-4
			{0, 75, 0, 0, 0, 0}, // DescribeTopicPartitions v0-0
		}

		body := make([]byte, 0, 64)
		body = append(body, 0, 0)                 // error_code: 0
		body = append(body, byte(len(apiKeys)+1)) // api_keys array length (compact): N+1
		for _, k := range apiKeys {
			body = append(body, k...) // api_key, min_version, max_version
			body = append(body, 0)    // TAG_BUFFER per api_key
		}
		body = append(body, 0, 0, 0, 0) // throttle_time_ms: 0
		body = append(body, 0)          // TAG_BUFFER for response body

		msgSize := make([]byte, 4)
		binary.BigEndian.PutUint32(msgSize, uint32(len(corr)+len(body)))
		conn.Write(append(append(msgSize, corr...), body...))

	}
}
