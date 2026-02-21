package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Helpers to decode Kafka compact types (sufficient for small test payloads).
func readUvarint(b []byte, pos *int) (uint64, bool) {
	v, n := binary.Uvarint(b[*pos:])
	if n <= 0 {
		return 0, false
	}
	*pos += n
	return v, true
}

func skipTagBuffer(b []byte, pos *int) bool {
	sizePlusOne, ok := readUvarint(b, pos)
	if !ok {
		return false
	}
	size := int(sizePlusOne - 1)
	if size < 0 || *pos+size > len(b) {
		return false
	}
	*pos += size
	return true
}

func readCompactString(b []byte, pos *int) (string, bool) {
	sizePlusOne, ok := readUvarint(b, pos)
	if !ok {
		return "", false
	}
	if sizePlusOne == 0 {
		return "", true // null
	}
	size := int(sizePlusOne - 1)
	if size < 0 || *pos+size > len(b) {
		return "", false
	}
	s := string(b[*pos : *pos+size])
	*pos += size
	return s, true
}

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func appendCompactString(dst []byte, s string) []byte {
	dst = appendUvarint(dst, uint64(len(s)+1))
	return append(dst, s...)
}

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
		// Request header (flexible v2): ApiKey int16, ApiVersion int16, CorrelationId int32, ClientId (compact nullable), TAG_BUFFER
		if len(req) < 8 {
			continue
		}
		apiKey := binary.BigEndian.Uint16(req[0:2])
		version := binary.BigEndian.Uint16(req[2:4])
		corr := req[4:8]

		pos := 8
		if _, ok := readCompactString(req, &pos); !ok { // client_id
			continue
		}
		if !skipTagBuffer(req, &pos) { // request header tags
			continue
		}

		if apiKey == 75 { // DescribeTopicPartitions v0
			// topics compact array
			topicsPlusOne, ok := readUvarint(req, &pos)
			if !ok || topicsPlusOne == 0 {
				continue
			}
			if int(topicsPlusOne-1) < 1 {
				continue
			}
			topicName, ok := readCompactString(req, &pos)
			if !ok {
				continue
			}
			if !skipTagBuffer(req, &pos) { // topic tags
				continue
			}
			if !skipTagBuffer(req, &pos) { // request body tags
				continue
			}

			// Response header v1: correlation_id + empty TAG_BUFFER
			header := make([]byte, 0, 5)
			header = append(header, corr...)
			header = append(header, 0) // empty tagged fields

			// Response body
			body := make([]byte, 0, 64)
			body = append(body, 0, 0, 0, 0) // throttle_time_ms
			body = append(body, 2)          // topics compact array length (1 element -> 2)
			body = append(body, 0, 3)       // error_code: UNKNOWN_TOPIC_OR_PARTITION
			body = appendCompactString(body, topicName)
			// topic_id zeros
			body = append(body,
				0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0, 0, 0,
				0, 0, 0, 0)
			body = append(body, 0)          // is_internal: false
			body = append(body, 1)          // partitions compact array length: 0 elements (N+1)
			body = append(body, 0, 0, 0, 0) // topic_authorized_operations: 0
			body = append(body, 0)          // topic TAG_BUFFER
			body = append(body, 0xff)       // next_cursor: -1 (null)
			body = append(body, 0)          // response TAG_BUFFER

			msgSize := make([]byte, 4)
			binary.BigEndian.PutUint32(msgSize, uint32(len(header)+len(body)))
			conn.Write(append(append(msgSize, header...), body...))
			continue
		}

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
