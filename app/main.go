package main

import (
	"fmt"
	"net"
	"os"
	"io"
	"bytes"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	// fmt.Println("Listening to TCP server 0.0.0.0:9092")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handle(conn)
	}
}

func handle (conn net.Conn) {
	defer conn.Close()
	
	for {
		received := bytes.Buffer{}
		buff := make([]byte, 1024)


		_, err := conn.Read(buff)
		if err != nil && err == io.EOF {
			break
		}
		correlation_id := buff[8:12]
		msg := []byte{0, 0, 0, 0}
		received.Write(buff)
		res := append(msg, correlation_id...)
		conn.Write(res)
	}
}
