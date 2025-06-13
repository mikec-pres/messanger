package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	listenAddr := flag.String("listen", "", "address to wait for a peer on, e.g. :9000")
	connectAddr := flag.String("connect", "", "peer address to connect to, e.g. 127.0.0.1:9000")
	flag.Parse()

	if (*listenAddr == "") == (*connectAddr == "") {
		fmt.Fprintln(os.Stderr, "use exactly one of -listen or -connect")
		flag.Usage()
		os.Exit(2)
	}

	var conn net.Conn
	var err error

	if *listenAddr != "" {
		var ln net.Listener
		ln, err = net.Listen("tcp", *listenAddr)
		if err != nil {
			log.Fatal(err)
		}
		defer ln.Close()

		fmt.Println("waiting for peer on", ln.Addr())
		conn, err = ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
	} else {
		conn, err = net.Dial("tcp", *connectAddr)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer conn.Close()

	fmt.Println("connected to", conn.RemoteAddr())
	chat(conn)
}

func chat(conn net.Conn) {
	console := bufio.NewReader(os.Stdin)
	peer := bufio.NewReader(conn)

	var line, reply string
	var err error

	go func() {
		for {
			reply, err = peer.ReadString('\n')
			if err == io.EOF {
				fmt.Println("peer disconnected")
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("peer:", strings.TrimRight(reply, "\r\n"))
		}
	}()

	for {
		fmt.Print("> ")
		line, err = console.ReadString('\n')
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")

		_, err = fmt.Fprintf(conn, "%s\n", line)
		if err != nil {
			log.Fatal(err)
		}
	}
}
