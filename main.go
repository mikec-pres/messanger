package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
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

	fmt.Println("connected to", conn.RemoteAddr())
	err = chat(conn)
	if err != nil {
		log.Fatal(err)
	}
}

func chat(conn net.Conn) error {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)

	done := make(chan struct{})
	defer close(done)

	lines := make(chan string)
	consoleErr := make(chan error, 1)
	go readConsole(lines, consoleErr, done)

	peerErr := make(chan error, 1)
	go func() {
		peerErr <- readPeer(conn)
	}()

	for {
		select {
		case line := <-lines:
			_, err := fmt.Fprintf(conn, "%s\n", line)
			if err != nil {
				return closeAndWait(conn, peerErr, fmt.Errorf("send: %w", err))
			}
		case err := <-consoleErr:
			if err == io.EOF {
				return closeAndWait(conn, peerErr, nil)
			}
			return closeAndWait(conn, peerErr, fmt.Errorf("read console: %w", err))
		case <-interrupt:
			return closeAndWait(conn, peerErr, nil)
		case err := <-peerErr:
			closeErr := conn.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return fmt.Errorf("close connection: %w", closeErr)
			}
			return nil
		}
	}
}

func closeAndWait(conn net.Conn, peerErr <-chan error, reason error) error {
	err := conn.Close()
	if err != nil {
		return fmt.Errorf("close connection: %w", err)
	}
	err = <-peerErr
	if reason != nil {
		return reason
	}
	return err
}

func readConsole(lines chan<- string, errs chan<- error, done <-chan struct{}) {
	console := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, err := console.ReadString('\n')
		if err != nil {
			errs <- err
			return
		}
		select {
		case lines <- strings.TrimRight(line, "\r\n"):
		case <-done:
			return
		}
	}
}

func readPeer(conn net.Conn) error {
	peer := bufio.NewReader(conn)
	for {
		reply, err := peer.ReadString('\n')
		if err == io.EOF {
			fmt.Println("peer disconnected")
			return nil
		}
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		fmt.Println("peer:", strings.TrimRight(reply, "\r\n"))
	}
}
