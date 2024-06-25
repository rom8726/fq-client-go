package fq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var ErrConnPoolClosed = errors.New("connection pool closed")

type ConnectionPool struct {
	connections chan *TCPClient
	wg          sync.WaitGroup
	closed      atomic.Bool

	newConn func() (*TCPClient, error)
}

func NewConnectionPool(size int, newConn func() (*TCPClient, error)) *ConnectionPool {
	return &ConnectionPool{
		connections: make(chan *TCPClient, size),
		newConn:     newConn,
	}
}

func (cp *ConnectionPool) GetConnection() (*TCPClient, error) {
	if cp.closed.Load() {
		return nil, ErrConnPoolClosed
	}

	var conn *TCPClient

	select {
	case conn = <-cp.connections:
		cp.wg.Add(1)
	default:
		cp.wg.Add(1)

		var err error
		conn, err = cp.newConn()
		if err != nil {
			cp.wg.Done()

			return nil, fmt.Errorf("new connection: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		err = getAndSetMsgSize(ctx, conn)
		if err != nil {
			cp.wg.Done()

			return nil, fmt.Errorf("get and set msg size: %w", err)
		}
	}

	return conn, nil
}

func (cp *ConnectionPool) ReleaseConnection(conn *TCPClient) {
	if cp.closed.Load() {
		return
	}

	cp.connections <- conn
	cp.wg.Done()
}

func (cp *ConnectionPool) Close() {
	cp.wg.Wait()
	cp.closed.Store(true)
	close(cp.connections)

	for conn := range cp.connections {
		_ = conn.Close()
	}
}
