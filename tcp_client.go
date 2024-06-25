package fq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var ErrConnClosed = errors.New("connection closed by server")

type TCPClient struct {
	connection     net.Conn
	address        string
	maxMessageSize int
	idleTimeout    time.Duration
	bufferPool     *bytesPool
}

func NewTCPClient(address string, maxMessageSize int, idleTimeout time.Duration) (*TCPClient, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &TCPClient{
		connection:     connection,
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		bufferPool:     newBytesPool(maxMessageSize),
	}, nil
}

func (c *TCPClient) Send(ctx context.Context, request []byte) ([]byte, error) {
	if len(request) > c.maxMessageSize {
		return nil, fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return nil, err
	}

	if _, err := c.connection.Write(request); err != nil {
		return nil, err
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	count, err := c.connection.Read(response)
	if err != nil {
		if err == io.EOF {
			return nil, ErrConnClosed
		}

		return nil, err
	}

	result := make([]byte, count)
	copy(result, response[:count])

	return result, nil
}

func (c *TCPClient) Close() error {
	return c.connection.Close()
}

func (c *TCPClient) SetMaxMessageSizeUnsafe(size int) {
	c.maxMessageSize = size
	c.bufferPool = newBytesPool(size)
}

func (c *TCPClient) Reconnect() error {
	_ = c.connection.Close()

	connection, err := net.Dial("tcp", c.address)
	if err != nil {
		return err
	}

	c.connection = connection

	return nil
}

func (c *TCPClient) deadline(ctx context.Context) time.Time {
	stdDeadline := time.Now().Add(c.idleTimeout)
	deadline, ok := ctx.Deadline()
	if ok {
		if stdDeadline.Before(deadline) {
			deadline = stdDeadline
		}
	} else {
		deadline = stdDeadline
	}

	return deadline
}
