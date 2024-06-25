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

	c := &TCPClient{
		connection:     connection,
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		bufferPool:     newBytesPool(maxMessageSize),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := c.getAndSetMsgSize(ctx); err != nil {
		return nil, fmt.Errorf("failed to set msg size: %w", err)
	}

	return c, nil
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
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
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

func (c *TCPClient) getAndSetMsgSize(ctx context.Context) error {
	sz, err := msgSize(ctx, c)
	if err != nil {
		return err
	}

	c.SetMaxMessageSizeUnsafe(sz)

	return nil
}

func msgSize(ctx context.Context, client *TCPClient) (int, error) {
	resp, err := client.Send(ctx, []byte(CommandMsgSize))
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return int(result.value), nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}
