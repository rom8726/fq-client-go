//nolint:dupl // it's ok
package fq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	CommandIncr    = "INCR"
	CommandGet     = "GET"
	CommandDel     = "DEL"
	CommandMDel    = "MDEL"
	CommandMsgSize = "MSGSIZE"
)

type CappingKey struct {
	Key     string
	Capping uint32
}

type Client struct {
	pool *ConnectionPool
}

func New(address string, idleTimeout time.Duration, poolSize int) (*Client, error) {
	newConnFn := func() (*TCPClient, error) {
		return NewTCPClient(address, 4096, idleTimeout)
	}

	pool := NewConnectionPool(poolSize, newConnFn)

	conns := make([]*TCPClient, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		c, err := pool.GetConnection()
		if err != nil {
			return nil, err
		}

		conns = append(conns, c)
	}

	for _, conn := range conns {
		pool.ReleaseConnection(conn)
	}

	return &Client{pool: pool}, nil
}

func (c *Client) Close() {
	c.pool.Close()
}

func (c *Client) Incr(ctx context.Context, key CappingKey) (uint64, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandIncr, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return 0, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Get(ctx context.Context, key CappingKey) (uint64, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandGet, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return 0, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Del(ctx context.Context, key CappingKey) (bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandDel, key)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return false, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return false, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return false, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		boolResult := result.value == 1

		return boolResult, nil
	case ResponseStatusError:
		return false, result.err
	default:
		return false, ErrUnknownRespStatus
	}
}

func (c *Client) MDel(ctx context.Context, keys []CappingKey) ([]bool, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeMultiCommand(buf, CommandMDel, keys)

	conn, err := c.pool.GetConnection()
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}

	defer c.pool.ReleaseConnection(conn)

	resp, err := sendWithReconnect(ctx, conn, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	result, err := parseMultiResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return valuesToBools(result.values), nil
	case ResponseStatusError:
		return nil, result.err
	default:
		return nil, ErrUnknownRespStatus
	}
}

func writeCommand(buf *bytes.Buffer, command string, key CappingKey) {
	cappingStr := strconv.FormatUint(uint64(key.Capping), 10)

	buf.WriteString(command)
	buf.WriteByte(' ')
	buf.WriteString(key.Key)
	buf.WriteByte(' ')
	buf.WriteString(cappingStr)
}

func writeMultiCommand(buf *bytes.Buffer, command string, keys []CappingKey) {
	buf.WriteString(command)
	buf.WriteByte(' ')

	for i, key := range keys {
		cappingStr := strconv.FormatUint(uint64(key.Capping), 10)

		buf.WriteString(key.Key)
		buf.WriteByte(' ')
		buf.WriteString(cappingStr)

		if i < len(keys)-1 {
			buf.WriteByte(' ')
		}
	}
}

func valuesToBools(values []uint64) []bool {
	bools := make([]bool, len(values))
	for i, value := range values {
		bools[i] = value == 1
	}

	return bools
}

func sendWithReconnect(ctx context.Context, conn *TCPClient, data []byte) ([]byte, error) {
	resp, err := conn.Send(ctx, data)
	if err != nil {
		if errors.Is(err, ErrConnClosed) {
			if err := conn.Reconnect(); err != nil {
				return nil, fmt.Errorf("reconnect: %w", err)
			}

			return conn.Send(ctx, data)
		}

		return nil, err
	}

	return resp, nil
}
