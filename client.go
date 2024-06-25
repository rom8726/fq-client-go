//nolint:dupl // it's ok
package fq

import (
	"bytes"
	"context"
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
	client *TCPClient
}

func New(address string, idleTimeout time.Duration) (*Client, error) {
	client, err := NewTCPClient(address, 4096, idleTimeout)
	if err != nil {
		return nil, err
	}

	c := &Client{
		client: client,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	msgSize, err := c.msgSize(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get message size: %w", err)
	}

	c.client.SetMaxMessageSizeUnsafe(msgSize)

	return c, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Incr(ctx context.Context, key CappingKey) (uint64, error) {
	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	writeCommand(buf, CommandIncr, key)

	resp, err := c.client.Send(ctx, buf.Bytes())
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

	resp, err := c.client.Send(ctx, buf.Bytes())
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

	resp, err := c.client.Send(ctx, buf.Bytes())
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

	resp, err := c.client.Send(ctx, buf.Bytes())
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

func (c *Client) msgSize(ctx context.Context) (int, error) {
	resp, err := c.client.Send(ctx, []byte(CommandMsgSize))
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
