package rwc

import (
	"io"

	"github.com/gorilla/websocket"
)

// ReadWriteCloser adapts a websocket.Conn to an io.ReadWriteCloser for jrpc2.
type ReadWriteCloser struct {
	ws *websocket.Conn
	r  io.Reader
	w  io.WriteCloser
}

// New wraps a websocket connection as a ReadWriteCloser.
func New(ws *websocket.Conn) *ReadWriteCloser {
	return &ReadWriteCloser{ws: ws}
}

func (c *ReadWriteCloser) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			_, r, err := c.ws.NextReader()
			if err != nil {
				return 0, err
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *ReadWriteCloser) Write(p []byte) (int, error) {
	w, err := c.ws.NextWriter(websocket.TextMessage)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(p)
	if err != nil {
		return n, err
	}
	return n, w.Close()
}

func (c *ReadWriteCloser) Close() error {
	return c.ws.Close()
}
