package main

import "io"
 
// chanReader receives on the channel when its
// Read method is called. Extra data received is
// buffered until read.
type chanReader struct {
	buf []byte
	c   <-chan []byte
}

func newChanReader(c <-chan []byte) *chanReader {
	return &chanReader{c: c}
}

func (r *chanReader) Read(buf []byte) (int, error) {
	for len(r.buf) == 0 {
		var ok bool
		r.buf, ok = <-r.c
		if !ok {
			return 0, io.EOF
		}
	}
	n := copy(buf, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// chanWriter writes on the channel when its
// Write method is called.
type chanWriter struct {
	c chan<- []byte
}

func newChanWriter(c chan<- []byte) *chanWriter {
	return &chanWriter{c: c}
}
func (w *chanWriter) Write(buf []byte) (n int, err error) {
	b := make([]byte, len(buf))
	copy(b, buf)
	w.c <- b
	return len(buf), nil
}

