package cproto

import "fmt"

type buffer []byte

type cbbuf struct {
	buf  []buffer
	head int
	tail int
}

func newCBBuf(size int) cbbuf {
	return cbbuf{
		buf: make([]buffer, size, size),
	}
}

func (b *cbbuf) Tail(nread int) []buffer {
	if b.head == b.tail {
		return b.buf[b.tail:0]
	}
	if b.tail > b.head {
		return b.buf[b.tail:]
	}
	return b.buf[b.tail : b.head-b.tail]

}

var errNoRoom = fmt.Errorf("No room for buffer")

func (b *cbbuf) Put(buf buffer) error {
	if (b.head+1)%cap(b.buf) == b.tail {
		return errNoRoom
	}
	b.buf[b.head] = buf
	b.head = (b.head + 1) % cap(b.buf)
	return nil
}
