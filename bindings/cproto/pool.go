package cproto

import (
	"math/rand"
	"sync/atomic"

	"github.com/restream/reindexer/v3/bindings"
)

type pool struct {
	conns            []*connection
	lbAlgorithm      bindings.LoadBalancingAlgorithm
	roundRobinParams struct {
		next uint64
	}
}

func (p *pool) GetConnection() *connection {
	switch p.lbAlgorithm {
	case bindings.LBRandom:
		return p.lbRandom()

	case bindings.LBPowerOfTwoChoices:
		return p.lbPowerOfTwoChoices()
	}

	return p.lbRoundRobin()
}

// Load balance connections in round-robin fashion
func (p *pool) lbRoundRobin() *connection {
	nextP := &p.roundRobinParams.next
	id := atomic.AddUint64(nextP, 1)

	for id >= uint64(len(p.conns)) {
		if atomic.CompareAndSwapUint64(nextP, id, 0) {
			id = 0
		} else {
			id = atomic.AddUint64(nextP, 1)
		}
	}

	return p.conns[id]
}

// Load balance connections randomly
func (p *pool) lbRandom() *connection {
	id := rand.Intn(len(p.conns))

	return p.conns[id]
}

// Load balance connections using "Power of Two Choices" algorithm.
// See also: https://www.nginx.com/blog/nginx-power-of-two-choices-load-balancing-algorithm/
func (p *pool) lbPowerOfTwoChoices() *connection {
	id1 := rand.Intn(len(p.conns))
	conn1 := p.conns[id1]
	conn1QueueUsage := cap(conn1.seqs) - len(conn1.seqs)

	id2 := rand.Intn(len(p.conns))
	conn2 := p.conns[id2]
	conn2QueueUsage := cap(conn2.seqs) - len(conn2.seqs)

	if conn2QueueUsage < conn1QueueUsage {
		return conn2
	}

	return conn1
}
