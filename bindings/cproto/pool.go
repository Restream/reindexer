package cproto

import (
	"math/rand"
	"sync/atomic"

	"github.com/restream/reindexer/v5/bindings"
)

type pool struct {
	sharedConns      []connection
	eventsConn       connection
	lbAlgorithm      bindings.LoadBalancingAlgorithm
	roundRobinParams struct {
		next uint64
	}
}

func (p *pool) GetConnection() connection {
	switch p.lbAlgorithm {
	case bindings.LBRandom:
		return p.lbRandom()

	case bindings.LBPowerOfTwoChoices:
		return p.lbPowerOfTwoChoices()
	}

	return p.lbRoundRobin()
}

// Load balance connections in round-robin fashion
func (p *pool) lbRoundRobin() connection {
	nextP := &p.roundRobinParams.next
	id := atomic.AddUint64(nextP, 1)

	for id >= uint64(len(p.sharedConns)) {
		if atomic.CompareAndSwapUint64(nextP, id, 0) {
			id = 0
		} else {
			id = atomic.AddUint64(nextP, 1)
		}
	}

	return p.sharedConns[id]
}

// Load balance connections randomly
func (p *pool) lbRandom() connection {
	id := rand.Intn(len(p.sharedConns))

	return p.sharedConns[id]
}

// Load balance connections using "Power of Two Choices" algorithm.
// See also: https://www.nginx.com/blog/nginx-power-of-two-choices-load-balancing-algorithm/
func (p *pool) lbPowerOfTwoChoices() connection {
	id1 := rand.Intn(len(p.sharedConns))
	conn1 := p.sharedConns[id1]
	conn1Seqs := conn1.getSeqs()
	conn1QueueUsage := cap(conn1Seqs) - len(conn1Seqs)

	id2 := rand.Intn(len(p.sharedConns))
	conn2 := p.sharedConns[id2]
	conn2Seqs := conn2.getSeqs()
	conn2QueueUsage := cap(conn2Seqs) - len(conn2Seqs)

	if conn2QueueUsage < conn1QueueUsage {
		return conn2
	}

	return conn1
}
