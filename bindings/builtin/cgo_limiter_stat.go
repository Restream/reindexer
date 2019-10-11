package builtin

import (
	"context"
	"sync/atomic"
	"time"
)

type cgoLimiterStat struct {
	ctx         context.Context
	cancel      context.CancelFunc
	binding     *Builtin
	lastMinAvg  int32
	usagesTotal int
	usagesCnt   int
}

func newCgoLimiterStat(binding *Builtin) *cgoLimiterStat {
	s := new(cgoLimiterStat)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.binding = binding
	go s.loop()
	return s
}

func (s *cgoLimiterStat) loop() {
	tm := time.NewTicker(100 * time.Millisecond)
	ts := time.NewTicker(time.Minute)
	for {
		select {
		case <-tm.C:
			s.usagesCnt++
			s.usagesTotal += len(s.binding.cgoLimiter)
		case <-ts.C:
			if s.usagesCnt > 0 {
				atomic.StoreInt32(&s.lastMinAvg, int32(s.usagesTotal/s.usagesCnt))
				s.usagesTotal = 0
				s.usagesCnt = 0
			}
		case <-s.ctx.Done():
			atomic.StoreInt32(&s.lastMinAvg, 0)
			return
		}
	}
}

func (s *cgoLimiterStat) LastMinAvg() int {
	return int(atomic.LoadInt32(&s.lastMinAvg))
}

func (s *cgoLimiterStat) Stop() {
	s.cancel()
}
