package events

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

type EventsStream struct {
	id           int
	err          error
	owner        *EventsHandler
	stream       chan *Event
	done         chan struct{}
	hasBgRoutine bool
	mtx          sync.Mutex
}

// Error - return current stream error
func (es *EventsStream) Error() error {
	es.mtx.Lock()
	defer es.mtx.Unlock()
	return es.err
}

// Chan - returns events channel. It will be closed in case of errors, do not close it from the user's code
func (es *EventsStream) Chan() chan *Event {
	return es.stream
}

// Close must be called to finalize stream correctly
func (es *EventsStream) Close(ctx context.Context) error {
	return es.setError(ctx, errors.New("stream was finalized"))
}

// internal finalizer (just in case)
func (es *EventsStream) closeBGCtx() error {
	return es.setError(context.Background(), errors.New("stream was finalized"))
}

func (es *EventsStream) runBgRoutine(owner *EventsHandler) error {
	if owner == nil {
		return errors.New("unable to run background routine for the events stream without owner")
	}

	es.mtx.Lock()
	defer es.mtx.Unlock()

	if es.err != nil {
		return fmt.Errorf("unable to run background routine for the invalid events stream: %w", es.err)
	}
	if es.hasBgRoutine {
		return errors.New("unable to run background routine for the events stream more than once")
	}
	es.owner = owner
	es.hasBgRoutine = true
	es.stream = make(chan *Event)
	go es.bgRoutine()

	return nil
}

func (es *EventsStream) initWithClosedChannel() {
	es.mtx.Lock()
	defer es.mtx.Unlock()

	es.stream = make(chan *Event)
	close(es.stream)
}

func (es *EventsStream) setError(ctx context.Context, err error) error {
	if err == nil {
		return errors.New("error can not be nil in EventsStream.setError")
	}
	es.mtx.Lock()
	defer es.mtx.Unlock()

	if es.err == nil {
		es.err = err
		runtime.SetFinalizer(es, nil)
		close(es.done)
		if es.owner != nil {
			return es.owner.removeStream(ctx, es)
		}
	}
	return nil
}

func (es *EventsStream) bgRoutine() {
	defer func() {
		if rec := recover(); rec != nil {
			var err string
			switch x := rec.(type) {
			case error:
				err = x.Error()
			case string:
				err = x
			default:
				err = "unknown panic type"
			}
			es.setError(context.TODO(), fmt.Errorf("events stream: background routine was terminated: %s", err))
		}
	}()
	defer close(es.stream)
	for {
		events, err := es.owner.readEvents(es)
		if err != nil {
			es.setError(context.TODO(), err)
			return
		}
		for i := 0; i < len(events); i++ {
			if val := events[i].streamsMask & (1 << es.id); val > 0 {
				if events[i].err == nil {
					select {
					case es.stream <- &events[i].ev:
					case <-es.done:
						return
					}
				} else {
					es.setError(context.TODO(), events[i].err)
					return
				}
			}
		}
	}
}
