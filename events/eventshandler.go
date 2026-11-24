package events

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/restream/reindexer/v5/cjson"
)

const (
	kEventsPerNode             = 1024
	kMaxSubsPerBinding         = 32
	kSubscriptionConfigVersion = 1
)

type eventWrapper struct {
	ev          Event
	err         error
	streamsMask uint32
}

type eventsQueueNode struct {
	events [kEventsPerNode]eventWrapper

	firstID int64
	cnt     int64
	next    *eventsQueueNode
	prev    *eventsQueueNode
	refs    int32
}

type eventsQueue struct {
	tail *eventsQueueNode
}

type eventsStream struct {
	nextUpdateID int64
	node         *eventsQueueNode
	s            *EventsStream
	opts         *EventsStreamOptions
	removed      bool
}

type streamsArray struct {
	streams [kMaxSubsPerBinding]*eventsStream
	mtx     sync.RWMutex
	binding bindings.RawBinding
}

type EventsHandler struct {
	activeSubs  streamsArray
	subsCnt     int32
	nextEventID int64
	queue       eventsQueue
	mtx         sync.RWMutex

	waitersCnt int64
	cvMtx      sync.Mutex
	cv         *sync.Cond
}

func NewEventsHandler(owner bindings.RawBinding) *EventsHandler {
	eh := &EventsHandler{}
	eh.activeSubs.binding = owner
	eh.cv = sync.NewCond(&eh.cvMtx)
	return eh
}

func (eh *EventsHandler) CreateStream(ctx context.Context, opts *EventsStreamOptions) *EventsStream {
	es := eh.activeSubs.createStream(ctx, eh, opts)
	err := es.Error()
	if err == nil {
		err = es.runBgRoutine(eh)
	}
	if err != nil {
		es.setError(ctx, err)
		es.initWithClosedChannel()
	}

	return es
}

func (eh *EventsHandler) removeStream(ctx context.Context, stream *EventsStream) error {
	return eh.activeSubs.removeStream(ctx, eh, stream)
}

func addStreamToSubscriptionOpts(opts *bindings.SubscriptionOptions, id int, streamOpts *EventsStreamOptions) {
	newStream := bindings.EventsStreamOptionsByStream{
		StreamID: id,
		EventsStreamOptions: &bindings.EventsStreamOptions{
			Namespaces:          streamOpts.namespaces,
			WithConfigNamespace: streamOpts.withConfigNamespace,
			EventTypes:          make(bindings.EventTypesMap),
		},
	}
	for _, typ := range streamOpts.eventTypes {
		newStream.EventTypes[typ] = struct{}{}
	}

	opts.Streams = append(opts.Streams, newStream)
	if streamOpts.withDBName {
		opts.WithDBName = true
	}
	if streamOpts.withLSN {
		opts.WithLSN = true
	}
	if streamOpts.withServerID {
		opts.WithServerID = true
	}
	if streamOpts.withShardID {
		opts.WithShardID = true
	}
	if streamOpts.withTimestamp {
		opts.WithTimestamp = true
	}
	if streamOpts.withData {
		// TODO: No data yet. Will be implemented later
	}
}

func (subs *streamsArray) buildSubscriptionConfig(except int) *bindings.SubscriptionOptions {
	subscriptionOpts := &bindings.SubscriptionOptions{Version: kSubscriptionConfigVersion,
		Streams:  make([]bindings.EventsStreamOptionsByStream, 0, kMaxSubsPerBinding),
		DataType: bindings.SubscriptionDataTypeNone,
	}
	for i, s := range subs.streams {
		if s != nil && i != except {
			addStreamToSubscriptionOpts(subscriptionOpts, i, s.opts)
		}
	}
	return subscriptionOpts
}

func (subs *streamsArray) createStream(ctx context.Context, eh *EventsHandler, opts *EventsStreamOptions) *EventsStream {
	stream := &EventsStream{id: kMaxSubsPerBinding, done: make(chan struct{})}

	subs.mtx.Lock()
	defer subs.mtx.Unlock()

	stream.id = kMaxSubsPerBinding
	for i, s := range subs.streams {
		if s == nil {
			stream.id = i
			break
		}
	}
	if stream.id == kMaxSubsPerBinding {
		stream.setError(context.TODO(), fmt.Errorf("can not create more than %d events stream in the single binding", kMaxSubsPerBinding))
		return stream
	}
	subOpts := subs.buildSubscriptionConfig(kMaxSubsPerBinding)
	addStreamToSubscriptionOpts(subOpts, stream.id, opts)
	err := subs.binding.Subscribe(ctx, subOpts)
	if err != nil {
		stream.setError(context.TODO(), err)
		return stream
	}

	eh.mtx.Lock()
	defer eh.mtx.Unlock()
	nextNode := eh.queue.tail
	if nextNode != nil && nextNode.cnt == kEventsPerNode {
		nextNode = nil
	}
	subs.streams[stream.id] = &eventsStream{nextUpdateID: eh.nextEventID, node: nextNode, s: stream, opts: opts, removed: false}
	if eh.nextEventID%kEventsPerNode != 0 {
		eh.queue.tail.refs += 1
	}
	eh.subsCnt += 1

	runtime.SetFinalizer(stream, (*EventsStream).closeBGCtx)
	return stream
}

func (subs *streamsArray) removeStream(ctx context.Context, eh *EventsHandler, stream *EventsStream) error {
	if stream == nil {
		return errors.New("unable to remove nil stream")
	}
	if stream.id < 0 || stream.id >= kMaxSubsPerBinding {
		return fmt.Errorf("stream id must be in range [0, %d), but was %d", kMaxSubsPerBinding, stream.id)
	}

	subs.mtx.Lock()
	defer subs.mtx.Unlock()

	if s := subs.streams[stream.id]; s != nil {
		if s.s != stream {
			return fmt.Errorf("unexpected stream pointer for ID %d", stream.id)
		}
		var err error
		subOpts := subs.buildSubscriptionConfig(stream.id)
		if len(subOpts.Streams) == 0 {
			err = subs.binding.Unsubscribe(ctx)
		} else {
			err = subs.binding.Subscribe(ctx, subOpts)
		}

		eh.mtx.Lock()
		defer eh.mtx.Unlock()
		subs.streams[stream.id] = nil
		for s.node != nil {
			s.node = eh.queue.removeRef(s.node)
		}
		s.removed = true
		eh.subsCnt -= 1
		eh.cv.Broadcast()
		return err
	}
	return fmt.Errorf("events stream with id %d was not found", stream.id)
}

func (eh *EventsHandler) awaitEvent() {
	// Condvar does not work with shared mutex by itself
	eh.cv.L.Lock()
	defer eh.cv.L.Unlock()
	eh.mtx.RUnlock()
	eh.activeSubs.mtx.RUnlock()

	atomic.AddInt64(&eh.waitersCnt, 1)
	eh.cv.Wait()
	atomic.AddInt64(&eh.waitersCnt, -1)
}

func (eh *EventsHandler) readEvents(stream *EventsStream) ([]eventWrapper, error) {
	if stream == nil {
		return nil, errors.New("unable to get events for nil stream")
	}
	if stream.id < 0 || stream.id >= kMaxSubsPerBinding {
		return nil, fmt.Errorf("stream id must be in range [0, %d), but was %d", kMaxSubsPerBinding, stream.id)
	}

	eh.activeSubs.mtx.RLock()
	defer eh.activeSubs.mtx.RUnlock()
	eh.mtx.RLock()
	defer eh.mtx.RUnlock()

	if s := eh.activeSubs.streams[stream.id]; s != nil {
		if s.s != stream {
			return nil, fmt.Errorf("unexpected stream pointer for id %d", stream.id)
		}

		for s.nextUpdateID >= eh.nextEventID {
			eh.awaitEvent()
			// Relock
			eh.activeSubs.mtx.RLock()
			eh.mtx.RLock()
			if s.removed {
				return nil, errors.New("events stream was closed")
			}
		}
		if s.node == nil {
			s.node = eh.queue.tail
			for s.node != nil && s.node.firstID > s.nextUpdateID {
				s.node = s.node.prev
			}
		}
		if s.node == nil {
			return nil, fmt.Errorf("invalid next event id in the event stream. EventHandler's next ID: %d, streams next ID: %d", eh.nextEventID, s.nextUpdateID)
		}

		slice := s.node.events[s.nextUpdateID-s.node.firstID : s.node.cnt]
		sliceCopy := make([]eventWrapper, len(slice))
		copy(sliceCopy, slice)

		s.nextUpdateID += int64(len(slice))
		if s.node.cnt == kEventsPerNode {
			s.node = eh.queue.removeRef(s.node)
		}

		return sliceCopy, nil
	}
	return nil, errors.New("events stream not found")
}

func (q *eventsQueue) removeRef(node *eventsQueueNode) *eventsQueueNode {
	refs := atomic.AddInt32(&node.refs, -1)
	node = node.next
	if refs == 0 {
		if node != nil {
			node.prev = nil
		}
	}
	return node
}

// This method is executed in the single goroutine
func (eh *EventsHandler) OnEvent(e bindings.RawBuffer) (ret error) {
	defer func() {
		if rec := recover(); rec != nil {
			var err error
			switch x := rec.(type) {
			case error:
				err = x
			default:
				err = errors.New("unknown panic during event handling")
			}
			eh.OnError(err)
			ret = err
		}
	}()
	defer e.Free()

	// TODO: Implement size limitations #1725

	notify, ret := eh.pushEventToQueue(nil, e)
	if ret != nil {
		return
	}
	if notify {
		eh.cv.Broadcast()
	}
	return
}

func (eh *EventsHandler) OnError(e error) error {
	notify, err := eh.pushEventToQueue(e, nil)
	if err != nil {
		return err
	}
	if notify {
		eh.cv.Broadcast()
	}
	return nil
}

type EventSerializationOpts struct {
	IsWithDBName    bool
	IsWithLSN       bool
	IsWithShardID   bool
	IsWithServerID  bool
	IsWithTimestamp bool
	IsWithData      bool
}

func parseEventSerializationOpts(data uint64) EventSerializationOpts {
	const (
		kEventDataFormatMask                 = 0x7
		kEventsSerializationOptWithDbName    = 1 << 3
		kEventsSerializationOptWithShardID   = 1 << 4
		kEventsSerializationOptWithLSN       = 1 << 5
		kEventsSerializationOptWithServerID  = 1 << 6
		kEventsSerializationOptWithTimestamp = 1 << 7
	)
	opts := EventSerializationOpts{}
	opts.IsWithData = (data & kEventDataFormatMask) != 0
	opts.IsWithDBName = (data & kEventsSerializationOptWithDbName) != 0
	opts.IsWithShardID = (data & kEventsSerializationOptWithShardID) != 0
	opts.IsWithLSN = (data & kEventsSerializationOptWithLSN) != 0
	opts.IsWithServerID = (data & kEventsSerializationOptWithServerID) != 0
	opts.IsWithTimestamp = (data & kEventsSerializationOptWithTimestamp) != 0

	return opts
}

func parseEvent(rbuf bindings.RawBuffer, event *eventWrapper) error {
	const kEventSerializationFormatVersion = 1
	ser := cjson.NewSerializer(rbuf.GetBuf())
	if version := ser.GetVarUInt(); version != kEventSerializationFormatVersion {
		return fmt.Errorf("unknown event format version: %d. %d version was expected", version, kEventSerializationFormatVersion)
	}
	serOpts := parseEventSerializationOpts(ser.GetVarUInt())
	if serOpts.IsWithData {
		return fmt.Errorf("unable to parse event: it contains unsupported data format")
	}
	event.streamsMask = ser.GetUInt32()
	if serOpts.IsWithDBName {
		event.ev.impl.Database = ser.GetVString()
	}
	event.ev.impl.Namespace = ser.GetVString()
	event.ev.impl.Type = int(ser.GetVarInt())
	if serOpts.IsWithLSN {
		event.ev.impl.NSVersion = bindings.CreateLSNFromInt64(ser.GetVarInt())
		event.ev.impl.LSN = bindings.CreateLSNFromInt64(ser.GetVarInt())
	} else {
		event.ev.impl.NSVersion = bindings.CreateEmptyLSN()
		event.ev.impl.LSN = event.ev.impl.NSVersion
	}
	if serOpts.IsWithServerID {
		event.ev.impl.ServerID = int(ser.GetVarInt())
	}
	if serOpts.IsWithShardID {
		event.ev.impl.ShardID = int(ser.GetVarInt())
	}
	if serOpts.IsWithTimestamp {
		unixTS := ser.GetVarInt()
		event.ev.impl.Timestamp = time.Unix(unixTS/1000000000, unixTS%1000000000)
	}

	return nil
}

func (eh *EventsHandler) pushEventToQueue(e error, rbuf bindings.RawBuffer) (notify bool, err error) {
	if eh.nextEventID%kEventsPerNode != 0 {
		// Tail can not be nil here
		node := eh.queue.tail
		node.events[node.cnt] = eventWrapper{}
		if e != nil {
			node.events[node.cnt].err = e
			node.events[node.cnt].streamsMask = 0xFFFFFFFF
		} else {
			err = parseEvent(rbuf, &node.events[node.cnt])
			if err != nil {
				return
			}
		}

		eh.mtx.Lock()
		defer eh.mtx.Unlock()
		if eh.subsCnt != 0 {
			notify = atomic.LoadInt64(&eh.waitersCnt) > 0
			node.cnt += 1
			eh.nextEventID += 1
		}
	} else {
		// Create new node
		node := &eventsQueueNode{firstID: eh.nextEventID, cnt: 1, next: nil, prev: nil, refs: 0}
		if e != nil {
			node.events[0].err = e
			node.events[0].streamsMask = 0xFFFFFFFF
		} else {
			err = parseEvent(rbuf, &node.events[0])
			if err != nil {
				return
			}
		}

		eh.mtx.Lock()
		defer eh.mtx.Unlock()
		if eh.subsCnt != 0 {
			notify = atomic.LoadInt64(&eh.waitersCnt) > 0
			node.refs = eh.subsCnt
			if eh.queue.tail != nil {
				if eh.queue.tail.refs != 0 {
					eh.queue.tail.next = node
					node.prev = eh.queue.tail
				}
			}
			eh.queue.tail = node
			eh.nextEventID += 1
		}
	}
	return
}
