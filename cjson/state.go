package cjson

import (
	"reflect"
	"sync"
)

type StateData struct {
	tagsMatcher tagsMatcher
	payloadType payloadType
	structCache structCache
	sCacheLock  sync.RWMutex
	ctagsWCache ctagsWCache
	Version     int32
	StateToken  int32
}

type State struct {
	*StateData
	lock *sync.RWMutex
}

func NewState() State {
	return State{
		StateData: &StateData{Version: -1},
		lock:      &sync.RWMutex{},
	}
}

func (state *State) Copy() State {
	state.lock.Lock()
	defer state.lock.Unlock()
	return State{
		StateData: state.StateData,
		lock:      state.lock,
	}
}

func (state *State) ReadPayloadType(s *Serializer, loggerOwner LoggerOwner, ns string) State {
	state.lock.Lock()
	defer state.lock.Unlock()
	stateToken := int32(s.GetVarUInt())
	version := int32(s.GetVarUInt())
	skip := state.Version >= version && state.StateToken == stateToken

	if !skip {
		if loggerOwner != nil {
			if logger := loggerOwner.GetLogger(); logger != nil {
				logger.Printf(3, "rq: '%s' TagsMatcher was updated: { state_token: 0x%08X, version: %d } -> { state_token: 0x%08X, version: %d }",
					ns, state.StateToken, state.Version, uint32(stateToken), version)
			}
		}
		state.StateData = &StateData{Version: version, StateToken: stateToken}
	}
	state.tagsMatcher.Read(s, skip)
	state.payloadType.Read(s, skip)
	return State{StateData: state.StateData, lock: state.lock}
}

func (state *State) NewEncoder() Encoder {
	return Encoder{
		state: state,
	}
}

func (state *State) makeStructCache() {
	state.sCacheLock.Lock()
	defer state.sCacheLock.Unlock()
	if state.structCache == nil {
		state.structCache = make(map[reflect.Type]*ctagsCache, 1)
	}
}

func initCtagsCache(dec *Decoder, item interface{}) {
	cachePtr := &ctagsCache{missing: make(missingTagsCache)}

	dec.state.sCacheLock.Lock()
	defer dec.state.sCacheLock.Unlock()
	if st, ok := dec.state.structCache[reflect.TypeOf(item)]; ok {
		cachePtr = st
	} else {
		dec.state.structCache[reflect.TypeOf(item)] = cachePtr
	}
	dec.ctagsCache = cachePtr
}

func (state *State) NewDecoder(item interface{}, loggerOwner LoggerOwner) Decoder {
	dec := Decoder{
		state:       state,
		loggerOwner: loggerOwner,
	}

	dec.state.sCacheLock.RLock()
	if dec.state.structCache == nil {
		dec.state.sCacheLock.RUnlock()
		dec.state.makeStructCache()
		dec.state.sCacheLock.RLock()
	}

	if st, ok := dec.state.structCache[reflect.TypeOf(item)]; ok {
		dec.ctagsCache = st
		dec.state.sCacheLock.RUnlock()
	} else {
		dec.state.sCacheLock.RUnlock()
		initCtagsCache(&dec, item)
	}

	return dec
}

func (state *State) Reset() {
	state.lock.Lock()
	state.StateData = &StateData{Version: -1}
	state.lock.Unlock()
}
