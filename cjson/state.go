package cjson

import (
	"reflect"
	"sync"
)

type StateData struct {
	tagsMatcher tagsMatcher
	payloadType payloadType
	structCache structCache
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

func (state *State) ReadPayloadType(s *Serializer) State {
	state.lock.Lock()
	defer state.lock.Unlock()
	stateToken := int32(s.GetVarUInt())
	version := int32(s.GetVarUInt())
	skip := state.Version >= version && state.StateToken == stateToken

	if !skip {
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

func (state *State) NewDecoder(item interface{}) Decoder {
	dec := Decoder{
		state: state,
	}

	dec.state.lock.RLock()
	if dec.state.structCache == nil {
		dec.state.lock.RUnlock()
		dec.state.lock.Lock()
		if dec.state.structCache == nil {
			dec.state.structCache = make(map[reflect.Type]*ctagsCache, 1)
		}
		dec.state.lock.Unlock()
		dec.state.lock.RLock()
	}

	if st, ok := dec.state.structCache[reflect.TypeOf(item)]; ok {
		dec.ctagsCache = st
	} else {
		cachePtr := &ctagsCache{}
		dec.state.lock.RUnlock()
		dec.state.lock.Lock()
		if st, ok := dec.state.structCache[reflect.TypeOf(item)]; ok {
			cachePtr = st
		} else {
			dec.state.structCache[reflect.TypeOf(item)] = cachePtr
		}
		dec.state.lock.Unlock()
		dec.state.lock.RLock()
		dec.ctagsCache = cachePtr
	}
	dec.state.lock.RUnlock()

	return dec
}

func (state *State) Reset() {
	state.lock.Lock()
	state.StateData = &StateData{Version: -1}
	state.lock.Unlock()
}
