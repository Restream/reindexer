package cjson

import (
	"sync"
)

type StateData struct {
	tagsMatcher tagsMatcher
	payloadType payloadType
	ctagsCache  ctagsCache
	ctagsWCache ctagsWCache
	Version     int32
	CacheToken  int32
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
	cacheToken := int32(s.GetVarUInt())
	version := int32(s.GetVarUInt())
	skip := state.Version >= version && state.CacheToken == cacheToken

	if !skip {
		state.StateData = &StateData{Version: version, CacheToken: cacheToken}
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

func (state *State) NewDecoder() Decoder {
	return Decoder{
		state: state,
	}
}

func (state *State) Reset() {
	state.lock.Lock()
	state.StateData = &StateData{Version: -1}
	state.lock.Unlock()
}
