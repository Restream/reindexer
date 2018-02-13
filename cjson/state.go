package cjson

import (
	"sync"
)

type State struct {
	tagsMatcher tagsMatcher
	payloadType payloadType
	lock        sync.RWMutex
	ctagsCache  ctagsCache
	ctagsWCache ctagsWCache
}

func NewState() *State {
	return &State{
		tagsMatcher: tagsMatcher{Version: -1},
	}
}

func (state *State) ReadPayloadType(s *Serializer) {
	state.lock.Lock()
	state.tagsMatcher.Read(s)
	state.payloadType.Read(s)
	state.ctagsCache.Reset()
	state.ctagsWCache.Reset()
	state.lock.Unlock()
}

func (state *State) PayloadTypeVersion() int32 {
	state.lock.RLock()
	version := state.tagsMatcher.Version
	state.lock.RUnlock()
	return version
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
