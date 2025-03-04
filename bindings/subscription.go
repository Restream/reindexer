package bindings

import (
	"bytes"
	"encoding/json"
	"time"
)

// Operation counter and server id
type LsnT struct {
	// Operation counter
	Counter int64 `json:"counter"`
	// Node identifier
	ServerId int `json:"server_id"`
}

const kLSNCounterBitsCount = 48
const kLSNCounterMask int64 = (int64(1) << kLSNCounterBitsCount) - int64(1)
const kLSNDigitCountMult int64 = 1000000000000000

func (lsn *LsnT) IsCompatibleWith(o LsnT) bool {
	return lsn.ServerId == o.ServerId
}

func (lsn *LsnT) IsNewerThen(o LsnT) bool {
	return lsn.Counter > o.Counter
}

func (lsn *LsnT) IsEmpty() bool { return lsn.Counter == kLSNDigitCountMult-1 }

func CreateLSNFromInt64(v int64) LsnT {
	if (v & kLSNCounterMask) == kLSNCounterMask {
		return LsnT{Counter: kLSNDigitCountMult - 1, ServerId: -1}
	}

	server := v / kLSNDigitCountMult
	return LsnT{Counter: v - server*kLSNDigitCountMult, ServerId: int(server)}
}

func CreateInt64FromLSN(v LsnT) int64 {
	return int64(v.ServerId)*kLSNDigitCountMult + v.Counter
}

func CreateEmptyLSN() LsnT {
	return LsnT{Counter: kLSNDigitCountMult - 1, ServerId: 0}
}

const (
	EventTypeNone                      = 0
	EventTypeItemUpdate                = 1
	EventTypeItemUpsert                = 2
	EventTypeItemDelete                = 3
	EventTypeItemInsert                = 4
	EventTypeItemUpdateTx              = 5
	EventTypeItemUpsertTx              = 6
	EventTypeItemDeleteTx              = 7
	EventTypeItemInsertTx              = 8
	EventTypeIndexAdd                  = 9
	EventTypeIndexDrop                 = 10
	EventTypeIndexUpdate               = 11
	EventTypePutMeta                   = 12
	EventTypePutMetaTx                 = 13
	EventTypeUpdateQuery               = 14
	EventTypeDeleteQuery               = 15
	EventTypeUpdateQueryTx             = 16
	EventTypeDeleteQueryTx             = 17
	EventTypeSetSchema                 = 18
	EventTypeTruncate                  = 19
	EventTypeBeginTx                   = 20
	EventTypeCommitTx                  = 21
	EventTypeAddNamespace              = 22
	EventTypeDropNamespace             = 23
	EventTypeCloseNamespace            = 24
	EventTypeRenameNamespace           = 25
	EventTypeResyncNamespaceGeneric    = 26
	EventTypeResyncNamespaceLeaderInit = 27
	EventTypeResyncOnUpdatesDrop       = 28
	EventTypeEmptyUpdate               = 29
	EventTypeNodeNetworkCheck          = 30
	EventTypeSetTagsMatcher            = 31
	EventTypeSetTagsMatcherTx          = 32
	EventTypeSaveShardingConfig        = 33
	EventTypeApplyShardingConfig       = 34
	EventTypeResetOldShardingConfig    = 35
	EventTypeResetCandidateConfig      = 36
	EventTypeRollbackCandidateConfig   = 37
	EventTypeDeleteMeta                = 38
)

// Events handler interface
type Event struct {
	Type      int
	NSVersion LsnT
	LSN       LsnT
	ShardID   int
	ServerID  int
	Database  string
	Namespace string
	Timestamp time.Time
}

const (
	SubscriptionTypeNamespaces = 0
	SubscriptionTypeDatabase   = 1
)

type EventsNamespaceFilters struct {
	Name string `json:"name"`
	// TODO: No filters yet
}

type EventsStreamOptions struct {
	Namespaces          []EventsNamespaceFilters `json:"namespaces"`
	WithConfigNamespace bool                     `json:"with_config_namespace"`
	EventTypes          EventTypesMap            `json:"event_types"`
}

type EventsStreamOptionsByStream struct {
	*EventsStreamOptions
	StreamID int `json:"id"`
}

const (
	SubscriptionDataTypeNone = "none"
)

type EventTypesMap map[string]struct{}

type SubscriptionOptions struct {
	Version          int                           `json:"version"`
	SubscriptionType int                           `json:"subscription_type"`
	Streams          []EventsStreamOptionsByStream `json:"streams"`
	WithDBName       bool                          `json:"with_db_name"`
	WithServerID     bool                          `json:"with_server_id"`
	WithShardID      bool                          `json:"with_shard_id"`
	WithLSN          bool                          `json:"with_lsn"`
	WithTimestamp    bool                          `json:"with_timestamp"`
	DataType         string                        `json:"data_type"`
}

func (em EventTypesMap) MarshalJSON() ([]byte, error) {
	out := bytes.NewBuffer(make([]byte, 0, 1024))
	out.WriteByte('[')
	i := 0
	for typ := range em {
		if i != 0 {
			out.WriteByte(',')
		}
		i++
		out.WriteByte('"')
		out.WriteString(typ)
		out.WriteByte('"')
	}
	out.WriteByte(']')
	return out.Bytes(), nil
}

func (em EventTypesMap) UnmarshalJSON(b []byte) error {
	em = make(EventTypesMap)
	var slice []string
	if err := json.Unmarshal(b, &slice); err != nil {
		return err
	}
	for _, typ := range slice {
		em[typ] = struct{}{}
	}
	return nil
}

type EventsHandler interface {
	OnEvent(RawBuffer) error
	OnError(error) error
}
