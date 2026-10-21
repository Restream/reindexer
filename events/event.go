package events

import (
	"time"

	"github.com/restream/reindexer/v5/bindings"
)

type EventType int
type LsnT bindings.LsnT

func (lsn LsnT) IsEmpty() bool { return (*bindings.LsnT)(&lsn).IsEmpty() }

type Event struct {
	impl bindings.Event
}

// Type - event type
func (e *Event) Type() EventType {
	return EventType(e.impl.Type)
}

// NamespaceVersion - source namespace version
func (e *Event) NamespaceVersion() LsnT {
	return LsnT(e.impl.NSVersion)
}

// LSN - corresponding LSN in the namespace's WAL
func (e *Event) LSN() LsnT {
	return LsnT(e.impl.LSN)
}

// ShardID of the source node
func (e *Event) ShardID() int {
	return e.impl.ShardID
}

// ServerID of the source node
func (e *Event) ServerID() int {
	return e.impl.ServerID
}

// Database name from the source node
func (e *Event) Database() string {
	return e.impl.Database
}

// Namespace name
func (e *Event) Namespace() string {
	return e.impl.Namespace
}

// Timestamp of the event's creation
func (e *Event) Timestamp() time.Time {
	return e.impl.Timestamp
}

// Reindexer event types
const (
	// Data modification events
	EventTypeItemUpdate = EventType(bindings.EventTypeItemUpdate)
	EventTypeItemUpsert = EventType(bindings.EventTypeItemUpsert)
	EventTypeItemDelete = EventType(bindings.EventTypeItemDelete)
	EventTypeItemInsert = EventType(bindings.EventTypeItemInsert)
	EventTypeTruncate   = EventType(bindings.EventTypeTruncate)
	EventTypePutMeta    = EventType(bindings.EventTypePutMeta)
	EventTypeDeleteMeta = EventType(bindings.EventTypeDeleteMeta)
	// Index modification events
	EventTypeIndexAdd    = EventType(bindings.EventTypeIndexAdd)
	EventTypeIndexDrop   = EventType(bindings.EventTypeIndexDrop)
	EventTypeIndexUpdate = EventType(bindings.EventTypeIndexUpdate)
	// Transaction events
	EventTypeBeginTx       = EventType(bindings.EventTypeBeginTx)
	EventTypeCommitTx      = EventType(bindings.EventTypeCommitTx)
	EventTypeItemUpdateTx  = EventType(bindings.EventTypeItemUpdateTx)
	EventTypeItemUpsertTx  = EventType(bindings.EventTypeItemUpsertTx)
	EventTypeItemDeleteTx  = EventType(bindings.EventTypeItemDeleteTx)
	EventTypeItemInsertTx  = EventType(bindings.EventTypeItemInsertTx)
	EventTypePutMetaTx     = EventType(bindings.EventTypePutMetaTx)
	EventTypeUpdateQuery   = EventType(bindings.EventTypeUpdateQuery)
	EventTypeDeleteQuery   = EventType(bindings.EventTypeDeleteQuery)
	EventTypeUpdateQueryTx = EventType(bindings.EventTypeUpdateQueryTx)
	EventTypeDeleteQueryTx = EventType(bindings.EventTypeDeleteQueryTx)
	// Schema setting
	EventTypeSetSchema = EventType(bindings.EventTypeSetSchema)
	// Namespace operation events
	EventTypeAddNamespace    = EventType(bindings.EventTypeAddNamespace)
	EventTypeDropNamespace   = EventType(bindings.EventTypeDropNamespace)
	EventTypeCloseNamespace  = EventType(bindings.EventTypeCloseNamespace)
	EventTypeRenameNamespace = EventType(bindings.EventTypeRenameNamespace)
	// Replication sync events
	EventTypeNamespaceSync             = EventType(bindings.EventTypeResyncNamespaceGeneric)
	EventTypeNamespaceSyncOnLeaderInit = EventType(bindings.EventTypeResyncNamespaceLeaderInit)
	// Updates queue overflow event
	EventTypeUpdatesDrop = EventType(bindings.EventTypeResyncOnUpdatesDrop)
	// Internal system events
	EventTypeNone                    = EventType(bindings.EventTypeNone)
	EventTypeEmptyUpdate             = EventType(bindings.EventTypeEmptyUpdate)
	EventTypeNodeNetworkCheck        = EventType(bindings.EventTypeNodeNetworkCheck)
	EventTypeSetTagsMatcher          = EventType(bindings.EventTypeSetTagsMatcher)
	EventTypeSetTagsMatcherTx        = EventType(bindings.EventTypeSetTagsMatcherTx)
	EventTypeSaveShardingConfig      = EventType(bindings.EventTypeSaveShardingConfig)
	EventTypeApplyShardingConfig     = EventType(bindings.EventTypeApplyShardingConfig)
	EventTypeResetOldShardingConfig  = EventType(bindings.EventTypeResetOldShardingConfig)
	EventTypeResetCandidateConfig    = EventType(bindings.EventTypeResetCandidateConfig)
	EventTypeRollbackCandidateConfig = EventType(bindings.EventTypeRollbackCandidateConfig)
)
