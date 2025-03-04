package events

import "github.com/restream/reindexer/v5/bindings"

type EventsStreamOptions struct {
	eventTypes          []string
	namespaces          []bindings.EventsNamespaceFilters
	withConfigNamespace bool
	withData            bool
	withDBName          bool
	withServerID        bool
	withShardID         bool
	withLSN             bool
	withTimestamp       bool
}

// DefaultSubscriptionOptions return default subscription opts
func DefaultEventsStreamOptions() *EventsStreamOptions {
	return &EventsStreamOptions{
		eventTypes:          make([]string, 0),
		namespaces:          make([]bindings.EventsNamespaceFilters, 0),
		withConfigNamespace: false,
		withData:            false, // In the current version data transmission is not implemented
		withDBName:          false,
		withServerID:        false,
		withShardID:         false,
		withLSN:             false,
		withTimestamp:       false,
	}
}

// Set target subscription namespaces to recieve events from
func (opts *EventsStreamOptions) WithNamespacesList(namespaces ...string) *EventsStreamOptions {
	opts.namespaces = make([]bindings.EventsNamespaceFilters, len(namespaces))
	for i, ns := range namespaces {
		opts.namespaces[i].Name = ns
	}
	return opts
}

// Enable events from #config-namespace
func (opts *EventsStreamOptions) WithConfigNamespace() *EventsStreamOptions {
	opts.withConfigNamespace = true
	return opts
}

// Enable documents modification events (except transaction records)
func (opts *EventsStreamOptions) WithDocModifyEvents() *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, getDocModifyEvents()...)
	return opts
}

// Enable full transactions events set (beginTx, all internal tx records and commit)
func (opts *EventsStreamOptions) WithAllTransactionEvents() *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, getAllTxEvents()...)
	return opts
}

// Enable transactions commit events
func (opts *EventsStreamOptions) WithTransactionCommitEvents() *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, getCommitTxEvents()...)
	return opts
}

// Enable index modification events
func (opts *EventsStreamOptions) WithIndexModifyEvents() *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, getIndexModifyEvents()...)
	return opts
}

// Enable namespaces modification events (add, drop, rename)
func (opts *EventsStreamOptions) WithNamespaceOperationEvents() *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, getNamespaceOperationEvents()...)
	return opts
}

// Add custom events set
func (opts *EventsStreamOptions) WithEvents(types ...EventType) *EventsStreamOptions {
	opts.eventTypes = append(opts.eventTypes, eventTypesToStr(types)...)
	return opts
}

// Enable all possible events
func (opts *EventsStreamOptions) WithAnyEvents() *EventsStreamOptions {
	opts.eventTypes = make([]string, 0)
	return opts
}

// If set, each event will contain record's LSN
func (opts *EventsStreamOptions) WithLSN() *EventsStreamOptions {
	opts.withLSN = true
	return opts
}

// If set, each event will contain shard ID of the emmiter node
func (opts *EventsStreamOptions) WithShardID() *EventsStreamOptions {
	opts.withShardID = true
	return opts
}

// If set, each event will contain server ID of the emmiter node
func (opts *EventsStreamOptions) WithServerID() *EventsStreamOptions {
	opts.withServerID = true
	return opts
}

// If set, each event will contain creation timestamp
func (opts *EventsStreamOptions) WithTimestamp() *EventsStreamOptions {
	opts.withTimestamp = true
	return opts
}

// If set, each event will contain source database name
// This option is for the future development, when cluster subscription will be implemented
func (opts *EventsStreamOptions) WithDBName() *EventsStreamOptions {
	opts.withDBName = true
	return opts
}

func getInternalReplicationEvents() []EventType {
	return []EventType{EventTypeNamespaceSync, EventTypeNamespaceSyncOnLeaderInit, EventTypeUpdatesDrop}
}

func getDocModifyEvents() []string {
	return eventTypesToStr(append([]EventType{EventTypeItemUpdate, EventTypeItemUpsert, EventTypeItemDelete, EventTypeItemInsert,
		EventTypePutMeta, EventTypeDeleteMeta, EventTypeUpdateQuery, EventTypeDeleteQuery, EventTypeTruncate}, getInternalReplicationEvents()...))
}

func getAllTxEvents() []string {
	return eventTypesToStr(append([]EventType{EventTypeItemUpdateTx, EventTypeItemUpsertTx, EventTypeItemDeleteTx, EventTypeItemInsertTx,
		EventTypePutMetaTx, EventTypeUpdateQueryTx, EventTypeDeleteQueryTx, EventTypeBeginTx, EventTypeCommitTx}, getInternalReplicationEvents()...))
}

func getCommitTxEvents() []string {
	return eventTypesToStr(append([]EventType{EventTypeCommitTx}, getInternalReplicationEvents()...))
}

func getNamespaceOperationEvents() []string {
	return eventTypesToStr(append([]EventType{EventTypeAddNamespace, EventTypeDropNamespace, EventTypeRenameNamespace, EventTypeCloseNamespace},
		getInternalReplicationEvents()...))
}

func getIndexModifyEvents() []string {
	return eventTypesToStr(append([]EventType{EventTypeIndexAdd, EventTypeIndexDrop, EventTypeIndexUpdate}, getInternalReplicationEvents()...))
}

func eventTypeToStr(typ EventType) string {
	switch typ {
	case EventTypeItemUpdate:
		return "ItemUpdate"
	case EventTypeItemUpsert:
		return "ItemUpsert"
	case EventTypeItemDelete:
		return "ItemDelete"
	case EventTypeItemInsert:
		return "ItemInsert"
	case EventTypeTruncate:
		return "Truncate"
	case EventTypePutMeta:
		return "PutMeta"
	case EventTypeIndexAdd:
		return "IndexAdd"
	case EventTypeIndexDrop:
		return "IndexDrop"
	case EventTypeIndexUpdate:
		return "IndexUpdate"
	case EventTypeBeginTx:
		return "BeginTx"
	case EventTypeCommitTx:
		return "CommitTx"
	case EventTypeItemUpdateTx:
		return "ItemUpdateTx"
	case EventTypeItemUpsertTx:
		return "ItemUpsertTx"
	case EventTypeItemDeleteTx:
		return "ItemDeleteTx"
	case EventTypeItemInsertTx:
		return "ItemInsertTx"
	case EventTypePutMetaTx:
		return "PutMetaTx"
	case EventTypeUpdateQuery:
		return "UpdateQuery"
	case EventTypeDeleteQuery:
		return "DeleteQuery"
	case EventTypeUpdateQueryTx:
		return "UpdateQueryTx"
	case EventTypeDeleteQueryTx:
		return "DeleteQueryTx"
	case EventTypeSetSchema:
		return "SetSchema"
	case EventTypeAddNamespace:
		return "AddNamespace"
	case EventTypeDropNamespace:
		return "DropNamespace"
	case EventTypeCloseNamespace:
		return "CloseNamespace"
	case EventTypeRenameNamespace:
		return "RenameNamespace"
	case EventTypeNamespaceSync:
		return "NamespaceSync"
	case EventTypeNamespaceSyncOnLeaderInit:
		return "NamespaceSyncOnLeaderInit"
	case EventTypeUpdatesDrop:
		return "UpdatesDrop"
	case EventTypeNone:
		return "None"
	case EventTypeEmptyUpdate:
		return "EmptyUpdate"
	case EventTypeNodeNetworkCheck:
		return "NodeNetworkCheck"
	case EventTypeSetTagsMatcher:
		return "SetTagsMatcher"
	case EventTypeSetTagsMatcherTx:
		return "SetTagsMatcherTx"
	case EventTypeSaveShardingConfig:
		return "SaveShardingConfig"
	case EventTypeApplyShardingConfig:
		return "ApplyShardingConfig"
	case EventTypeResetOldShardingConfig:
		return "ResetOldShardingConfig"
	case EventTypeResetCandidateConfig:
		return "ResetCandidateConfig"
	case EventTypeRollbackCandidateConfig:
		return "RollbackCandidateConfig"
	case EventTypeDeleteMeta:
		return "DeleteMeta"
	default:
		return "Unknown"
	}
}

func eventTypesToStr(types []EventType) []string {
	ret := make([]string, 0, len(types))
	for _, t := range types {
		ret = append(ret, eventTypeToStr(t))
	}
	return ret
}
