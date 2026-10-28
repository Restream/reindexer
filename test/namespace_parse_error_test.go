package reindexer

import (
	"testing"

	rx "github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type badCollateNamespaceItem struct {
	ID   int    `json:"id" reindex:"id,,pk"`
	Name string `json:"name" reindex:"name,tree,collate_ascii,collate_utf8"`
}

type badExpireAfterNamespaceItem struct {
	ID   int   `json:"id" reindex:"id,,pk"`
	Date int64 `json:"date" reindex:"date,ttl,,expire_after=abc"`
}

func TestOpenNamespaceInvalidCollateReturnsError(t *testing.T) {
	err := DB.Reindexer.OpenNamespace("test_bad_collate_namespace", rx.DefaultNamespaceOptions(), badCollateNamespaceItem{})
	require.EqualError(t, err, "collate mode is already set to '1'. Misunderstanding 'collate_utf8'")
}

func TestOpenNamespaceInvalidExpireAfterReturnsError(t *testing.T) {
	err := DB.Reindexer.OpenNamespace("test_bad_expire_after_namespace", rx.DefaultNamespaceOptions(), badExpireAfterNamespaceItem{})
	require.EqualError(t, err, "'ExpireAfter' should be an integer value")
}
