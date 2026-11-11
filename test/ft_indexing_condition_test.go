package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

type ftIndexingConditionItem struct {
	ID          int      `json:"id" reindex:"id,,pk"`
	Type        string   `json:"type" reindex:"type,hash"`
	Title       string   `json:"title" reindex:"title,text"`
	Notes       string   `json:"notes" reindex:"notes,text,sparse"`
	Description string   `json:"description" reindex:"description,-"`
	_           struct{} `reindex:"title+description=search,text,composite"`
}

const testFtIndexingConditionNs = "ft_indexing_condition"

func TestFTIndexingConditionSurvivesOpenNamespace(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *reindexer.NamespaceOptions
	}{
		{"default", reindexer.DefaultNamespaceOptions()},
		{"drop_on_indexes_conflict", reindexer.DefaultNamespaceOptions().DropOnIndexesConflict()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const condition = "type = 'series'"
			storageDSN := "builtin://" + t.TempDir()
			items := []any{
				&ftIndexingConditionItem{ID: 1, Type: "series", Title: "token", Notes: "token", Description: "token"},
				&ftIndexingConditionItem{ID: 2, Type: "movie", Title: "token", Notes: "token", Description: "token"},
			}
			withDB := func(run func(*reindexer.Reindexer)) {
				db, err := reindexer.NewReindex(storageDSN)
				require.NoError(t, err)
				defer db.Close()
				run(db)
			}
			check := func(db *reindexer.Reindexer) {
				description, err := db.DescribeNamespace(testFtIndexingConditionNs)
				require.NoError(t, err)
				fulltextIndexes := 0
				for _, index := range description.Indexes {
					if index.IndexType != "text" {
						continue
					}
					fulltextIndexes++
					require.Equal(t, condition, index.IndexingCondition, index.Name)
					found, err := db.Query(testFtIndexingConditionNs).Match(index.Name, "token").Exec().FetchAll()
					require.NoError(t, err)
					require.Equal(t, items[:1], found, index.Name)
				}
				require.Equal(t, 3, fulltextIndexes)
				stored, err := db.Query(testFtIndexingConditionNs).Sort("id", false).Exec().FetchAll()
				require.NoError(t, err)
				require.Equal(t, items, stored)
			}

			withDB(func(db *reindexer.Reindexer) {
				require.NoError(t, db.OpenNamespace(testFtIndexingConditionNs, tc.opts, ftIndexingConditionItem{}))
				description, err := db.DescribeNamespace(testFtIndexingConditionNs)
				require.NoError(t, err)
				for _, index := range description.Indexes {
					if index.IndexType == "text" {
						index.IndexingCondition = condition
						require.NoError(t, db.UpdateIndex(testFtIndexingConditionNs, index.IndexDef))
					}
				}
				for _, item := range items {
					require.NoError(t, db.Upsert(testFtIndexingConditionNs, item))
				}
				check(db)
				require.NoError(t, db.OpenNamespace(testFtIndexingConditionNs, tc.opts, ftIndexingConditionItem{}))
				check(db)
			})

			withDB(func(db *reindexer.Reindexer) {
				require.NoError(t, db.OpenNamespace(testFtIndexingConditionNs, tc.opts, ftIndexingConditionItem{}))
				check(db)
			})
		})
	}
}
