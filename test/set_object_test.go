package reindexer

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/restream/reindexer/v5"
	"github.com/stretchr/testify/require"
)

const (
	namespace = "services"
	elements  = 100
)

type MetricsInfo struct {
	ID      int      `json:"id" reindex:"id,tree"`
	Name    string   `json:"name" yaml:"name" reindex:"name,,pk" validate:"required"`
	Metrics *Metrics `json:"metrics,omitempty"`
}

type Metrics struct {
	MemInfo Info `json:"mem_info"`
	CPUInfo Info `json:"cpu_info"`
	FDsInfo Info `json:"fds_info"`
}

type Info struct {
	Used        uint64  `json:"used"`
	Total       uint64  `json:"total"`
	UsedPercent float64 `json:"used_percent"`
}

func init() {
	tnamespaces["test_set_object"] = MetricsInfo{}
}

func GetTupleDataSize(t *testing.T) int64 {
	stats, err := DB.GetNamespaceMemStat(namespace)
	require.NoError(t, err)
	require.Equal(t, 3, len(stats.Indexes))
	return stats.Indexes[0].DataSize
}

func Update(ctx context.Context, service *MetricsInfo) error {
	query := DB.WithContext(ctx).Query(namespace).
		WhereString("name", reindexer.EQ, service.Name).
		Set("name", service.Name)

	if service.Metrics != nil {
		query.SetObject("metrics.mem_info", service.Metrics.MemInfo).
			SetObject("metrics.cpu_info", service.Metrics.CPUInfo).
			SetObject("metrics.fds_info", service.Metrics.FDsInfo)
	} else {
		query.Drop("metrics")
	}

	_, err := query.Update().FetchOne()
	return err
}

func TestSetObject(t *testing.T) {
	err := DB.OpenNamespace(namespace, reindexer.DefaultNamespaceOptions(), MetricsInfo{})
	require.NoError(t, err)

	for i := 0; i < elements; i++ {
		require.NoError(t, DB.Upsert(namespace, randomItem(i)))
	}

	startTupleSize := GetTupleDataSize(t)

	for i := 0; i < 10000; i++ {
		id := rand.Intn(elements)
		err = Update(context.Background(), randomItem(id))
		require.NoError(t, err)
	}

	finalTupleSize := GetTupleDataSize(t)
	require.True(t, finalTupleSize <= startTupleSize)
}

func randomItem(id int) *MetricsInfo {
	return &MetricsInfo{
		ID:   id,
		Name: fmt.Sprintf("test_%d", id),
		Metrics: &Metrics{
			MemInfo: Info{
				Used:        uint64(rand.Int31()),
				Total:       uint64(rand.Int31()),
				UsedPercent: float64(rand.Int31()),
			},
			CPUInfo: Info{
				Used:        uint64(rand.Int31()),
				Total:       uint64(rand.Int31()),
				UsedPercent: float64(rand.Int31()),
			},
			FDsInfo: Info{
				Used:        uint64(rand.Int31()),
				Total:       uint64(rand.Int31()),
				UsedPercent: float64(rand.Int31()),
			},
		},
	}
}
