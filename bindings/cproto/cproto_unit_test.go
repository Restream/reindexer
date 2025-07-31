//go:build unittest
// +build unittest

package cproto

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/restream/reindexer/v5/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	*NetCProto
	mockConns       []*MockConnection
	mockConnFactory *MockConnFactory
}

var ctx = context.Background()

var dsns = []url.URL{
	{Host: "127.0.0.1:6531", Scheme: "cproto", Path: "db"},
	{Host: "127.0.0.1:6532", Scheme: "cproto", Path: "db"},
	{Host: "127.0.0.1:6533", Scheme: "cproto", Path: "db"},
}

func newFixture(t *testing.T) *fixture {
	mockConnFactory := NewMockConnFactory(t)

	fx := &fixture{
		NetCProto: &NetCProto{
			dsn: dsn{
				connFactory:       mockConnFactory,
				allowUnknownNodes: true,
			},
			pool: pool{},
		},
		mockConnFactory: mockConnFactory,
	}
	return fx
}

func (fx *fixture) addDSNs(t *testing.T, dsns []url.URL) {
	for _, u := range dsns {
		fx.dsn.urls = append(fx.dsn.urls, u)
		mock := NewMockConnection(t)
		fx.pool.sharedConns = append(fx.pool.sharedConns, mock)
		fx.mockConns = append(fx.mockConns, mock)
	}
}

func (fx *fixture) createStat() *bindings.ReplicationStat {
	return &bindings.ReplicationStat{
		Type: "cluster",
		Nodes: []bindings.ReplicationNodeStat{
			{
				DSN:            fmt.Sprintf("%s://%s/%s", fx.dsn.urls[0].Scheme, fx.dsn.urls[0].Host, fx.dsn.urls[2].Path),
				IsSynchronized: true,
				Status:         clusterNodeStatusOnline,
				Role:           clusterNodeRoleLeader,
			}, {
				DSN:            fmt.Sprintf("%s://%s/%s", fx.dsn.urls[1].Scheme, fx.dsn.urls[1].Host, fx.dsn.urls[2].Path),
				IsSynchronized: true,
				Status:         clusterNodeStatusOnline,
				Role:           clusterNodeRoleFollower,
			}, {
				DSN:            fmt.Sprintf("%s://%s/%s", fx.dsn.urls[2].Scheme, fx.dsn.urls[2].Host, fx.dsn.urls[2].Path),
				IsSynchronized: true,
				Status:         clusterNodeStatusOnline,
				Role:           clusterNodeRoleFollower,
			},
		},
	}

}

func TestNextDSN(t *testing.T) {
	t.Run("for reconnectStrategyNext", func(t *testing.T) {
		t.Run("should get next url", func(t *testing.T) {
			fx := newFixture(t)
			fx.addDSNs(t, dsns)
			assert.Equal(t, fx.dsn.urls[0].Host, fx.getActiveDSN().Host)
			err := fx.nextDSN(ctx, reconnectStrategyNext, nil)
			require.NoError(t, err)
			assert.Equal(t, fx.dsn.urls[1].Host, fx.getActiveDSN().Host)
		})

		t.Run("should get first if current is last idx", func(t *testing.T) {
			fx := newFixture(t)
			fx.addDSNs(t, dsns)

			for i := range fx.dsn.urls {
				err := fx.nextDSN(ctx, reconnectStrategyNext, nil)
				require.NoError(t, err)
				assert.Equal(t, fx.dsn.urls[(i+1)%len(fx.dsn.urls)].Host, fx.getActiveDSN().Host)
			}
		})
	})

	t.Run("for reconnectStrategyRandom", func(t *testing.T) {
		t.Run("should get random url", func(t *testing.T) {
			fx := newFixture(t)
			dsnsLocal := make([]url.URL, 0, 1000)
			for i := 1; i < 1000; i++ {
				dsnsLocal = append(dsnsLocal, url.URL{Host: fmt.Sprintf("127.0.0.1:%d", i), Scheme: "cproto", Path: "db"})
			}
			fx.addDSNs(t, dsnsLocal)

			findHost := func(targetHost string) int {
				for i, cur := range dsnsLocal {
					if cur.Host == targetHost {
						return i
					}
				}
				return -1
			}

			var host1, host2, host3 string
			for i := 0; i < 2; i++ {
				fx.dsn.reconnectionStrategy = reconnectStrategyRandom
				err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, nil)
				require.NoError(t, err)
				host1 = fx.getActiveDSN().Host
				assert.NotEqual(t, findHost(host1), -1, host1)

				err = fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, nil)
				require.NoError(t, err)
				host2 = fx.getActiveDSN().Host
				assert.NotEqual(t, findHost(host2), -1, host2)

				err = fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, nil)
				require.NoError(t, err)
				host3 = fx.getActiveDSN().Host
				assert.NotEqual(t, findHost(host3), -1, host3)

				if host1 != host2 && host1 != host3 {
					break
				}
			}

			assert.True(t, host1 != host2 && host1 != host3, "Hosts: %v, %v, %v", host1, host2, host3)
		})
	})

	t.Run("for reconnectStrategySynchronized", func(t *testing.T) {
		t.Run("should get random synced node", func(t *testing.T) {
			fx := newFixture(t)
			fx.addDSNs(t, dsns)
			stat := fx.createStat()
			stat.Nodes[0].Status = clusterNodeStatusOffline
			stat.Nodes[0].IsSynchronized = false

			fx.mockConns[1].expect().hasError().Return(false)

			fx.dsn.reconnectionStrategy = reconnectStrategySynchronized
			err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
			require.NoError(t, err)
			assert.Contains(t, []string{fx.dsn.urls[1].Host, fx.dsn.urls[2].Host}, fx.getActiveDSN().Host)
		})
	})

	t.Run("for reconnectStrategyReadOnly", func(t *testing.T) {
		t.Run("should get follower", func(t *testing.T) {
			fx := newFixture(t)
			fx.addDSNs(t, dsns)

			stat := fx.createStat()
			fx.mockConns[1].expect().hasError().Return(false)

			fx.dsn.reconnectionStrategy = reconnectStrategyReadOnly
			err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
			require.NoError(t, err)
			assert.Contains(t, []string{fx.dsn.urls[1].Host, fx.dsn.urls[2].Host}, fx.getActiveDSN().Host)
		})
	})

	t.Run("for reconnectStrategyPrefferWrite", func(t *testing.T) {
		t.Run("should get leader", func(t *testing.T) {
			fx := newFixture(t)
			fx.addDSNs(t, dsns)

			stat := fx.createStat()
			stat.Nodes[0].Role = clusterNodeRoleLeader

			fx.mockConns[1].expect().hasError().Return(false)

			fx.dsn.reconnectionStrategy = reconnectStrategyPrefferWrite
			err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
			require.NoError(t, err)
			assert.Equal(t, fx.dsn.urls[0].Host, fx.getActiveDSN().Host)
		})
	})

	t.Run("for reconnectStrategyPrefferRead", func(t *testing.T) {
		t.Run("type cluster", func(t *testing.T) {
			t.Run("should get follower", func(t *testing.T) {
				fx := newFixture(t)
				fx.addDSNs(t, dsns)

				stat := fx.createStat()
				stat.Type = replicationTypeCluster
				stat.Nodes[1].Status = clusterNodeStatusOffline

				fx.mockConns[1].expect().hasError().Return(false)

				fx.dsn.reconnectionStrategy = reconnectStrategyPrefferRead
				err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
				require.NoError(t, err)
				assert.Equal(t, fx.dsn.urls[2].Host, fx.getActiveDSN().Host)
			})
			t.Run("should get leader", func(t *testing.T) {
				fx := newFixture(t)
				fx.addDSNs(t, dsns)
				fx.dsn.allowUnknownNodes = true

				stat := fx.createStat()
				stat.Nodes[1].Status = clusterNodeStatusOffline
				stat.Nodes[2].Status = clusterNodeStatusOffline

				fx.mockConns[1].expect().hasError().Return(false)

				fx.dsn.reconnectionStrategy = reconnectStrategyPrefferRead
				err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
				require.NoError(t, err)
				assert.Equal(t, fx.dsn.urls[0].Host, fx.getActiveDSN().Host)
			})
		})
	})

	t.Run("should add new cluster dsn to url list", func(t *testing.T) {
		fx := newFixture(t)
		fx.addDSNs(t, dsns)
		fx.dsn.allowUnknownNodes = true

		stat := fx.createStat()
		stat.Nodes[0].IsSynchronized = false
		stat.Nodes[1].IsSynchronized = false
		stat.Nodes[2].IsSynchronized = false
		stat.Nodes = append(stat.Nodes, bindings.ReplicationNodeStat{
			DSN:            "cproto://127.0.0.1:6534",
			IsSynchronized: true,
			Status:         clusterNodeStatusOnline,
			Role:           clusterNodeRoleFollower,
		})

		fx.mockConns[1].expect().hasError().Return(false)

		fx.dsn.reconnectionStrategy = reconnectStrategySynchronized
		err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
		require.NoError(t, err)
		assert.Len(t, fx.dsn.urls, 4)
		assert.Equal(t, fx.dsn.urls[3].Host, fx.getActiveDSN().Host)
	})

	t.Run("should error if not found dsn", func(t *testing.T) {
		fx := newFixture(t)
		fx.addDSNs(t, dsns)

		stat := fx.createStat()
		stat.Nodes[0].IsSynchronized = false
		stat.Nodes[1].IsSynchronized = false
		stat.Nodes[2].IsSynchronized = false

		fx.mockConns[1].expect().hasError().Return(false)

		fx.dsn.reconnectionStrategy = reconnectStrategySynchronized
		err := fx.nextDSN(ctx, fx.dsn.reconnectionStrategy, stat)
		require.EqualError(t, err, "can't find cluster node for reconnect")
	})
}

func TestConnectDSN(t *testing.T) {
	t.Run("should fill connection pool", func(t *testing.T) {
		fx := newFixture(t)

		connCount := 2
		connAddr := "127.0.0.1:1234"
		fx.dsn.urls = []url.URL{{Path: connAddr}}

		conn := &connectionImpl{}
		params := newConnParams{
			dsn:               &url.URL{Path: connAddr},
			loginTimeout:      fx.timeouts.LoginTimeout,
			requestTimeout:    fx.timeouts.LoginTimeout,
			createDBIfMissing: fx.connectOpts.CreateDBIfMissing,
			appName:           fx.appName,
			enableCompression: fx.compression.EnableCompression,
		}
		fx.mockConnFactory.expect().newConnection(ctx, params, nil).Return(conn, "", int64(0), nil)

		err := fx.connectDSN(ctx, connCount, bindings.LBRoundRobin)
		require.NoError(t, err)
	})
}

func TestGetConn(t *testing.T) {
	t.Run("should reconnect with strategy after reconnect to next dsn", func(t *testing.T) {
		fx := newFixture(t)
		fx.dsn.reconnectionStrategy = reconnectStrategySynchronized
		fx.addDSNs(t, dsns)

		for i := range fx.dsn.urls {
			conn := &connectionImpl{}
			params := newConnParams{
				dsn:               &url.URL{Path: fx.dsn.urls[i].Path},
				loginTimeout:      fx.timeouts.LoginTimeout,
				requestTimeout:    fx.timeouts.LoginTimeout,
				createDBIfMissing: fx.connectOpts.CreateDBIfMissing,
				appName:           fx.appName,
				enableCompression: fx.compression.EnableCompression,
			}
			fx.mockConnFactory.expect().newConnection(ctx, params, nil).Return(conn, "", int64(0), nil)
		}

		fx.mockConns[1].expect().hasError().Return(true)
		fx.mockConns[1].expect().curError().Return(errors.New("exp"))

		fx.GetReplicationStat(func(ctx context.Context) (*bindings.ReplicationStat, error) {
			stat := fx.createStat()
			stat.Nodes[0].Status = clusterNodeStatusOffline
			stat.Nodes[0].IsSynchronized = false
			return stat, nil
		})

		_, err := fx.getConnection(ctx)
		require.NoError(t, err)
	})

	t.Run("should not get stat for random strategy", func(t *testing.T) {
		fx := newFixture(t)
		fx.dsn.reconnectionStrategy = reconnectStrategyRandom
		fx.addDSNs(t, dsns)

		for i := range fx.dsn.urls {
			conn := &connectionImpl{}
			params := newConnParams{
				dsn:               &url.URL{Path: fx.dsn.urls[i].Path},
				loginTimeout:      fx.timeouts.LoginTimeout,
				requestTimeout:    fx.timeouts.LoginTimeout,
				createDBIfMissing: fx.connectOpts.CreateDBIfMissing,
				appName:           fx.appName,
				enableCompression: fx.compression.EnableCompression,
			}
			fx.mockConnFactory.expect().newConnection(ctx, params, nil).Return(conn, "", int64(0), nil)
		}

		fx.mockConns[1].expect().hasError().Return(true)
		fx.mockConns[1].expect().curError().Return(errors.New("exp"))

		_, err := fx.getConnection(ctx)
		require.NoError(t, err)
	})
}
