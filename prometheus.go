package reindexer

import (
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	promStatsClientCallsLatency = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "reindexer",
			Subsystem: "client",
			Name:      "calls_latency_seconds",
			Help:      "Latency of Reindexer Client calls",

			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			MaxAge:     3 * time.Minute,
			AgeBuckets: 3,
		},
		[]string{"dsn", "cmd", "ns"},
	)
)

type reindexerPrometheusMetrics struct {
	clientCallsLatency prometheus.ObserverVec
}

func newPrometheusMetrics(dsnParsed []url.URL) *reindexerPrometheusMetrics {
	return &reindexerPrometheusMetrics{
		clientCallsLatency: promStatsClientCallsLatency.MustCurryWith(prometheus.Labels{"dsn": dsnString(dsnParsed)}),
	}
}
