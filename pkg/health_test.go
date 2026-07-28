package pool

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNodeHealthTracker(t *testing.T) {
	tracker := &NodeHealthTracker{
		healthyNodes:  make(map[uint32]struct{}),
		nodesEverSeen: make(map[uint32]*rate.Limiter),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), 2)
		},
	}

	tracker.SetNodeHealth(1, true)
	require.True(t, tracker.IsHealthy(1))
	require.False(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)

	tracker.SetNodeHealth(2, true)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 2)

	// just 1 mark isn't enough to trigger false
	tracker.SetNodeHealth(1, false)
	require.True(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 2)

	tracker.SetNodeHealth(1, false)
	tracker.SetNodeHealth(1, false)
	require.False(t, tracker.IsHealthy(1))
	require.True(t, tracker.IsHealthy(2))
	require.Equal(t, tracker.HealthyNodeCount(), 1)
}

func TestJitteredInterval(t *testing.T) {
	// nolint:gosec
	// G404 a deterministic source keeps the test reproducible.
	rng := rand.New(rand.NewSource(1))

	const interval = 100 * time.Millisecond
	var sum time.Duration
	const draws = 10000

	for range draws {
		jittered := jitteredInterval(rng, interval)
		require.GreaterOrEqual(t, jittered, interval/2)
		require.Less(t, jittered, interval+interval/2)
		sum += jittered
	}

	// the distribution is centered on the requested interval
	require.InDelta(t, interval, sum/draws, float64(interval/20))
}
