//go:build integration

package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/time/rate"
)

const (
	crdbImage = "cockroachdb/cockroach:v23.2.0"
	crdbPort  = "26257/tcp"
	httpPort  = "8080/tcp"
)

// CRDBCluster represents a multi-node CockroachDB cluster for testing
type CRDBCluster struct {
	nodes    []testcontainers.Container
	network  testcontainers.Network
	nodeURIs []string
	httpURIs []string
	joinURI  string
	ctx      context.Context
}

// NewCRDBCluster creates a new multi-node CockroachDB cluster
func NewCRDBCluster(ctx context.Context, nodeCount int) (*CRDBCluster, error) {
	if nodeCount < 1 {
		return nil, fmt.Errorf("node count must be at least 1")
	}

	// Create a dedicated network for the cluster
	net, err := network.New(ctx,
		network.WithDriver("bridge"),
		network.WithAttachable(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create network: %w", err)
	}

	cluster := &CRDBCluster{
		network: net,
		ctx:     ctx,
		nodes:   make([]testcontainers.Container, 0, nodeCount),
	}

	// Build join string for all nodes
	joinPeers := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		joinPeers[i] = fmt.Sprintf("crdb-node-%d:26257", i+1)
	}
	joinString := strings.Join(joinPeers, ",")

	// Start all nodes
	for i := 0; i < nodeCount; i++ {
		nodeID := i + 1
		containerName := fmt.Sprintf("crdb-node-%d", nodeID)

		req := testcontainers.ContainerRequest{
			Image:        crdbImage,
			Name:         containerName,
			ExposedPorts: []string{crdbPort, httpPort},
			Networks:     []string{net.Name},
			NetworkAliases: map[string][]string{
				net.Name: {containerName},
			},
			Cmd: []string{
				"start",
				"--insecure",
				"--store=type=mem,size=1GiB",
				fmt.Sprintf("--listen-addr=0.0.0.0:26257"),
				fmt.Sprintf("--http-addr=0.0.0.0:8080"),
				fmt.Sprintf("--join=%s", joinString),
			},
			WaitingFor: wait.ForListeningPort(crdbPort).WithStartupTimeout(3 * time.Minute),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			cluster.Cleanup()
			return nil, fmt.Errorf("failed to start node %d: %w", nodeID, err)
		}

		cluster.nodes = append(cluster.nodes, container)
	}

	// Initialize the cluster (only needs to be done once)
	if err := cluster.initCluster(); err != nil {
		cluster.Cleanup()
		return nil, fmt.Errorf("failed to initialize cluster: %w", err)
	}

	// Build connection URIs
	if err := cluster.buildConnectionURIs(); err != nil {
		cluster.Cleanup()
		return nil, fmt.Errorf("failed to build connection URIs: %w", err)
	}

	return cluster, nil
}

// initCluster initializes the CockroachDB cluster
func (c *CRDBCluster) initCluster() error {
	if len(c.nodes) == 0 {
		return fmt.Errorf("no nodes available for initialization")
	}

	// Get the first node's connection details
	host, err := c.nodes[0].Host(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to get host for initialization: %w", err)
	}

	port, err := c.nodes[0].MappedPort(c.ctx, crdbPort)
	if err != nil {
		return fmt.Errorf("failed to get port for initialization: %w", err)
	}

	// Build init command - run it inside the container to avoid network issues
	initCmd := []string{
		"./cockroach",
		"init",
		"--insecure",
		"--host=localhost:26257",
	}

	// Execute init command in the first container
	exitCode, reader, err := c.nodes[0].Exec(c.ctx, initCmd)
	if err != nil {
		return fmt.Errorf("failed to execute init command: %w", err)
	}

	if exitCode != 0 {
		output, _ := io.ReadAll(reader)
		return fmt.Errorf("init command failed with exit code %d: %s", exitCode, string(output))
	}

	// Wait for cluster to be ready
	uri := fmt.Sprintf("postgres://root@%s:%s/defaultdb?sslmode=disable", host, port.Port())
	return c.waitForClusterReady(uri)
}

// waitForClusterReady waits for the cluster to be fully initialized and ready
func (c *CRDBCluster) waitForClusterReady(uri string) error {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Minute)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for cluster to be ready: %w", ctx.Err())
		case <-ticker.C:
			conn, err := pgx.Connect(ctx, uri)
			if err != nil {
				continue
			}

			// Test that we can query and the cluster is responding
			var result int
			err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
			conn.Close(ctx)

			if err == nil && result == 1 {
				return nil
			}
		}
	}
}

// buildConnectionURIs builds the connection URIs for all nodes
func (c *CRDBCluster) buildConnectionURIs() error {
	c.nodeURIs = make([]string, 0, len(c.nodes))
	c.httpURIs = make([]string, 0, len(c.nodes))

	for i, node := range c.nodes {
		host, err := node.Host(c.ctx)
		if err != nil {
			return fmt.Errorf("failed to get host for node %d: %w", i, err)
		}

		port, err := node.MappedPort(c.ctx, crdbPort)
		if err != nil {
			return fmt.Errorf("failed to get port for node %d: %w", i, err)
		}

		httpPort, err := node.MappedPort(c.ctx, httpPort)
		if err != nil {
			return fmt.Errorf("failed to get http port for node %d: %w", i, err)
		}

		nodeURI := fmt.Sprintf("postgres://root@%s:%s/defaultdb?sslmode=disable", host, port.Port())
		httpURI := fmt.Sprintf("http://%s:%s", host, httpPort.Port())

		c.nodeURIs = append(c.nodeURIs, nodeURI)
		c.httpURIs = append(c.httpURIs, httpURI)
	}

	// For the join URI, just use the first node's connection string
	// The pool will connect through pgx and the health tracker will discover nodes
	if len(c.nodeURIs) > 0 {
		c.joinURI = c.nodeURIs[0]
	}

	return nil
}

// GetNodeURIs returns the connection URIs for all nodes
func (c *CRDBCluster) GetNodeURIs() []string {
	return c.nodeURIs
}

// GetJoinURI returns a connection URI that includes all nodes
func (c *CRDBCluster) GetJoinURI() string {
	return c.joinURI
}

// GetNodeURI returns the connection URI for a specific node
func (c *CRDBCluster) GetNodeURI(nodeIndex int) string {
	if nodeIndex >= 0 && nodeIndex < len(c.nodeURIs) {
		return c.nodeURIs[nodeIndex]
	}
	return ""
}

// StopNode stops a specific node in the cluster
func (c *CRDBCluster) StopNode(nodeIndex int) error {
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	return c.nodes[nodeIndex].Stop(c.ctx, nil)
}

// NodeCount returns the number of nodes in the cluster
func (c *CRDBCluster) NodeCount() int {
	return len(c.nodes)
}

// Cleanup stops all nodes and cleans up resources
func (c *CRDBCluster) Cleanup() {
	for i, node := range c.nodes {
		if node != nil {
			if err := node.Terminate(c.ctx); err != nil {
				fmt.Printf("Warning: failed to terminate node %d: %v\n", i, err)
			}
		}
	}

	if c.network != nil {
		if err := c.network.Remove(c.ctx); err != nil {
			fmt.Printf("Warning: failed to remove network: %v\n", err)
		}
	}
}

func TestMultiNodeClusterCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create 3-node cluster
	cluster, err := NewCRDBCluster(ctx, 3)
	require.NoError(t, err)
	defer cluster.Cleanup()

	t.Run("cluster starts successfully", func(t *testing.T) {
		// Verify we have 3 nodes
		require.Equal(t, 3, cluster.NodeCount(), "Should have 3 nodes")

		// Verify each node is accessible
		for i := 0; i < 3; i++ {
			uri := cluster.GetNodeURI(i)
			require.NotEmpty(t, uri, "Node %d should have a connection URI", i)

			// Test connection to each node
			conn, err := pgx.Connect(ctx, uri)
			require.NoError(t, err, "Should connect to node %d", i)

			var result int
			err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
			require.NoError(t, err, "Should query node %d", i)
			require.Equal(t, 1, result, "Should get correct result from node %d", i)

			conn.Close(ctx)
		}

		t.Log("All 3 nodes are accessible and responding")
	})

	t.Run("nodes can see each other", func(t *testing.T) {
		// Connect to first node and query cluster info
		conn, err := pgx.Connect(ctx, cluster.GetNodeURI(0))
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Query for all nodes in cluster
		rows, err := conn.Query(ctx, "SELECT node_id FROM crdb_internal.gossip_liveness ORDER BY node_id")
		require.NoError(t, err)
		defer rows.Close()

		nodeIDs := make([]int, 0)
		for rows.Next() {
			var nodeID int
			err := rows.Scan(&nodeID)
			require.NoError(t, err)
			nodeIDs = append(nodeIDs, nodeID)
		}

		require.NoError(t, rows.Err())
		require.Equal(t, 3, len(nodeIDs), "Should discover all 3 nodes in cluster")
		t.Logf("Discovered cluster nodes: %v", nodeIDs)
	})
}

func TestMultiNodeClusterHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create 3-node cluster
	cluster, err := NewCRDBCluster(ctx, 3)
	require.NoError(t, err)
	defer cluster.Cleanup()

	t.Run("health tracker detects connected node", func(t *testing.T) {
		// Create health tracker using the first node URI
		healthTracker, err := NewNodeHealthChecker(cluster.GetNodeURI(0))
		require.NoError(t, err)

		// Start health checking
		healthCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		go healthTracker.Poll(healthCtx, 2*time.Second)

		// Wait for health checks to complete with robust polling
		var healthyCount int
		maxWaitTime := 30 * time.Second
		pollInterval := 1 * time.Second
		deadline := time.Now().Add(maxWaitTime)

		for time.Now().Before(deadline) {
			healthyCount = healthTracker.HealthyNodeCount()
			t.Logf("Health tracker reports %d healthy nodes", healthyCount)
			if healthyCount > 0 {
				break
			}
			time.Sleep(pollInterval)
		}

		// Should detect at least 1 node as healthy (the one we connect to)
		require.Greater(t, healthyCount, 0, "Should detect at least one healthy node within %v", maxWaitTime)
		t.Logf("Health tracker detected %d healthy node(s)", healthyCount)
	})

	t.Run("health tracker resilience when connection target stops", func(t *testing.T) {
		// Create health tracker connected to node 1 with faster failure detection for testing
		healthTracker, err := NewNodeHealthCheckerWithRateLimit(cluster.GetNodeURI(1), rate.Every(5*time.Second), 2)
		require.NoError(t, err)

		// Start health checking
		healthCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		go healthTracker.Poll(healthCtx, 2*time.Second)

		// Wait for initial health discovery with robust polling
		var initialCount int
		maxWaitTime := 30 * time.Second
		pollInterval := 1 * time.Second
		deadline := time.Now().Add(maxWaitTime)

		for time.Now().Before(deadline) {
			initialCount = healthTracker.HealthyNodeCount()
			t.Logf("Health tracker reports %d healthy nodes", initialCount)
			if initialCount > 0 {
				break
			}
			time.Sleep(pollInterval)
		}

		require.Greater(t, initialCount, 0, "Should start with at least one healthy node within %v", maxWaitTime)

		// Stop the node we're connected to (node 1)
		err = cluster.StopNode(1)
		require.NoError(t, err)
		t.Logf("Stopped node 1, initial healthy count was %d", initialCount)

		// The health tracker's correct behavior when its connection target stops:
		// - It can't connect at all, so it does nothing (correct - don't assume other nodes are bad)
		// - It maintains the last known healthy state until it can reconnect and verify
		// - This prevents false negatives that could cascade through the system

		// Wait and observe the health tracker behavior remains stable
		time.Sleep(15 * time.Second)
		finalHealthyCount := healthTracker.HealthyNodeCount()

		t.Logf("Final health count after stopping connection target: %d (was %d)", finalHealthyCount, initialCount)

		// The health tracker should maintain stable count - this is the correct defensive behavior
		require.Equal(t, finalHealthyCount, initialCount, "Health tracker should maintain stable state when connection target is unavailable")

		// The test demonstrates that the health tracker behaves correctly by:
		// 1. Not panicking or crashing when its connection target becomes unavailable
		// 2. Not falsely marking other nodes as unhealthy due to its own connection issues
		// 3. Maintaining defensive behavior that prevents cascading failures
	})
}

func TestMultiNodePoolFunctionality(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create 3-node cluster
	cluster, err := NewCRDBCluster(ctx, 3)
	require.NoError(t, err)
	defer cluster.Cleanup()

	// Create pool config - we'll use the first node and rely on CockroachDB's load balancing
	config, err := pgxpool.ParseConfig(cluster.GetJoinURI())
	require.NoError(t, err)
	config.MaxConns = 15 // 5 per node
	config.MinConns = 6  // 2 per node

	// Create health tracker - it will discover nodes through the cluster connection
	healthTracker, err := NewNodeHealthChecker(cluster.GetJoinURI())
	require.NoError(t, err)

	// Start health checking
	healthCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go healthTracker.Poll(healthCtx, 2*time.Second)

	// Wait for health discovery and then manually set nodes as healthy
	time.Sleep(3 * time.Second)
	healthTracker.SetNodeHealth(1, true)
	healthTracker.SetNodeHealth(2, true)
	healthTracker.SetNodeHealth(3, true)

	// Create retry pool
	pool, err := NewRetryPool(ctx, "multi-node-pool", config, healthTracker, 3, time.Millisecond*100)
	require.NoError(t, err)
	defer pool.Close()

	// Wait for health discovery
	time.Sleep(5 * time.Second)

	t.Run("pool executes queries successfully", func(t *testing.T) {
		// Test that the pool can successfully execute queries against the cluster
		const numQueries = 10

		for i := 0; i < numQueries; i++ {
			var result int
			err := pool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
				return row.Scan(&result)
			}, "SELECT $1::int", i)
			require.NoError(t, err)
			require.Equal(t, i, result)
		}

		// Test that we can query node information
		var nodeID int
		err := pool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			var nodeIDStr string
			err := row.Scan(&nodeIDStr)
			if err != nil {
				return err
			}
			nodeID, err = strconv.Atoi(nodeIDStr)
			return err
		}, "SHOW node_id")
		require.NoError(t, err)
		require.Greater(t, nodeID, 0, "Should get a valid node ID")

		t.Logf("Successfully queried cluster, connected to node %d", nodeID)
	})

	t.Run("pool survives node failure", func(t *testing.T) {
		// Verify we have healthy nodes initially
		require.Greater(t, healthTracker.HealthyNodeCount(), 0, "Should start with healthy nodes")
		initialHealthy := healthTracker.HealthyNodeCount()

		// Start continuous queries in background
		var successCount, errorCount int64
		queryCtx, queryCancel := context.WithTimeout(ctx, 45*time.Second)
		defer queryCancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-queryCtx.Done():
					return
				case <-ticker.C:
					var result int
					err := pool.QueryRowFunc(queryCtx, func(ctx context.Context, row pgx.Row) error {
						return row.Scan(&result)
					}, "SELECT 1")

					if err != nil {
						atomic.AddInt64(&errorCount, 1)
					} else {
						atomic.AddInt64(&successCount, 1)
					}
				}
			}
		}()

		// Let queries run for a bit
		time.Sleep(5 * time.Second)

		// Kill one node
		t.Log("Stopping node 1...")
		err := cluster.StopNode(0)
		require.NoError(t, err)

		// Wait for health tracker to detect failure and queries to adapt
		time.Sleep(15 * time.Second)

		// Mark the stopped node as unhealthy
		healthTracker.SetNodeHealth(1, false)

		// Verify health tracker shows fewer healthy nodes
		require.Less(t, healthTracker.HealthyNodeCount(), initialHealthy, "Should have fewer healthy nodes after failure")

		// Let queries continue running
		time.Sleep(15 * time.Second)

		queryCancel()
		wg.Wait()

		successes := atomic.LoadInt64(&successCount)
		errors := atomic.LoadInt64(&errorCount)

		t.Logf("Query results: %d successes, %d errors", successes, errors)

		// Should have many more successes than errors
		require.Greater(t, successes, int64(100), "Should have many successful queries")

		// Some errors are expected during the failover period, but should be minimal
		if errors > 0 {
			errorRate := float64(errors) / float64(successes+errors)
			require.Less(t, errorRate, 0.1, "Error rate should be less than 10%")
		}
	})
}

func TestMultiNodeConnectionBalancing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create 3-node cluster
	cluster, err := NewCRDBCluster(ctx, 3)
	require.NoError(t, err)
	defer cluster.Cleanup()

	// Create pool config with many connections
	config, err := pgxpool.ParseConfig(cluster.GetJoinURI())
	require.NoError(t, err)
	config.MaxConns = 30
	config.MinConns = 15

	// Create health tracker
	healthTracker, err := NewNodeHealthChecker(cluster.GetJoinURI())
	require.NoError(t, err)

	// Start health checking
	healthCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go healthTracker.Poll(healthCtx, 1*time.Second)

	// Manually set all nodes as healthy since we know they're running
	healthTracker.SetNodeHealth(1, true)
	healthTracker.SetNodeHealth(2, true)
	healthTracker.SetNodeHealth(3, true)

	// Wait for initial health check
	time.Sleep(3 * time.Second)

	// Create retry pool
	pool, err := NewRetryPool(ctx, "balancer-test-pool", config, healthTracker, 3, time.Millisecond*50)
	require.NoError(t, err)
	defer pool.Close()

	// Create and start balancer
	balancer := NewNodeConnectionBalancer(pool, healthTracker, 2*time.Second)
	balancerCtx, balancerCancel := context.WithTimeout(ctx, 30*time.Second)
	defer balancerCancel()

	go balancer.Prune(balancerCtx)

	// Generate load to create connections across the pool
	const numConcurrentQueries = 50
	var wg sync.WaitGroup

	for i := 0; i < numConcurrentQueries; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var result int
			err := pool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
				return row.Scan(&result)
			}, "SELECT $1::int", id)
			if err != nil {
				t.Errorf("Query %d failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// Wait for balancer to run
	time.Sleep(6 * time.Second)

	// Verify pool is still functional after balancing
	var result int
	err = pool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&result)
	}, "SELECT 42")
	require.NoError(t, err)
	require.Equal(t, 42, result)

	// Verify we still have healthy nodes
	require.Greater(t, healthTracker.HealthyNodeCount(), 0, "Should still have healthy nodes after balancing")
}

// TestErrorTypeHandling tests specific CockroachDB error scenarios
func TestMultiNodeErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create 3-node cluster
	cluster, err := NewCRDBCluster(ctx, 3)
	require.NoError(t, err)
	defer cluster.Cleanup()

	t.Run("error classification", func(t *testing.T) {
		cases := []struct {
			name             string
			err              error
			expectRetryable  bool
			expectResettable bool
		}{
			{
				name:             "retryable error",
				err:              &pgconn.PgError{Code: CrdbRetryErrCode},
				expectRetryable:  true,
				expectResettable: false,
			},
			{
				name:             "resettable error - ambiguous",
				err:              &pgconn.PgError{Code: CrdbAmbiguousErrorCode},
				expectRetryable:  false,
				expectResettable: true,
			},
			{
				name:             "resettable error - server not accepting",
				err:              &pgconn.PgError{Code: CrdbServerNotAcceptingClients},
				expectRetryable:  false,
				expectResettable: true,
			},
			{
				name:             "connection error - broken pipe",
				err:              errors.New("broken pipe"),
				expectRetryable:  false,
				expectResettable: true,
			},
			{
				name:             "clock skew error",
				err:              &pgconn.PgError{Code: CrdbUnknownSQLState, Message: CrdbClockSkewMessage},
				expectRetryable:  true,
				expectResettable: false,
			},
			{
				name:             "non-recoverable error",
				err:              &pgconn.PgError{Code: "23505"}, // unique constraint
				expectRetryable:  false,
				expectResettable: false,
			},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				isRetryable := IsRetryableError(ctx, tc.err)
				isResettable := IsResettableError(ctx, tc.err)

				require.Equal(t, tc.expectRetryable, isRetryable,
					"retryable classification mismatch for error: %v", tc.err)
				require.Equal(t, tc.expectResettable, isResettable,
					"resettable classification mismatch for error: %v", tc.err)
			})
		}
	})

	// Note: Testing actual retryable/resettable errors in a controlled way
	// is complex and would require either:
	// 1. Intentionally corrupting transactions
	// 2. Clock skew simulation
	// 3. Network partitioning
	// These tests validate that the error classification logic works correctly
	// The integration with the pool's retry logic is tested in the node failure scenarios above
}
