# crdbpool

`crdbpool` implements a connection pool for CockroachDB that wraps [pgx](https://github.com/jackc/pgx).
It ensures connections are evenly balanced across Cockroach nodes and can retry queries against a different node when the query fails.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/authzed/crdbpool"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func main() {
	// create a health checker that will discover crdb nodes
	healthChecker, err := crdbpool.NewNodeHealthChecker("postgres://username:password@localhost:5432/database_name")
	if err != nil {
		panic(err)
	}

	// configure pgxpool as you normally would
	config, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/database_name")
	if err != nil {
		panic(err)
	}

	// create a RetryPool to wrap the pgxpool
	maxRetries := 10
	connectRate := 100 * time.Millisecond
	pool, err := crdbpool.NewRetryPool(context.Background(), "pool", config, healthChecker, maxRetries, connectRate)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start balancing connections and discovering nodes
	g, ctx := errgroup.WithContext(ctx)
	balancer := pool.NewNodeConnectionBalancer(pool, healthChecker, 5*time.Second)
	g.Go(func() error {
		balancer.Prune(ctx)
		return nil
	})
	g.Go(func() error {
		healthChecker.Poll(ctx, 5*time.Second)
		return nil
	})

	// use the pool
	pool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		docs := make([][]byte, 0)
		for rows.Next() {
			var doc []byte
			err = rows.Scan(&doc)
			if err != nil {
				return fmt.Errorf("couldn't scan document")
			}
			docs = append(docs, doc)
		}
		return rows.Err()
	}, "SELECT * FROM documents;")
}
```
