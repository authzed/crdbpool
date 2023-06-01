# crdbpool

[![Go Report Card](https://goreportcard.com/badge/github.com/authzed/crdbpool)](https://goreportcard.com/report/github.com/authzed/crdbpool)
[![GoDoc](https://pkg.go.dev/badge/s.svg)](https://pkg.go.dev/github.com/authzed/crdbpool)

`crdbpool` implements a node-aware connection pool for [CockroachDB] by wrapping [pgx].

By introducing advanced health-checking, this library is able to improve:
- performance by balancing connections evenly across nodes
- resiliency by retrying failed queries against a different node

Have questions? Ask in our [Discord].

Looking to contribute? See [CONTRIBUTING.md].

You can find issues by priority: [Urgent], [High], [Medium], [Low], [Maybe].
There are also [good first issues].

[CockroachDB]: https://github.com/cockroachdb/cockroach
[pgx]: https://github.com/jackc/pgx
[Discord]: https://authzed.com/discord
[CONTRIBUTING.md]: https://github.com/authzed/spicedb/blob/main/CONTRIBUTING.md
[Urgent]: https://github.com/authzed/spicedb/labels/priority%2F0%20urgent
[High]: https://github.com/authzed/spicedb/labels/priority%2F1%20high
[Medium]: https://github.com/authzed/spicedb/labels/priority%2F2%20medium
[Low]: https://github.com/authzed/spicedb/labels/priority%2F3%20low
[Maybe]: https://github.com/authzed/spicedb/labels/priority%2F4%20maybe
[good first issues]: https://github.com/authzed/spicedb/labels/hint%2Fgood%20first%20issue

## Example Usage

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
	// Allocate a health checker that will track CockroachDB nodes
	healthChecker, err := crdbpool.NewNodeHealthChecker("postgres://username:password@localhost:5432/database_name")
	if err != nil {
		panic(err)
	}

	// Configure pgxpool as you normally would
	config, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/database_name")
	if err != nil {
		panic(err)
	}

	// Wrap the pgxpool with node-aware retry logic
	maxRetries := 10
	connectRate := 100 * time.Millisecond
	pool, err := crdbpool.NewRetryPool(context.TODO(), "pool", config, healthChecker, maxRetries, connectRate)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// Start balancing connections and discovering nodes
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

	// Use the pgxpool as normal.
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

## Acknowledgements

This library was produced from [AuthZed]'s findings along with the collaboration of [Cockroach Labs].
As a result, we'd like to thank a few notable contributors:

- [Evan Cordell](https://github.com/ecordell)
- [Bram Gruneir](https://github.com/BramGruneir)
- Steven Hand

[AuthZed]: https://authzed.com
[Cockroach Labs]: https://cockroachdb.com
