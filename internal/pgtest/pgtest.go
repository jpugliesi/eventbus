// Package pgtest provides a shared, reusable Postgres testcontainer for this
// module's integration tests. It boots (or attaches to) a single Docker
// container across test packages and hands out freshly created, isolated
// databases per test.
package pgtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// sharedContainerName is the stable name of the singleton Postgres container
// reused across every test package's TestMain, via testcontainers-go's reuse
// mode: the first TestMain to call SetupContainer creates the container;
// every subsequent caller (in this process or any other, on the same host)
// attaches to the running container instead of starting a new one.
const sharedContainerName = "eventbus-pgtest"

// superuser is the single Postgres role every test connects as. eventbus has
// no row-level-security or multi-role model of its own, so there's no need
// for an admin/app role split here.
const (
	superuserName     = "test"
	superuserPassword = "test"
)

// Container holds the shared PostgreSQL test container.
type Container struct {
	*postgres.PostgresContainer
}

// SetupContainer boots (or attaches to) the shared Postgres container. Call
// once from TestMain; the returned cleanup is a no-op because the container
// is shared and reaped by testcontainers' Ryuk sidecar once nothing is
// attached to it anymore.
func SetupContainer(ctx context.Context) (*Container, func(), error) {
	pgc, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername(superuserName),
		postgres.WithPassword(superuserPassword),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithReuseByName(sharedContainerName),
		// Default max_connections=100 is exhausted quickly once several test
		// packages hit the shared instance concurrently.
		testcontainers.WithCmdArgs("-c", "max_connections=500"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, fmt.Errorf("get connection string: %w", err)
	}
	adminDB, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer adminDB.Close()

	// On reuse, another concurrent test process may have just created the
	// container; wait for it to accept connections before issuing DDL.
	if err := waitForReady(ctx, adminDB); err != nil {
		return nil, nil, fmt.Errorf("wait for postgres ready: %w", err)
	}

	return &Container{PostgresContainer: pgc}, func() {}, nil
}

// waitForReady pings db until it responds or the deadline elapses. Handles
// the race where one process is mid-creating the shared container while
// another attaches via WithReuseByName.
func waitForReady(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := db.PingContext(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("postgres not ready after 30s: %w", lastErr)
}

// DropDatabase drops dbName via a fresh admin connection. WITH (FORCE)
// terminates any lingering sessions (Postgres 13+) so the drop always
// succeeds even if a test left a pool connection open. The shared container
// persists across `go test` invocations, so without this cleanup, databases
// accumulate forever.
func (c *Container) DropDatabase(dbName string) error {
	ctx := context.Background()
	adminDSN, err := c.ConnectionString(ctx, "sslmode=disable", "dbname=postgres")
	if err != nil {
		return err
	}
	adminDB, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return err
	}
	defer adminDB.Close()
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, dbName))
	return err
}

// TestDatabase is an isolated database created for a single test, connected
// to as the shared superuser role.
type TestDatabase struct {
	endpoint string
	dbName   string
}

// StartTestDatabaseWithoutMigrations creates a new, uniquely-named database on
// the shared container and registers its cleanup. eventbus manages its own
// schema via Client.EnsureSchema, so there's nothing to migrate here.
func (c *Container) StartTestDatabaseWithoutMigrations(t *testing.T) *TestDatabase {
	t.Helper()
	ctx := t.Context()

	var raw [16]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	dbName := "test_" + hex.EncodeToString(raw[:])

	adminDSN, err := c.ConnectionString(ctx, "sslmode=disable", "dbname=postgres")
	require.NoError(t, err)
	adminDB, err := sql.Open("pgx", adminDSN)
	require.NoError(t, err)
	defer adminDB.Close()

	_, err = adminDB.ExecContext(ctx, "CREATE DATABASE "+dbName)
	require.NoError(t, err)

	endpoint, err := c.PortEndpoint(ctx, "5432/tcp", "")
	require.NoError(t, err)

	t.Cleanup(func() { _ = c.DropDatabase(dbName) })

	return &TestDatabase{endpoint: endpoint, dbName: dbName}
}

// ConnectionURL returns the test database's connection string.
//
// AdminConnectionURL returns the identical string: unlike the internal
// helper this package was trimmed from, eventbus has no admin/app role split
// to model, so there's only one role. Both methods exist so call sites that
// expect either name work unmodified.
func (db *TestDatabase) ConnectionURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", superuserName, superuserPassword, db.endpoint, db.dbName)
}

// AdminConnectionURL returns the same connection string as ConnectionURL.
// See ConnectionURL for why.
func (db *TestDatabase) AdminConnectionURL() string {
	return db.ConnectionURL()
}
