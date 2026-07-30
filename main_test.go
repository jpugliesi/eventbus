package eventbus_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jpugliesi/eventbus/internal/pgtest"
)

// testContainer is the shared Postgres testcontainer; each test gets its own
// isolated database from it.
var testContainer *pgtest.Container

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, cleanup, err := pgtest.SetupContainer(ctx)
	if err != nil {
		log.Fatalf("eventbus: setup postgres container: %v", err)
	}
	testContainer = container
	code := m.Run()
	cleanup()
	os.Exit(code)
}
