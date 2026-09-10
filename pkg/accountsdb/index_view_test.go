package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustIndexResource(
	t *testing.T,
	name string,
	value any,
	closeFn func() error,
	deleteFn func() error,
) *IndexGenerationResource {
	t.Helper()
	resource, err := NewIndexGenerationResource(name, value, closeFn, deleteFn)
	require.NoError(t, err)
	return resource
}

func waitIndexResource(t *testing.T, resource *IndexGenerationResource) {
	t.Helper()
	select {
	case <-resource.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for resource %q", resource.Name())
	}
}

func TestIndexViewPublicationDoesNotWaitForPinnedReader(t *testing.T) {
	t.Parallel()

	var cleanupMu sync.Mutex
	var cleanupOrder []string
	oldResource := mustIndexResource(t, "base-1", "old mapping", func() error {
		cleanupMu.Lock()
		cleanupOrder = append(cleanupOrder, "close")
		cleanupMu.Unlock()
		return nil
	}, func() error {
		cleanupMu.Lock()
		cleanupOrder = append(cleanupOrder, "delete")
		cleanupMu.Unlock()
		return nil
	})
	newResource := mustIndexResource(t, "base-2", "new mapping", nil, func() error {
		t.Error("current resource must not be deleted during shutdown")
		return nil
	})

	initial := validRootIndexCatalogForTest(t, 2, 1)
	manager, err := NewIndexViewManager(initial, "generation one", []*IndexGenerationResource{oldResource})
	require.NoError(t, err)
	oldView, err := manager.Acquire()
	require.NoError(t, err)

	next := validRootIndexCatalogForTest(t, 2, 2)
	started := time.Now()
	require.NoError(t, manager.Publish(
		next,
		"generation two",
		[]*IndexGenerationResource{newResource},
		[]*IndexGenerationResource{oldResource},
	))
	require.Less(t, time.Since(started), time.Second)
	select {
	case <-oldResource.Done():
		t.Fatal("old resource was finalized while its read view remained pinned")
	default:
	}

	newView, err := manager.Acquire()
	require.NoError(t, err)
	require.Equal(t, uint64(2), newView.Catalog().Generation)
	require.Equal(t, "generation two", newView.Payload())
	resourceFromView, ok := newView.Resource("base-2")
	require.True(t, ok)
	require.Same(t, newResource, resourceFromView)
	require.Equal(t, "new mapping", resourceFromView.Value())

	require.NoError(t, oldView.Close())
	require.NoError(t, oldView.Close())
	require.Nil(t, oldView.Catalog())
	require.Nil(t, oldView.Payload())
	_, ok = oldView.Resource("base-1")
	require.False(t, ok)
	waitIndexResource(t, oldResource)
	cleanupMu.Lock()
	require.Equal(t, []string{"close", "delete"}, cleanupOrder)
	cleanupMu.Unlock()

	require.NoError(t, newView.Close())
	require.NoError(t, manager.Shutdown(context.Background()))
	waitIndexResource(t, newResource)
}

func TestIndexViewRejectsObsoleteResourceNameReuse(t *testing.T) {
	t.Parallel()

	oldResource := mustIndexResource(t, "base/current", nil, nil, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		nil,
		[]*IndexGenerationResource{oldResource},
	)
	require.NoError(t, err)

	replacementHandle := mustIndexResource(t, "base/current", nil, nil, nil)
	err = manager.Publish(
		validRootIndexCatalogForTest(t, 2, 2),
		nil,
		[]*IndexGenerationResource{replacementHandle},
		[]*IndexGenerationResource{oldResource},
	)
	require.ErrorIs(t, err, ErrIndexGenerationRejected)
	waitIndexResource(t, replacementHandle)
	select {
	case <-oldResource.Done():
		t.Fatal("rejected publication released the current generation")
	default:
	}
	require.NoError(t, manager.Close())
}

func TestIndexViewSharedResourceSurvivesGenerationRetirement(t *testing.T) {
	t.Parallel()

	var sharedCloses atomic.Int32
	var sharedDeletes atomic.Int32
	shared := mustIndexResource(t, "extents", nil, func() error {
		sharedCloses.Add(1)
		return nil
	}, func() error {
		sharedDeletes.Add(1)
		return nil
	})
	oldOnly := mustIndexResource(t, "base-1", nil, nil, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		nil,
		[]*IndexGenerationResource{shared, oldOnly},
	)
	require.NoError(t, err)

	// A resource cannot be retained by the replacement and declared obsolete
	// in the same atomic publication.
	err = manager.Publish(
		validRootIndexCatalogForTest(t, 2, 2),
		nil,
		[]*IndexGenerationResource{shared},
		[]*IndexGenerationResource{shared},
	)
	require.ErrorIs(t, err, ErrIndexGenerationRejected)
	select {
	case <-shared.Done():
		t.Fatal("failed publication released the current generation's shared resource")
	default:
	}

	newOnly := mustIndexResource(t, "base-2", nil, nil, nil)
	require.NoError(t, manager.Publish(
		validRootIndexCatalogForTest(t, 2, 2),
		nil,
		[]*IndexGenerationResource{shared, newOnly},
		[]*IndexGenerationResource{oldOnly},
	))
	waitIndexResource(t, oldOnly)
	select {
	case <-shared.Done():
		t.Fatal("shared resource was closed while retained by the new generation")
	default:
	}

	require.NoError(t, manager.Close())
	waitIndexResource(t, shared)
	require.Equal(t, int32(1), sharedCloses.Load())
	require.Zero(t, sharedDeletes.Load())
}

func TestIndexViewPublishRejectsStaleGenerationAndShardMigration(t *testing.T) {
	t.Parallel()

	current := mustIndexResource(t, "current", nil, nil, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 10),
		nil,
		[]*IndexGenerationResource{current},
	)
	require.NoError(t, err)

	for name, candidate := range map[string]*RootIndexCatalog{
		"same":   validRootIndexCatalogForTest(t, 2, 10),
		"gap":    validRootIndexCatalogForTest(t, 2, 12),
		"shards": validRootIndexCatalogForTest(t, 4, 11),
	} {
		name, candidate := name, candidate
		t.Run(name, func(t *testing.T) {
			fresh := mustIndexResource(t, "candidate-"+name, nil, nil, nil)
			err := manager.Publish(candidate, nil, []*IndexGenerationResource{fresh}, nil)
			require.ErrorIs(t, err, ErrIndexGenerationRejected)
			waitIndexResource(t, fresh)
		})
	}

	view, err := manager.Acquire()
	require.NoError(t, err)
	require.Equal(t, uint64(10), view.Catalog().Generation)
	require.NoError(t, view.Close())
	require.NoError(t, manager.Close())
}

func TestIndexViewShutdownCanTimeOutAndResume(t *testing.T) {
	t.Parallel()

	resource := mustIndexResource(t, "mapping", nil, nil, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		nil,
		[]*IndexGenerationResource{resource},
	)
	require.NoError(t, err)
	view, err := manager.Acquire()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, manager.Shutdown(ctx), context.Canceled)
	_, err = manager.Acquire()
	require.ErrorIs(t, err, ErrIndexViewManagerClosed)

	rejected := mustIndexResource(t, "rejected", nil, nil, nil)
	err = manager.Publish(
		validRootIndexCatalogForTest(t, 2, 2),
		nil,
		[]*IndexGenerationResource{rejected},
		nil,
	)
	require.ErrorIs(t, err, ErrIndexViewManagerClosed)
	waitIndexResource(t, rejected)

	require.NoError(t, view.Close())
	require.NoError(t, manager.Shutdown(context.Background()))
	waitIndexResource(t, resource)
	select {
	case <-manager.Done():
	default:
		t.Fatal("manager Done was not closed after shutdown drained")
	}
}

func TestIndexViewShutdownReportsCleanupErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("unmap failed")
	resource := mustIndexResource(t, "mapping", nil, func() error { return wantErr }, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		nil,
		[]*IndexGenerationResource{resource},
	)
	require.NoError(t, err)
	require.ErrorIs(t, manager.Close(), wantErr)
	require.ErrorIs(t, resource.Err(), wantErr)
	// Completed shutdown is stable and reports the same accumulated error.
	require.ErrorIs(t, manager.Close(), wantErr)
}

func TestIndexViewShutdownWaitsForRejectedGenerationCleanup(t *testing.T) {
	t.Parallel()

	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 10),
		nil,
		[]*IndexGenerationResource{mustIndexResource(t, "current", nil, nil, nil)},
	)
	require.NoError(t, err)

	cleanupStarted := make(chan struct{})
	allowCleanup := make(chan struct{})
	rejected := mustIndexResource(t, "rejected", nil, func() error {
		close(cleanupStarted)
		<-allowCleanup
		return nil
	}, nil)
	// A stale generation is rejected, but its retained mapping is deliberately
	// finalized asynchronously so Publish itself remains non-blocking.
	require.ErrorIs(t, manager.Publish(
		validRootIndexCatalogForTest(t, 2, 10),
		nil,
		[]*IndexGenerationResource{rejected},
		nil,
	), ErrIndexGenerationRejected)
	<-cleanupStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Shutdown(ctx), context.DeadlineExceeded)

	close(allowCleanup)
	require.NoError(t, manager.Shutdown(context.Background()))
	waitIndexResource(t, rejected)
}

func TestIndexViewConcurrentAcquireAndPublish(t *testing.T) {
	initialResource := mustIndexResource(t, "generation-1", nil, nil, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		uint64(1),
		[]*IndexGenerationResource{initialResource},
	)
	require.NoError(t, err)

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				view, err := manager.Acquire()
				if errors.Is(err, ErrIndexViewManagerClosed) {
					return
				}
				require.NoError(t, err)
				require.Equal(t, view.Catalog().Generation, view.Payload())
				require.NoError(t, view.Close())
			}
		}()
	}

	oldResource := initialResource
	for generation := uint64(2); generation <= 50; generation++ {
		newResource := mustIndexResource(t, fmt.Sprintf("generation-%d", generation), nil, nil, nil)
		require.NoError(t, manager.Publish(
			validRootIndexCatalogForTest(t, 2, generation),
			generation,
			[]*IndexGenerationResource{newResource},
			[]*IndexGenerationResource{oldResource},
		))
		oldResource = newResource
	}
	close(stop)
	readers.Wait()
	require.NoError(t, manager.Close())
}
