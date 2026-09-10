package accountsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrIndexViewManagerClosed  = errors.New("accountsdb: index view manager is shut down")
	ErrIndexGenerationRejected = errors.New("accountsdb: index generation rejected")
	ErrIndexResourceReleased   = errors.New("accountsdb: index generation resource is already released")
)

// IndexGenerationResource is a shareable lifetime handle for an immutable
// mmap, file descriptor, or other generation-owned object. Several catalog
// generations may retain the same resource. It is closed after the final
// retaining generation drains; it is deleted only when a successful
// publication explicitly marks it obsolete.
//
// closeFn and deleteFn run asynchronously from publication. deleteFn runs only
// after closeFn, and only for a resource supplied in Publish's obsolete list.
type IndexGenerationResource struct {
	name     string
	value    any
	closeFn  func() error
	deleteFn func() error

	mu        sync.Mutex
	refs      uint64
	finalized bool
	obsolete  bool
	done      chan struct{}
	err       error
}

func NewIndexGenerationResource(
	name string,
	value any,
	closeFn func() error,
	deleteFn func() error,
) (*IndexGenerationResource, error) {
	if name == "" {
		return nil, errors.New("accountsdb: empty index generation resource name")
	}
	return &IndexGenerationResource{
		name: name, value: value, closeFn: closeFn, deleteFn: deleteFn, done: make(chan struct{}),
	}, nil
}

func (resource *IndexGenerationResource) Name() string {
	if resource == nil {
		return ""
	}
	return resource.name
}

// Value returns the immutable object guarded by this lifetime handle. Callers
// must access it only while holding an IndexReadView that retains the resource.
func (resource *IndexGenerationResource) Value() any {
	if resource == nil {
		return nil
	}
	return resource.value
}

func (resource *IndexGenerationResource) Done() <-chan struct{} {
	if resource == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return resource.done
}

func (resource *IndexGenerationResource) Err() error {
	if resource == nil {
		return nil
	}
	resource.mu.Lock()
	defer resource.mu.Unlock()
	return resource.err
}

func (resource *IndexGenerationResource) retain() error {
	resource.mu.Lock()
	defer resource.mu.Unlock()
	if resource.finalized {
		return fmt.Errorf("%w: %s", ErrIndexResourceReleased, resource.name)
	}
	resource.refs++
	return nil
}

func (resource *IndexGenerationResource) markObsolete() error {
	resource.mu.Lock()
	defer resource.mu.Unlock()
	if resource.finalized {
		return fmt.Errorf("%w: cannot obsolete %s", ErrIndexResourceReleased, resource.name)
	}
	resource.obsolete = true
	return nil
}

// release closes, and optionally deletes, a resource when its last generation
// owner drains. It is called only by asynchronous generation finalizers.
func (resource *IndexGenerationResource) release() error {
	resource.mu.Lock()
	if resource.refs == 0 {
		resource.mu.Unlock()
		return fmt.Errorf("accountsdb: index resource %s reference underflow", resource.name)
	}
	resource.refs--
	if resource.refs != 0 {
		resource.mu.Unlock()
		return nil
	}
	resource.finalized = true
	closeFn, deleteFn, obsolete := resource.closeFn, resource.deleteFn, resource.obsolete
	resource.mu.Unlock()

	var cleanupErr error
	if closeFn != nil {
		cleanupErr = errors.Join(cleanupErr, closeFn())
	}
	if obsolete && deleteFn != nil {
		cleanupErr = errors.Join(cleanupErr, deleteFn())
	}

	resource.mu.Lock()
	resource.err = cleanupErr
	close(resource.done)
	resource.mu.Unlock()
	return cleanupErr
}

type managedIndexGeneration struct {
	catalog   *RootIndexCatalog
	payload   any
	resources []*IndexGenerationResource
	byName    map[string]*IndexGenerationResource
	manager   *IndexViewManager
	refs      atomic.Int64
	queued    atomic.Bool
}

func prepareManagedIndexGeneration(
	catalog *RootIndexCatalog,
	payload any,
	resources []*IndexGenerationResource,
) (*managedIndexGeneration, error) {
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	generation := &managedIndexGeneration{
		catalog:   catalog.Clone(),
		payload:   payload,
		resources: append([]*IndexGenerationResource(nil), resources...),
		byName:    make(map[string]*IndexGenerationResource, len(resources)),
	}
	for _, resource := range resources {
		if resource == nil {
			return nil, fmt.Errorf("%w: nil generation resource", ErrIndexGenerationRejected)
		}
		if _, duplicate := generation.byName[resource.name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate generation resource name %q",
				ErrIndexGenerationRejected,
				resource.name,
			)
		}
		generation.byName[resource.name] = resource
	}

	retained := make([]*IndexGenerationResource, 0, len(generation.resources))
	for _, resource := range generation.resources {
		if err := resource.retain(); err != nil {
			// No generation owns these references if construction fails. Release
			// them before returning so shutdown can never race untracked cleanup.
			var cleanupErr error
			for _, retainedResource := range retained {
				cleanupErr = errors.Join(cleanupErr, retainedResource.release())
			}
			return nil, errors.Join(err, cleanupErr)
		}
		retained = append(retained, resource)
	}
	// One reference represents ownership by the manager's current pointer.
	generation.refs.Store(1)
	return generation, nil
}

func (generation *managedIndexGeneration) release() {
	remaining := generation.refs.Add(-1)
	if remaining < 0 {
		panic("accountsdb: index generation reference underflow")
	}
	if remaining != 0 || !generation.queued.CompareAndSwap(false, true) {
		return
	}
	go generation.manager.finalizeGeneration(generation)
}

// IndexReadView pins one coherent root catalog and its immutable resources.
// Close is idempotent. A view must not be used concurrently with its Close.
type IndexReadView struct {
	generation *managedIndexGeneration
	closeOnce  sync.Once
	closed     atomic.Bool
}

// Catalog returns the immutable catalog selected for this view. Callers must
// not modify it or any element of its Shards slice.
func (view *IndexReadView) Catalog() *RootIndexCatalog {
	if view == nil || view.generation == nil || view.closed.Load() {
		return nil
	}
	return view.generation.catalog
}

// Payload returns the generation-specific immutable runtime object supplied
// to NewIndexViewManager or Publish.
func (view *IndexReadView) Payload() any {
	if view == nil || view.generation == nil || view.closed.Load() {
		return nil
	}
	return view.generation.payload
}

func (view *IndexReadView) Resource(name string) (*IndexGenerationResource, bool) {
	if view == nil || view.generation == nil || view.closed.Load() {
		return nil, false
	}
	resource, ok := view.generation.byName[name]
	return resource, ok
}

func (view *IndexReadView) Close() error {
	if view == nil {
		return nil
	}
	view.closeOnce.Do(func() {
		view.closed.Store(true)
		if view.generation != nil {
			view.generation.release()
		}
	})
	return nil
}

// IndexViewManager atomically publishes immutable index generations. Acquire
// and Publish hold a small mutex only long enough to change a pointer and a
// reference count. Closing mappings and deleting retired files always happens
// asynchronously after all readers of the old generation have released it.
type IndexViewManager struct {
	mu sync.Mutex

	current         *managedIndexGeneration
	shuttingDown    bool
	liveGenerations uint64
	// inFlightPublications covers a Publish call while it prepares a candidate
	// outside manager.mu. pendingFinalizers covers rejected candidates whose
	// retained resources are being released asynchronously. Both are part of
	// the shutdown drain: the store lock must not be released while either can
	// still touch a generation-owned mapping or file.
	inFlightPublications uint64
	pendingFinalizers    uint64
	drained              chan struct{}
	drainedClosed        bool
	cleanupErr           error
}

func NewIndexViewManager(
	catalog *RootIndexCatalog,
	payload any,
	resources []*IndexGenerationResource,
) (*IndexViewManager, error) {
	generation, err := prepareManagedIndexGeneration(catalog, payload, resources)
	if err != nil {
		return nil, err
	}
	manager := &IndexViewManager{
		current:         generation,
		liveGenerations: 1,
		drained:         make(chan struct{}),
	}
	generation.manager = manager
	return manager, nil
}

func (manager *IndexViewManager) Acquire() (*IndexReadView, error) {
	if manager == nil {
		return nil, ErrIndexViewManagerClosed
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.shuttingDown || manager.current == nil {
		return nil, ErrIndexViewManagerClosed
	}
	manager.current.refs.Add(1)
	return &IndexReadView{generation: manager.current}, nil
}

// Publish selects next for new readers and retires the previous generation.
// It does not wait for readers, resource Close calls, or file deletion.
//
// obsolete lists resources belonging to the previous generation whose files
// may be deleted after their final retaining generation drains. A resource
// retained by next cannot simultaneously be obsolete.
func (manager *IndexViewManager) Publish(
	next *RootIndexCatalog,
	payload any,
	resources []*IndexGenerationResource,
	obsolete []*IndexGenerationResource,
) error {
	if manager == nil {
		return ErrIndexViewManagerClosed
	}
	manager.mu.Lock()
	if manager.shuttingDown || manager.current == nil {
		manager.mu.Unlock()
		// Preserve Publish's ownership contract for candidate resources while
		// ensuring no asynchronous cleanup begins after Shutdown has drained.
		generation, err := prepareManagedIndexGeneration(next, payload, resources)
		if err == nil {
			for _, resource := range generation.resources {
				err = errors.Join(err, resource.release())
			}
		}
		return errors.Join(ErrIndexViewManagerClosed, err)
	}
	manager.inFlightPublications++
	manager.mu.Unlock()

	generation, err := prepareManagedIndexGeneration(next, payload, resources)
	if err != nil {
		manager.mu.Lock()
		manager.finishPublicationLocked()
		manager.mu.Unlock()
		return err
	}
	generation.manager = manager

	manager.mu.Lock()
	reject := func(err error) error {
		manager.pendingFinalizers++
		manager.finishPublicationLocked()
		manager.mu.Unlock()
		go manager.finalizeUnpublishedGeneration(generation)
		return err
	}
	if manager.shuttingDown || manager.current == nil {
		return reject(ErrIndexViewManagerClosed)
	}
	old := manager.current
	if old.catalog.Generation == ^uint64(0) || generation.catalog.Generation != old.catalog.Generation+1 {
		return reject(fmt.Errorf(
			"%w: generation %d is not the immediate successor of current generation %d",
			ErrIndexGenerationRejected,
			generation.catalog.Generation,
			old.catalog.Generation,
		))
	}
	if generation.catalog.ShardCount != old.catalog.ShardCount {
		return reject(fmt.Errorf(
			"%w: shard count changed from %d to %d without a store migration",
			ErrIndexGenerationRejected,
			old.catalog.ShardCount,
			generation.catalog.ShardCount,
		))
	}
	oldResources := make(map[*IndexGenerationResource]struct{}, len(old.resources))
	newResources := make(map[*IndexGenerationResource]struct{}, len(generation.resources))
	for _, resource := range old.resources {
		oldResources[resource] = struct{}{}
	}
	for _, resource := range generation.resources {
		newResources[resource] = struct{}{}
	}
	seenObsolete := make(map[*IndexGenerationResource]struct{}, len(obsolete))
	for _, resource := range obsolete {
		if resource == nil {
			return reject(fmt.Errorf("%w: nil obsolete resource", ErrIndexGenerationRejected))
		}
		if _, duplicate := seenObsolete[resource]; duplicate {
			return reject(fmt.Errorf("%w: duplicate obsolete resource %q", ErrIndexGenerationRejected, resource.name))
		}
		seenObsolete[resource] = struct{}{}
		if _, belongedToOld := oldResources[resource]; !belongedToOld {
			return reject(fmt.Errorf("%w: obsolete resource %q is not in the current generation", ErrIndexGenerationRejected, resource.name))
		}
		if _, retainedByNext := newResources[resource]; retainedByNext {
			return reject(fmt.Errorf("%w: resource %q is both retained and obsolete", ErrIndexGenerationRejected, resource.name))
		}
		if _, sameNameRetainedByNext := generation.byName[resource.name]; sameNameRetainedByNext {
			return reject(fmt.Errorf(
				"%w: obsolete resource name %q is reused by the replacement generation",
				ErrIndexGenerationRejected,
				resource.name,
			))
		}
	}
	// old still holds its manager reference, so marking cannot race its final
	// cleanup. No fallible operation follows the marks.
	for _, resource := range obsolete {
		if err := resource.markObsolete(); err != nil {
			return reject(err)
		}
	}
	manager.current = generation
	manager.liveGenerations++
	manager.finishPublicationLocked()
	manager.mu.Unlock()

	old.release()
	return nil
}

func (manager *IndexViewManager) finalizeUnpublishedGeneration(generation *managedIndexGeneration) {
	var cleanupErr error
	for _, resource := range generation.resources {
		cleanupErr = errors.Join(cleanupErr, resource.release())
	}
	manager.mu.Lock()
	if manager.pendingFinalizers == 0 {
		manager.mu.Unlock()
		panic("accountsdb: pending index finalizer underflow")
	}
	manager.pendingFinalizers--
	manager.cleanupErr = errors.Join(manager.cleanupErr, cleanupErr)
	manager.maybeCloseDrainedLocked()
	manager.mu.Unlock()
}

func (manager *IndexViewManager) finalizeGeneration(generation *managedIndexGeneration) {
	var cleanupErr error
	for _, resource := range generation.resources {
		cleanupErr = errors.Join(cleanupErr, resource.release())
	}

	manager.mu.Lock()
	if manager.liveGenerations == 0 {
		manager.mu.Unlock()
		panic("accountsdb: live index generation underflow")
	}
	manager.liveGenerations--
	manager.cleanupErr = errors.Join(manager.cleanupErr, cleanupErr)
	manager.maybeCloseDrainedLocked()
	manager.mu.Unlock()
}

func (manager *IndexViewManager) finishPublicationLocked() {
	if manager.inFlightPublications == 0 {
		panic("accountsdb: in-flight index publication underflow")
	}
	manager.inFlightPublications--
	manager.maybeCloseDrainedLocked()
}

func (manager *IndexViewManager) maybeCloseDrainedLocked() {
	if manager.shuttingDown &&
		manager.liveGenerations == 0 &&
		manager.inFlightPublications == 0 &&
		manager.pendingFinalizers == 0 &&
		!manager.drainedClosed {
		close(manager.drained)
		manager.drainedClosed = true
	}
}

// Shutdown refuses new readers/publications, retires the current generation,
// and waits for every read view and asynchronous cleanup operation. If ctx
// expires, shutdown remains in progress; calling Shutdown again waits for the
// same drain and returns accumulated cleanup errors when it completes.
func (manager *IndexViewManager) Shutdown(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("accountsdb: nil index view shutdown context")
	}

	manager.mu.Lock()
	var retiring *managedIndexGeneration
	if !manager.shuttingDown {
		manager.shuttingDown = true
		retiring = manager.current
		manager.current = nil
		manager.maybeCloseDrainedLocked()
	}
	drained := manager.drained
	manager.mu.Unlock()
	if retiring != nil {
		retiring.release()
	}

	select {
	case <-drained:
		manager.mu.Lock()
		err := manager.cleanupErr
		manager.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close performs an unbounded orderly shutdown. Call Shutdown with a deadline
// when leaked reader detection is preferable to waiting indefinitely.
func (manager *IndexViewManager) Close() error {
	return manager.Shutdown(context.Background())
}

func (manager *IndexViewManager) Done() <-chan struct{} {
	if manager == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return manager.drained
}
