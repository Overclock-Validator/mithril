package accountsdb

import (
	"container/list"
	"errors"
	"os"
	"sync"
)

const defaultAppendVecFileCacheCapacity = 256

type appendVecFileCache struct {
	mu      sync.Mutex
	maxOpen int
	entries map[string]*appendVecFileCacheEntry
	lru     *list.List
}

type appendVecFileCacheEntry struct {
	path    string
	file    *os.File
	refs    int
	element *list.Element
}

func newAppendVecFileCache(maxOpen int) *appendVecFileCache {
	if maxOpen <= 0 {
		maxOpen = defaultAppendVecFileCacheCapacity
	}

	return &appendVecFileCache{
		maxOpen: maxOpen,
		entries: make(map[string]*appendVecFileCacheEntry, maxOpen),
		lru:     list.New(),
	}
}

func (c *appendVecFileCache) Acquire(path string) (*os.File, func(), error) {
	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		entry.refs++
		c.lru.MoveToFront(entry.element)
		file := entry.file
		c.mu.Unlock()
		return file, func() { c.release(path) }, nil
	}
	c.mu.Unlock()

	file, err := os.OpenFile(path, os.O_RDWR, 0o666)
	if err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	if entry, ok := c.entries[path]; ok {
		entry.refs++
		c.lru.MoveToFront(entry.element)
		c.mu.Unlock()
		_ = file.Close()
		return entry.file, func() { c.release(path) }, nil
	}

	entry := &appendVecFileCacheEntry{
		path: path,
		file: file,
		refs: 1,
	}
	entry.element = c.lru.PushFront(entry)
	c.entries[path] = entry
	toClose := c.trimLocked()
	c.mu.Unlock()

	closeFiles(toClose)
	return file, func() { c.release(path) }, nil
}

func (c *appendVecFileCache) release(path string) {
	c.mu.Lock()
	entry, ok := c.entries[path]
	if !ok {
		c.mu.Unlock()
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	toClose := c.trimLocked()
	c.mu.Unlock()

	closeFiles(toClose)
}

func (c *appendVecFileCache) Close() error {
	c.mu.Lock()
	toClose := make([]*os.File, 0, len(c.entries))
	for _, entry := range c.entries {
		toClose = append(toClose, entry.file)
	}
	c.entries = make(map[string]*appendVecFileCacheEntry)
	c.lru.Init()
	c.mu.Unlock()

	var err error
	for _, file := range toClose {
		err = errors.Join(err, file.Close())
	}
	return err
}

func (c *appendVecFileCache) trimLocked() []*os.File {
	var toClose []*os.File
	for len(c.entries) > c.maxOpen {
		var victim *appendVecFileCacheEntry
		for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
			entry := elem.Value.(*appendVecFileCacheEntry)
			if entry.refs == 0 {
				victim = entry
				break
			}
		}
		if victim == nil {
			break
		}

		delete(c.entries, victim.path)
		c.lru.Remove(victim.element)
		toClose = append(toClose, victim.file)
	}
	return toClose
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}
