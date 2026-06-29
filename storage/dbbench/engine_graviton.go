package dbbench

import (
	"bytes"
	"errors"
	"path/filepath"

	"github.com/deroproject/graviton"
)

const gravTree = "bench"

type gravEngine struct {
	store *graviton.Store
	tree  *graviton.Tree
}

func openGraviton(dir string) (engine, error) {
	store, err := graviton.NewDiskStore(filepath.Join(dir, "grav"))
	if err != nil {
		return nil, err
	}
	ss, err := store.LoadSnapshot(0)
	if err != nil {
		store.Close()
		return nil, err
	}
	tree, err := ss.GetTree(gravTree)
	if err != nil {
		store.Close()
		return nil, err
	}
	return &gravEngine{store, tree}, nil
}

func (e *gravEngine) name() string { return "graviton" }

func (e *gravEngine) batchWrite(pairs []kv) error {
	for _, p := range pairs {
		if err := e.tree.Put(p.k, p.v); err != nil {
			return err
		}
	}
	_, err := graviton.Commit(e.tree)
	return err
}

func (e *gravEngine) get(key []byte) ([]byte, error) {
	v, err := e.tree.Get(key)
	if errors.Is(err, graviton.ErrNotFound) { // graviton wraps the sentinel with %w
		return nil, nil
	}
	return v, err
}

// rangeScan is graviton's structural weakness: the tree iterates in key-HASH
// order, not key order, so there is no ordered seek. A height window can only be
// found by traversing every key and filtering — O(N), not O(window). This is
// exactly why storage/factory.go rejects graviton for HyperGnomon's Route B scans.
func (e *gravEngine) rangeScan(lo, hi []byte) (int, error) {
	n := 0
	c := e.tree.Cursor()
	for k, v, err := c.First(); err == nil; k, v, err = c.Next() {
		if bytes.Compare(k, lo) >= 0 && bytes.Compare(k, hi) < 0 {
			n++
			sink += len(v)
		}
	}
	return n, nil
}

func (e *gravEngine) scanAll() (int, error) {
	n := 0
	c := e.tree.Cursor()
	for _, v, err := c.First(); err == nil; _, v, err = c.Next() {
		n++
		sink += len(v)
	}
	return n, nil
}

func (e *gravEngine) close() error {
	e.store.Close()
	return nil
}
