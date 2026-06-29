package storage

import (
	"errors"
	"testing"
)

func TestOpen_Backends(t *testing.T) {
	t.Run("bbolt", func(t *testing.T) {
		s, err := Open("bbolt", t.TempDir(), "")
		if err != nil || s == nil {
			t.Fatalf("Open(bbolt) = %v, %v; want a store", s, err)
		}
		s.Close()
	})

	t.Run("empty defaults to bbolt", func(t *testing.T) {
		s, err := Open("", t.TempDir(), "")
		if err != nil || s == nil {
			t.Fatalf("Open(\"\") = %v, %v; want bbolt default", s, err)
		}
		s.Close()
	})

	t.Run("aliases", func(t *testing.T) {
		for _, name := range []string{"bolt", "boltdb", "BBolt", " bbolt "} {
			s, err := Open(name, t.TempDir(), "")
			if err != nil || s == nil {
				t.Errorf("Open(%q) = %v, %v; want bbolt", name, s, err)
				continue
			}
			s.Close()
		}
	})

	t.Run("sqlite not implemented", func(t *testing.T) {
		s, err := Open("sqlite", t.TempDir(), "")
		if s != nil || !errors.Is(err, ErrBackendNotImplemented) {
			t.Fatalf("Open(sqlite) = %v, %v; want ErrBackendNotImplemented", s, err)
		}
	})

	t.Run("graviton unsupported", func(t *testing.T) {
		for _, name := range []string{"graviton", "gravdb"} {
			s, err := Open(name, t.TempDir(), "")
			if s != nil || !errors.Is(err, ErrGravitonUnsupported) {
				t.Errorf("Open(%q) = %v, %v; want ErrGravitonUnsupported", name, s, err)
			}
		}
	})

	t.Run("unknown", func(t *testing.T) {
		s, err := Open("nonsense", t.TempDir(), "")
		if s != nil || !errors.Is(err, ErrUnknownBackend) {
			t.Fatalf("Open(nonsense) = %v, %v; want ErrUnknownBackend", s, err)
		}
	})
}
