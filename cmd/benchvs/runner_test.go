package main

import (
	"path/filepath"
	"testing"
)

func TestLogPathForRunUsesUniqueDBTrialName(t *testing.T) {
	dbDir := filepath.Join("C:", "tmp", "matrix", "db", "workspace", "trial-01")
	got := logPathForRun(filepath.Join("C:", "tmp", "bin", "hypergnomon.exe"), []string{"--db-dir=" + dbDir})
	want := filepath.Join("C:", "tmp", "matrix", "db", "workspace", "trial-01-benchvs.log")
	if got != want {
		t.Fatalf("logPathForRun = %q, want %q", got, want)
	}
}

func TestLogPathForRunFallsBackToBinaryDir(t *testing.T) {
	bin := filepath.Join("C:", "tmp", "bin", "hypergnomon.exe")
	got := logPathForRun(bin, []string{"--fastsync", "--timing"})
	want := filepath.Join("C:", "tmp", "bin", "benchvs.log")
	if got != want {
		t.Fatalf("logPathForRun without --db-dir = %q, want %q", got, want)
	}
}
