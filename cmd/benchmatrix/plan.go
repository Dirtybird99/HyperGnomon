package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

func defaultTargets() []Target {
	return []Target{
		targetForRef("v1.0.0"),
		targetForRef("origin/main"),
		{Ref: "workspace", Name: "workspace", Workspace: true},
	}
}

func parseTargets(raw string) ([]Target, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultTargets(), nil
	}
	parts := strings.Split(raw, ",")
	targets := make([]Target, 0, len(parts))
	for _, part := range parts {
		target, err := parseTargetSpec(part)
		if err != nil {
			return nil, err
		}
		if target.Ref == "" {
			continue
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no targets selected")
	}
	return targets, nil
}

func targetForRef(ref string) Target {
	target := Target{Ref: ref, Name: ref}
	if ref == "workspace" {
		target.Workspace = true
	}
	return target
}

func parseTargetSpec(raw string) (Target, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Target{}, nil
	}
	left := raw
	var extra []string
	if before, after, ok := strings.Cut(raw, "|"); ok {
		left = strings.TrimSpace(before)
		extra = strings.Fields(strings.TrimSpace(after))
	}
	name := ""
	ref := left
	if before, after, ok := strings.Cut(left, "="); ok {
		name = strings.TrimSpace(before)
		ref = strings.TrimSpace(after)
		if name == "" || ref == "" {
			return Target{}, fmt.Errorf("invalid target spec %q", raw)
		}
	}
	target := targetForRef(ref)
	if name != "" {
		target.Name = name
	}
	target.ExtraArgs = extra
	return target, nil
}

func buildRunSpecs(cfg Config, targets []Target) []RunSpec {
	specs := make([]RunSpec, 0, len(targets)*cfg.Trials)
	if cfg.Trials <= 0 {
		return specs
	}
	for targetIdx, target := range targets {
		safeName := sanitizeName(target.Name)
		for trial := 1; trial <= cfg.Trials; trial++ {
			runIdx := targetIdx*cfg.Trials + trial - 1
			apiPort := cfg.APIPortStart + runIdx
			wsPort := cfg.WSPortStart + runIdx
			dbDir := filepath.Join(cfg.RootDir, "db", safeName, fmt.Sprintf("trial-%02d", trial))
			indexerArgs := []string{
				"--fastsync",
				"--timing",
				"--timing-every=10",
				fmt.Sprintf("--api-address=127.0.0.1:%d", apiPort),
				fmt.Sprintf("--ws-address=127.0.0.1:%d", wsPort),
			}
			if target.SupportsSeedCacheDir {
				// Fresh seed dir per trial: every trial measures the cold
				// full-probe path instead of silently hitting a seed written
				// by an earlier trial or operator run. Listed before
				// ExtraArgs so a target spec can still override it.
				seedDir := filepath.Join(cfg.RootDir, "seed", safeName, fmt.Sprintf("trial-%02d", trial))
				indexerArgs = append(indexerArgs, "--classify-seed-cache-dir="+seedDir)
			}
			indexerArgs = append(indexerArgs, target.ExtraArgs...)
			targetArgs := strings.Join(indexerArgs, " ")
			args := []string{
				"--name=" + target.Name,
				"--target-ref=" + target.Ref,
				fmt.Sprintf("--trial=%d", trial),
				"--binary=" + target.BinaryPath,
				"--daemon=" + cfg.Daemon,
				"--db-dir=" + dbDir,
				fmt.Sprintf("--api-url=http://127.0.0.1:%d", apiPort),
				"--probe-duration=" + cfg.ProbeDuration.String(),
				fmt.Sprintf("--probe-workers=%d", cfg.ProbeWorkers),
				"--probe-paths=" + cfg.ProbePaths,
				"--tip-timeout=" + cfg.TipTimeout.String(),
				"--ready-timeout=" + cfg.ReadyTimeout.String(),
				"--out=" + cfg.RunMarkdownOut,
				"--json-out=" + cfg.JSONOut,
			}
			if cfg.ReadyLogPattern != "" {
				args = append(args, "--ready-log-pattern="+cfg.ReadyLogPattern)
			}
			args = append(args, "--args="+targetArgs)
			specs = append(specs, RunSpec{
				Target:      target,
				Trial:       trial,
				DBDir:       dbDir,
				APIPort:     apiPort,
				WSPort:      wsPort,
				BenchvsArgs: args,
			})
		}
	}
	return specs
}

func sanitizeName(name string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		" ", "_",
		"@", "_",
	)
	out := replacer.Replace(strings.TrimSpace(name))
	if out == "" {
		return "target"
	}
	return out
}
