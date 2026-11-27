// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: upgrade-lab fetch|audit|probe --route ID --out NEW_DIRECTORY; rehearse --route ID --run-dir NEW_DIRECTORY --run-config PRIVATE_JSON --phase prepare; check-recovery --route ID --baseline PRIVATE_JSON --checkpoint PRIVATE_JSON --out NEW_DIRECTORY")
	}
	action := args[0]
	if action == "rehearse" {
		return runRehearsal(ctx, args[1:])
	}
	if action == "check-recovery" {
		return runRecoveryCheck(args[1:])
	}
	if action != "fetch" && action != "audit" && action != "probe" {
		return fmt.Errorf("unknown action %q", action)
	}
	f := flag.NewFlagSet(action, flag.ContinueOnError)
	contracts := f.String("contracts", "tools/ncp-local-cluster/upgrade-lab/transitions", "transition contract directory")
	routeID := f.String("route", "", "exact route identity")
	inputs := f.String("inputs", "", "verified stack inventory directory")
	out := f.String("out", "", "new private output directory")
	timeout := f.Duration("timeout", 2*time.Minute, "timeout per external command")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || *out == "" || *timeout <= 0 {
		return errors.New("provide --out NEW_DIRECTORY and a positive timeout; no positional arguments are accepted")
	}
	lock, err := loadLock(filepath.Join(*contracts, "artifact-lock.json"))
	if err != nil {
		return err
	}
	route, err := loadRoute(filepath.Join(*contracts, *routeID+".json"), *routeID, lock)
	if err != nil {
		return err
	}
	if action != "fetch" && *inputs == "" {
		return errors.New("provide --inputs from a successful fetch")
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		return fmt.Errorf("output must be new: %w", err)
	}
	if action == "fetch" {
		return fetchInputs(ctx, lock, route, *out, *timeout)
	}
	loaded, err := loadInputs(lock, route, *inputs)
	if err != nil {
		return err
	}
	if action == "audit" {
		report, err := compareRoute(lock, route, loaded)
		if err != nil {
			return err
		}
		if err := writeJSON(filepath.Join(*out, "audit.json"), report); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, "components.md"), []byte(renderComparison(report)), 0600); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(report.Summary)
	}
	return probeRoute(ctx, route, loaded, *out, *timeout, execute)
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
}
