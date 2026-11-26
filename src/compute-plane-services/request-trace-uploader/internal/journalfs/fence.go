// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package journalfs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
)

// fenceSegment moves an unreadable journal file, its source and its
// transports to unreadable/<name>.<id>/, so the source is never discovered
// and submitted again as new work.
//
// The fence directory is linked durably before anything moves into it, and
// the journal file moves last: a crash part way leaves the journal file in
// place, and the next start finds the source already in the fence and
// finishes the move. A source already in the fence is never replaced; a
// file under the same name in the spool is then a new segment and stays for
// upload.
//
// The file name always identifies the source here; loadSegments stops the
// spool with ErrUnreadableState when it does not. Dynamo reuses names, so
// the spool file is moved only when its content gives the entry's id; a
// file with other content under that name is a new segment and stays for
// upload. A source that is gone left nothing to upload again, so the
// journal file and transports are fenced alone.
func (s *Store) fenceSegment(file, name, id string) error {
	fence := filepath.Join(s.layout.unreadable(), name+"."+id)
	source := filepath.Join(s.cfg.SpoolDir, name)
	sourceInSpool, err := present(source)
	if err != nil {
		return err
	}
	reused := false
	if sourceInSpool {
		same, err := hasSegmentID(source, name, id)
		if err != nil {
			return err
		}
		sourceInSpool, reused = same, !same
	}
	sourceFenced, err := present(filepath.Join(fence, name))
	if err != nil {
		return err
	}
	if err := mkdirDurable(s.hooks, fence); err != nil {
		return err
	}
	moves, err := segmentTransports(s.layout.transport(), id)
	if err != nil {
		return fmt.Errorf("list transports of unreadable journal entry: %w", err)
	}
	if sourceInSpool && !sourceFenced {
		moves = append(moves, source)
	}
	for _, path := range moves {
		if err := s.moveInto(path, fence); err != nil {
			return err
		}
	}
	// The source and transport moves are durable before the journal file
	// moves, so the journal file never leaves the spool while the source
	// may still be there after a power loss. unreadable/ is synced too: a
	// fence directory left by an earlier start may not be durable yet.
	for _, dir := range []string{s.layout.unreadable(), fence, s.layout.transport(), s.cfg.SpoolDir} {
		if err := syncDirWith(s.hooks, dir); err != nil {
			return err
		}
	}
	if err := s.moveInto(filepath.Join(s.layout.segments(), file), fence); err != nil {
		return err
	}
	for _, dir := range []string{fence, s.layout.segments()} {
		if err := syncDirWith(s.hooks, dir); err != nil {
			return err
		}
	}
	s.counter.Add(CounterUnreadable, 1)
	s.logger.Warn("fenced an unreadable journal entry",
		"segment_id", shortID(id),
		"source_present", sourceInSpool || sourceFenced,
		"name_reused", reused)
	return nil
}

// dynamoSegmentName matches <prefix>.<index>.jsonl.gz, the name Dynamo gives
// a request-trace segment.
var dynamoSegmentName = regexp.MustCompile(`^(.+)\.(\d+)\.jsonl\.gz$`)

// hasSegmentID reports whether the file at path, named name, has the
// journal id id: the id derives from the prefix and index in the name and
// the size and sha256 of the content. A name that is not a segment name
// never matches.
func hasSegmentID(path, name, id string) (bool, error) {
	parts := dynamoSegmentName.FindStringSubmatch(name)
	if parts == nil {
		return false, nil
	}
	index, err := strconv.Atoi(parts[2])
	if err != nil {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open source for fencing: %w", err)
	}
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, f)
	if err != nil {
		return false, fmt.Errorf("hash source for fencing: %w", err)
	}
	return journal.SegmentID(parts[1], index, size, hex.EncodeToString(hash.Sum(nil))) == id, nil
}

// fenceBatch moves an unreadable batch file aside. Its members are batched
// again by Recover, because the batch file is the only record of whether it
// was submitted.
func (s *Store) fenceBatch(file string) error {
	dir := filepath.Join(s.layout.unreadable(), "batches")
	if err := mkdirDurable(s.hooks, dir); err != nil {
		return err
	}
	if err := s.moveInto(filepath.Join(s.layout.batches(), file), dir); err != nil {
		return err
	}
	for _, d := range []string{s.layout.unreadable(), dir, s.layout.batches()} {
		if err := syncDirWith(s.hooks, d); err != nil {
			return err
		}
	}
	s.counter.Add(CounterUnreadable, 1)
	s.logger.Warn("fenced an unreadable batch entry")
	return nil
}

// moveInto renames path into dir without replacing anything there: a name
// already taken gets a numbered suffix. A path already gone is skipped, so an
// interrupted fence can be repeated.
func (s *Store) moveInto(path, dir string) error {
	if found, err := present(path); err != nil {
		return err
	} else if !found {
		return nil
	}
	target := filepath.Join(dir, filepath.Base(path))
	for n := 1; ; n++ {
		taken, err := present(target)
		if err != nil {
			return err
		}
		if !taken {
			break
		}
		target = filepath.Join(dir, filepath.Base(path)+"."+strconv.Itoa(n))
	}
	if err := os.Rename(path, target); err != nil {
		return fmt.Errorf("fence unreadable journal entry: %w", err)
	}
	s.hooks.emit("rename", target)
	return nil
}

func present(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat for fencing: %w", err)
}
