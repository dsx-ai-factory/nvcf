// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package journalfs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	jsonExt = ".json"
	tmpExt  = ".tmp"
)

var (
	segmentIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	batchIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// layout names every path under the state directory:
//
//	lock                            spool lock and holder
//	segments/<name>.<id>.json       one entry per segment
//	batches/<batch id>.json         one entry per batch
//	transport/<id>.<dest>.<ext>     prepared files, written by format writers
//	unreadable/<name>/              fenced entries with their source
type layout struct {
	root string
}

func (l layout) lockFile() string   { return filepath.Join(l.root, "lock") }
func (l layout) segments() string   { return filepath.Join(l.root, "segments") }
func (l layout) batches() string    { return filepath.Join(l.root, "batches") }
func (l layout) transport() string  { return filepath.Join(l.root, "transport") }
func (l layout) unreadable() string { return filepath.Join(l.root, "unreadable") }

func (l layout) dirs() []string {
	return []string{l.root, l.segments(), l.batches(), l.transport(), l.unreadable()}
}

// segmentFileName carries the source file name as well as the id, so the
// source can be found and fenced even when the content cannot be parsed.
func segmentFileName(name, id string) string {
	return name + "." + id + jsonExt
}

func parseSegmentFileName(file string) (name, id string, ok bool) {
	base, found := strings.CutSuffix(file, jsonExt)
	if !found {
		return "", "", false
	}
	dot := strings.LastIndexByte(base, '.')
	if dot <= 0 {
		return "", "", false
	}
	name, id = base[:dot], base[dot+1:]
	if !segmentIDPattern.MatchString(id) || !validSegmentName(name) {
		return "", "", false
	}
	return name, id, true
}

func batchFileName(id string) string { return id + jsonExt }

func parseBatchFileName(file string) (string, bool) {
	id, found := strings.CutSuffix(file, jsonExt)
	return id, found && batchIDPattern.MatchString(id)
}

func validSegmentName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\`) && !strings.ContainsRune(name, 0)
}

// TransportPath is where a format writer must put the transport for one
// segment and destination. The name starts with the segment id, so recovery
// can remove the files of an interrupted prepare. ext includes its dot.
func TransportPath(transportDir, segmentID, destination, ext string) string {
	sum := sha256.Sum256([]byte(destination))
	return filepath.Join(transportDir, segmentID+"."+hex.EncodeToString(sum[:8])+ext)
}

// segmentTransports lists every transport of one segment, finished or not.
// It reads the directory rather than globbing, so characters in the spool
// path are never read as a pattern. A missing directory has none.
func segmentTransports(transportDir, segmentID string) ([]string, error) {
	entries, err := os.ReadDir(transportDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), segmentID+".") {
			paths = append(paths, filepath.Join(transportDir, entry.Name()))
		}
	}
	return paths, nil
}
