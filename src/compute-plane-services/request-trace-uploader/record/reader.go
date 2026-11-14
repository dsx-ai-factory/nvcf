// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrLegacyAuditRecord reports a Dynamo v1.3.x AuditRecord. That type was
// removed in v1.4.0, and its shape differs enough that parsing it as a request
// trace record would silently produce empty fields rather than fail.
var ErrLegacyAuditRecord = errors.New("record: Dynamo v1.3.x AuditRecord found, minimum supported version is v1.4.0")

// ErrUnreadable reports a segment with bytes but no readable gzip data.
var ErrUnreadable = errors.New("record: segment has no readable gzip data")

// ErrDamaged reports a gzip member that decoded but failed its checksum, so
// records already read from it may have been changed on disk.
var ErrDamaged = errors.New("record: a gzip member failed its checksum")

// Stats summarizes one pass over a segment.
type Stats struct {
	Records     int
	Unparseable int
	Oversized   int
	ByEventType map[EventType]int
	Unknown     int
	Bytes       int64
	// LostBytes counts compressed bytes that could not be decompressed.
	LostBytes int64
	// Resyncs counts restarts at a later gzip header after a damaged member.
	Resyncs int
	// ChecksumFailures counts gzip members that decoded but failed their
	// checksum.
	ChecksumFailures int
	// UnsupportedVersion counts Dynamo v1.3.x AuditRecords, which are
	// dropped. See ErrLegacyAuditRecord.
	UnsupportedVersion int

	cut int
}

// Reader streams records out of a gzipped JSON-lines segment.
//
// It never holds more than one record in memory, so segment size does not
// bound memory. A record that fails to parse, including one that exceeds the
// size bound or is cut short by a damaged gzip member, is counted and
// skipped; it does not end the scan, because one bad line must not discard
// the other records in a segment.
type Reader struct {
	lines   *Lines
	current *Record
	err     error
	stats   Stats
}

// maxLineBytes bounds a single record. Payload records carry entire request
// and response bodies, so the default scanner limit is far too small.
const maxLineBytes = 64 << 20

// Open starts reading the segment at path.
func Open(path string) (*Reader, error) {
	lines, err := OpenLines(path, LineOptions{MaxLineBytes: maxLineBytes})
	if err != nil {
		return nil, err
	}
	return &Reader{
		lines: lines,
		stats: Stats{ByEventType: map[EventType]int{}},
	}, nil
}

// Next advances to the next record, reporting whether one was read.
func (r *Reader) Next() bool {
	defer r.syncLineStats()
	if r.err != nil {
		return false
	}
	for r.lines.Next() {
		line := r.lines.Line()
		rec, err := parseLine(line)
		if err != nil {
			if errors.Is(err, ErrLegacyAuditRecord) {
				r.stats.UnsupportedVersion++
				continue
			}
			r.stats.Unparseable++
			continue
		}

		r.stats.Records++
		r.stats.Bytes += int64(len(line))
		r.stats.ByEventType[rec.EventType]++
		if !rec.EventType.Known() {
			r.stats.Unknown++
		}
		r.current = rec
		return true
	}
	r.err = r.lines.Err()
	if r.err == nil {
		r.err = r.damage()
	}
	return false
}

// damage reports a pass that read no data, or whose data may have been
// changed on disk.
func (r *Reader) damage() error {
	line := r.lines.Stats()
	switch {
	case line.ChecksumFailures > 0:
		return ErrDamaged
	case line.Lines == 0 && line.Cut == 0 && line.Oversized == 0 && line.LostBytes > 0:
		return ErrUnreadable
	}
	return nil
}

// syncLineStats folds the line reader's losses into the record totals.
func (r *Reader) syncLineStats() {
	line := r.lines.Stats()
	r.stats.Unparseable += line.Oversized - r.stats.Oversized + line.Cut - r.stats.cut
	r.stats.Oversized = line.Oversized
	r.stats.cut = line.Cut
	r.stats.LostBytes = line.LostBytes
	r.stats.Resyncs = line.Resyncs
	r.stats.ChecksumFailures = line.ChecksumFailures
}

// Record returns the record read by the last successful Next.
func (r *Reader) Record() *Record { return r.current }

// Stats returns the running totals for this pass.
func (r *Reader) Stats() Stats { return r.stats }

// Err returns the error that ended the scan, if any: an I/O error,
// ErrUnreadable for a file with bytes but no readable gzip data, or
// ErrDamaged when a gzip member failed its checksum. A scan that ended at the
// end of the segment returns nil. Unparseable records are counted in Stats
// rather than reported here.
func (r *Reader) Err() error { return r.err }

// Sum returns the SHA-256 of the whole file once Next has returned false
// without an error, and nil before that.
func (r *Reader) Sum() []byte { return r.lines.Sum() }

// Close releases the segment.
func (r *Reader) Close() error { return r.lines.Close() }

// envelope is the timestamp-wrapped form some Dynamo sinks emit. The file sink
// writes bare records, so both shapes have to parse.
type envelope struct {
	Timestamp *uint64         `json:"timestamp"`
	Event     json.RawMessage `json:"event"`
}

// legacyAudit is the Dynamo v1.3.x AuditRecord. It is detected, never parsed.
// It has schema_version rather than schema, and no event_type at all.
type legacyAudit struct {
	SchemaVersion *uint32 `json:"schema_version"`
	EventType     *string `json:"event_type"`
}

func parseLine(line []byte) (*Record, error) {
	body := line

	var env envelope
	if err := json.Unmarshal(line, &env); err == nil && len(env.Event) > 0 {
		body = env.Event
	}

	var legacy legacyAudit
	if err := json.Unmarshal(body, &legacy); err == nil {
		if legacy.SchemaVersion != nil && legacy.EventType == nil {
			return nil, ErrLegacyAuditRecord
		}
	}

	var rec Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("parse record: %w", err)
	}
	if rec.EventType == "" {
		return nil, errors.New("parse record: no event_type")
	}

	rec.Raw = append([]byte(nil), body...)
	return &rec, nil
}
