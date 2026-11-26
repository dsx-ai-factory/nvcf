// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package segment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

const (
	// DefaultPrepareAhead bounds how many handed-off segments may wait for
	// submission. Each one costs disk for its transport file.
	DefaultPrepareAhead = 16
	// DefaultBatchSegments is the batcher's default segment count per
	// upload. Prepare-ahead below it could never fill a batch.
	DefaultBatchSegments = 16
)

// Sink receives closed segments. The prepare stage implements it. Accept
// returning nil means the sink owns the segment until it calls
// Intake.Release; an error leaves the segment for the next pass. The sink
// releases every segment it accepted, even one whose file has left the
// spool, or that segment's prepare-ahead slot stays taken.
type Sink interface {
	Accept(context.Context, Segment) error
}

// Gate is the free-space guard. Admit reports whether there is room to
// prepare the segment now. When it refuses, the pass stops handing off, so
// already prepared work can drain first.
type Gate interface {
	Admit(Segment) bool
}

// Config bounds spool intake.
type Config struct {
	// Prefixes lists the segment families to scan. See ValidatePrefixes.
	Prefixes []string
	// PrepareAhead bounds how many segments may be handed off and not yet
	// released at once.
	PrepareAhead int
	// BatchSegments is the batcher's segment count per upload. PrepareAhead
	// must be at least this, or a full batch can never form.
	BatchSegments int
}

// DefaultConfig returns the defaults for the given prefixes.
func DefaultConfig(prefixes ...string) Config {
	return Config{
		Prefixes:      prefixes,
		PrepareAhead:  DefaultPrepareAhead,
		BatchSegments: DefaultBatchSegments,
	}
}

// Validate rejects any setting intake cannot run with.
func (c Config) Validate() error {
	if err := ValidatePrefixes(c.Prefixes); err != nil {
		return err
	}
	if c.BatchSegments < 1 {
		return fmt.Errorf("batch segments must be at least 1, got %d", c.BatchSegments)
	}
	if c.PrepareAhead < c.BatchSegments {
		return fmt.Errorf("prepare ahead (%d) must be at least the batch segment count (%d)", c.PrepareAhead, c.BatchSegments)
	}
	return nil
}

// identity tells one file from a later file that reuses its name. A writer
// restarts its index at the highest existing index plus one, so a name comes
// back only after every higher file is gone; the reused file is new content.
type identity struct {
	size    int64
	modTime time.Time
}

func identify(item Segment) identity {
	return identity{size: item.Size, modTime: item.ModTime}
}

// handoff is a segment handed to the sink and still on the spool.
type handoff struct {
	// handedAs is the file as it was handed off. Release must name it, so
	// a late Release for an earlier file of the same name never frees this
	// hand-off's slot.
	handedAs identity
	// identity is the file as last seen.
	identity identity
	// holds reports that it still holds a prepare-ahead slot.
	holds bool
}

// removedHandoff names a hand-off whose file left the spool before Release.
type removedHandoff struct {
	name     string
	handedAs identity
}

// Intake scans the spool and hands each closed segment to the sink once.
// It never deletes or changes a file. Passes run one at a time; Release may
// be called at any time.
type Intake struct {
	directory string
	cfg       Config
	sink      Sink
	gate      Gate

	pass sync.Mutex

	mu sync.Mutex
	// handed maps the name of every segment handed off and still on the
	// spool to its hand-off.
	handed map[string]handoff
	// removed counts hand-offs whose file left the spool before Release.
	// Each keeps its slot until Release; its name is free for a new file.
	removed map[removedHandoff]int
	ahead   int
}

// PassResult summarizes one intake pass.
type PassResult struct {
	Scan Scan
	// Handed counts segments accepted by the sink in this pass.
	Handed int
	// Deferred counts closed segments left for a later pass because the
	// prepare-ahead bound or the gate stopped the pass.
	Deferred int
	// GateClosed reports that the gate refused a segment in this pass.
	GateClosed bool
	// Changed counts handed-off segments whose size or modification time
	// changed since the hand-off.
	Changed int
}

// NewIntake builds an intake for directory. The configuration is validated
// here, so a bad setting fails at startup.
func NewIntake(directory string, cfg Config, sink Sink, gate Gate) (*Intake, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("spool intake config: %w", err)
	}
	if sink == nil {
		return nil, errors.New("spool intake needs a sink")
	}
	if gate == nil {
		return nil, errors.New("spool intake needs a free-space gate")
	}
	return &Intake{
		directory: directory,
		cfg:       cfg,
		sink:      sink,
		gate:      gate,
		handed:    map[string]handoff{},
		removed:   map[removedHandoff]int{},
	}, nil
}

// Pass scans the spool once and hands off closed segments in order until
// the prepare-ahead bound or the gate stops it. A concurrent call waits for
// the running pass to end.
//
// A handed-off segment is identified by its name until it leaves the spool.
// If its size or modification time changes while it holds a slot, it is
// counted once in PassResult.Changed and not handed off again: the sink owns
// it until Release. A file that changes after Release is new content that
// reuses the name, and is handed off again. A segment whose file leaves the
// spool keeps its slot until Release, because the sink may still hold its
// prepared work; a later file that reuses the name is new work.
func (i *Intake) Pass(ctx context.Context) (PassResult, error) {
	i.pass.Lock()
	defer i.pass.Unlock()
	if err := ctx.Err(); err != nil {
		return PassResult{}, err
	}
	scan, err := ScanDir(i.directory, i.cfg.Prefixes)
	if err != nil {
		return PassResult{}, err
	}
	result := PassResult{Scan: scan}
	i.forgetRemoved(scan)

	pending := make([]Segment, 0, len(scan.Closed))
	for _, item := range scan.Closed {
		handed, changed := i.isHanded(item)
		if changed {
			result.Changed++
			slog.Warn("request trace segment changed after it was handed off",
				"prefix", item.Prefix,
				"segment", item.Index)
		}
		if !handed {
			pending = append(pending, item)
		}
	}
	for n, item := range pending {
		if err := ctx.Err(); err != nil {
			result.Deferred = len(pending) - n
			return result, err
		}
		if !i.takeSlot() {
			result.Deferred = len(pending) - n
			break
		}
		if !i.gate.Admit(item) {
			i.returnSlot()
			result.GateClosed = true
			result.Deferred = len(pending) - n
			break
		}
		// Record the hand-off before the sink sees the segment, so a worker
		// that finishes and calls Release before Accept returns frees the slot.
		i.markHanded(item)
		if err := i.sink.Accept(ctx, item); err != nil {
			i.unmarkHanded(item)
			result.Deferred = len(pending) - n
			return result, fmt.Errorf("hand off request trace segment %d: %w", item.Index, err)
		}
		result.Handed++
		slog.Info("request trace segment found",
			"prefix", item.Prefix,
			"segment", item.Index,
			"bytes", item.Size)
	}
	return result, nil
}

// Release frees the prepare-ahead slot of a handed-off segment. item is the
// Segment the sink was handed, even if the file changed or left the spool
// since. The next stage calls it once the segment no longer waits for
// submission. Releasing twice, or releasing a segment that was never handed
// off, does nothing, and a late Release for an earlier file never frees the
// slot of a later file with the same name.
func (i *Intake) Release(item Segment) {
	i.mu.Lock()
	defer i.mu.Unlock()
	name := filepath.Base(item.Path)
	if h, ok := i.handed[name]; ok && h.holds && h.handedAs == identify(item) {
		h.holds = false
		i.handed[name] = h
		i.ahead--
		return
	}
	key := removedHandoff{name: name, handedAs: identify(item)}
	if n := i.removed[key]; n > 0 {
		if n == 1 {
			delete(i.removed, key)
		} else {
			i.removed[key] = n - 1
		}
		i.ahead--
	}
}

// isHanded reports whether item still belongs to an earlier hand-off, and
// whether it changed while that hand-off holds its slot. A released
// hand-off whose file changed after the release is forgotten: the name now
// holds new content.
func (i *Intake) isHanded(item Segment) (handed, changed bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	name := filepath.Base(item.Path)
	h, ok := i.handed[name]
	switch {
	case !ok:
		return false, false
	case h.identity == identify(item):
		return true, false
	case h.holds:
		// The change is counted once; the file as it is now stays the
		// sink's until Release.
		h.identity = identify(item)
		i.handed[name] = h
		return true, true
	default:
		delete(i.handed, name)
		return false, false
	}
}

func (i *Intake) takeSlot() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.ahead >= i.cfg.PrepareAhead {
		return false
	}
	i.ahead++
	return true
}

func (i *Intake) returnSlot() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.ahead--
}

func (i *Intake) markHanded(item Segment) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.handed[filepath.Base(item.Path)] = handoff{handedAs: identify(item), identity: identify(item), holds: true}
}

// unmarkHanded undoes markHanded for a segment the sink refused, returning
// its slot unless a Release already did.
func (i *Intake) unmarkHanded(item Segment) {
	i.mu.Lock()
	defer i.mu.Unlock()
	name := filepath.Base(item.Path)
	if i.handed[name].holds {
		i.ahead--
	}
	delete(i.handed, name)
}

// forgetRemoved drops segments whose name left the spool, so a later file
// that reuses the name is new work. A removed segment that still holds a
// slot keeps it until Release.
func (i *Intake) forgetRemoved(scan Scan) {
	present := make(map[string]struct{}, len(scan.Closed)+len(scan.Active))
	for _, item := range scan.Closed {
		present[filepath.Base(item.Path)] = struct{}{}
	}
	for _, item := range scan.Active {
		present[filepath.Base(item.Path)] = struct{}{}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for name, h := range i.handed {
		if _, ok := present[name]; ok {
			continue
		}
		if h.holds {
			i.removed[removedHandoff{name: name, handedAs: h.handedAs}]++
		}
		delete(i.handed, name)
	}
}
