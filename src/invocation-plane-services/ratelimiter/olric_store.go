/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ratelimiter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/olric-data/olric"
	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/common"
)

// Client is an interface that allows using an Olric client.
type Client interface {
	NewDMap(name string, options ...olric.DMapOption) (olric.DMap, error)
}

// Store is the Olric store.
type Store struct {
	// Prefix used for the key.
	Prefix string
	// dmap is the distributed map instance.
	dmap olric.DMap
}

// GetDMap returns the underlying Olric DMap for metrics collection
func (store *Store) GetDMap() olric.DMap {
	return store.dmap
}

// NewStore returns an instance of Olric store with defaults.
func NewStore(client Client) (limiter.Store, error) {
	return NewStoreWithOptions(client, limiter.StoreOptions{
		Prefix: limiter.DefaultPrefix,
	})
}

// NewStoreWithOptions returns an instance of Olric store with options.
func NewStoreWithOptions(client Client, options limiter.StoreOptions) (limiter.Store, error) {
	dmap, err := client.NewDMap(options.Prefix)
	if err != nil {
		return nil, err
	}

	store := &Store{
		Prefix: options.Prefix,
		dmap:   dmap,
	}

	return store, nil
}

// Increment increments the limit by the given count & gives back the new limit for the given identifier.
// NOT USED
func (store *Store) Increment(ctx context.Context, key string, count int64, rate limiter.Rate) (limiter.Context, error) {
	return limiter.Context{}, errors.ErrUnsupported
}

// maxCompareAndSwapAttempts caps the CompareAndSwap retry loop. A mismatch
// means another check incremented the same key between the read and the swap;
// each retry reuses the authoritative current value returned by the failed
// swap, so the loop converges within a few attempts. Exhausting the cap fails
// the check instead of writing an unvalidated value: Store.Get returns an
// error and the caller denies the request (fail closed).
const maxCompareAndSwapAttempts = 100

// Get returns the limit for the given identifier.
func (store *Store) Get(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	// This logic is from drivers/store/redis/store.go. The counter update is
	// not a Get+Put pair: Get followed by Put is not atomic, so concurrent
	// checks could read the same value and lose one increment. increment
	// below performs the read-modify-write with CompareAndSwap.

	tracer := otel.Tracer("Olric")
	ctx, span := tracer.Start(ctx, "Olric-get")
	defer span.End()
	span.SetAttributes(attribute.String("key", key))

	fullKey := store.getCacheKey(key)
	value, expiration, err := store.increment(ctx, fullKey, rate)
	if err != nil {
		return limiter.Context{}, err
	}
	span.SetAttributes(attribute.String("value", strconv.Itoa(value)))
	span.SetAttributes(attribute.String("expiration", expiration.String()))

	now := time.Now()
	return common.GetContextFromState(now, rate, expiration, int64(value)), nil
}

// counterState is the single serialized value stored per rate-limit key. The
// window expiry rides inside the value so the compare-and-swap expected bytes
// carry the window identity as well as the counter: a retry that paused
// across expiry cannot match a new window that restarted at the same counter
// and write the old expiry over it.
type counterState struct {
	Count  int   `json:"c"`
	Expiry int64 `json:"e"` // window end, absolute Unix milliseconds
}

// decodeCounter reads a stored counter. Values written with the embedded
// window identity decode as counterState; a legacy plain-int counter (written
// by the previous Get+Put code, or seeded without a TTL) falls back to the
// key's stored TTL as its window identity.
func decodeCounter(resp *olric.GetResponse) (counterState, error) {
	var state counterState
	if err := json.Unmarshal(resp.RawValue(), &state); err == nil {
		return state, nil
	}
	if n, err := resp.Int(); err == nil {
		return counterState{Count: n, Expiry: resp.TTL()}, nil
	} else {
		return counterState{}, fmt.Errorf("decode counter value: %w", err)
	}
}

// increment bumps the per-key counter by one and returns the new value with
// the window expiration.
//
// The read-modify-write is a CompareAndSwap retry loop, the same primitive the
// LLM API gateway uses for its buckets (see
// llm-api-gateway/ratelimit/olric_store.go). On a mismatch the failed swap
// returns the authoritative current value, so a retry needs no extra Get.
//
// The first check in a window swaps with a nil expected value, which only
// succeeds when the key does not exist. Counter creation and window TTL
// arming are therefore a single atomic write. dmap.Incr cannot do this: it
// has no TTL option, and arming the TTL in a follow-up call would leave the
// counter without a window whenever that call fails.
//
// A read whose window ends before the swap takes the restart branch instead
// of re-arming a stale expiry: the swap still compares against the old bytes,
// so the write stays atomic, and the partition owner reports an expired key
// as absent, so the next attempt inserts a fresh window.
//
// The window expiry is embedded in the stored value (see counterState), so
// the compare-and-swap expected bytes carry the window identity as well as the
// counter: a retry that paused across expiry cannot match a new window that
// restarted at the same counter and write the old expiry over it.
func (store *Store) increment(ctx context.Context, fullKey string, rate limiter.Rate) (int, time.Time, error) {
	// current is the value the next swap compares against. nil means the key
	// does not exist and the next swap must insert it.
	var current *olric.GetResponse

	for attempt := 0; attempt < maxCompareAndSwapAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, time.Time{}, err
		}
		var (
			expected   []byte
			newValue   []byte
			newCount   int
			options    []olric.PutOption
			expiration time.Time
		)

		// startWindow restarts the counter at 1 and arms the window TTL in
		// the same atomic write.
		startWindow := func() error {
			expiration = time.Now().Add(rate.Period)
			newCount = 1
			b, err := json.Marshal(counterState{Count: newCount, Expiry: expiration.UnixMilli()})
			if err != nil {
				return fmt.Errorf("encode counter for %s: %w", redactKey(fullKey), err)
			}
			newValue = b
			if rate.Period.Milliseconds() > 0 {
				options = append(options, olric.PX(rate.Period))
			}
			return nil
		}

		if current == nil {
			// Insert-if-absent: expected stays nil, so the swap only succeeds
			// when the key does not exist. Counter creation and window TTL
			// arming are a single atomic write.
			if err := startWindow(); err != nil {
				return 0, time.Time{}, err
			}
		} else {
			// The expected bytes embed the window expiry, so a retry that
			// paused across expiry cannot match a new window that restarted
			// at the same counter.
			expected = current.RawValue()
			state, err := decodeCounter(current)
			if err != nil {
				// fullKey embeds the raw per-user subject, so only a short hash
				// prefix may leave the process. The returned error is logged by the
				// caller - do not log it here as well.
				return 0, time.Time{}, fmt.Errorf("decode counter for %s: %w", redactKey(fullKey), err)
			}
			if window := state.Expiry; window > 0 && time.Until(time.UnixMilli(window)) > 0 {
				// Same window: increment in place and keep the absolute expiry.
				// PXAT re-arms the key TTL verbatim, where a remaining-duration
				// round-trip can truncate to zero near the window edge.
				expiration = time.UnixMilli(window)
				newCount = state.Count + 1
				newValue, err = json.Marshal(counterState{Count: newCount, Expiry: window})
				if err != nil {
					return 0, time.Time{}, fmt.Errorf("encode counter for %s: %w", redactKey(fullKey), err)
				}
				if ttl := current.TTL(); ttl > 0 {
					options = append(options, olric.PXAT(time.Duration(ttl)*time.Millisecond))
				}
			} else {
				// The window ended between the read and the swap, or the key
				// has no TTL left: restart the window at 1. The swap still
				// compares against the old bytes, so it stays atomic.
				if err := startWindow(); err != nil {
					return 0, time.Time{}, err
				}
			}
		}

		swapped, next, err := store.dmap.CompareAndSwap(ctx, fullKey, expected, newValue, options...)
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("compare-and-swap counter for %s: %w", redactKey(fullKey), err)
		}
		if swapped {
			return newCount, expiration, nil
		}
		// Another check updated the key first. next holds what is stored now,
		// nil when the key expired in between. Retry against that value.
		current = next
		// Yield for a short random interval, so a hot key with 100+ concurrent
		// checks spreads its retries instead of exhausting the attempt budget
		// in a tight spin.
		timer := time.NewTimer(time.Duration(rand.Int64N(4)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}

	// Attempts exhausted: no log here - the returned error is the single
	// report, with the key redacted so the raw subject never reaches logs.
	return 0, time.Time{}, fmt.Errorf("counter %s did not stabilize after %d compare-and-swap attempts", redactKey(fullKey), maxCompareAndSwapAttempts)
}

// Peek returns the limit for the given identifier, without modification on current values.
// NOT USED
func (store *Store) Peek(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	return limiter.Context{}, errors.ErrUnsupported
}

// Reset returns the limit for the given identifier which is set to zero.
func (store *Store) Reset(ctx context.Context, key string, rate limiter.Rate) (limiter.Context, error) {
	fullKey := store.getCacheKey(key)
	_, err := store.dmap.Delete(ctx, fullKey)
	if err != nil {
		return limiter.Context{}, err
	}

	now := time.Now()
	expiration := now.Add(rate.Period)

	return common.GetContextFromState(now, rate, expiration, 0), nil
}

// redactKey keeps error messages correlatable without exposing the per-user
// subject: fullKey embeds the raw ClientAuthSubject, so only a short hash
// prefix leaves the process.
func redactKey(fullKey string) string {
	sum := sha256.Sum256([]byte(fullKey))
	return hex.EncodeToString(sum[:6])
}

// getCacheKey returns the full path for an identifier.
func (store *Store) getCacheKey(key string) string {
	return store.Prefix + ":" + key
}
