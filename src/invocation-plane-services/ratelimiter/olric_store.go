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
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"go.uber.org/zap"

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
// the check instead of writing an unvalidated value.
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
			newValue   int
			options    []olric.PutOption
			expiration time.Time
		)

		if current == nil {
			// Insert-if-absent: the first check in a window creates the
			// counter at 1 and arms the window TTL in the same atomic write.
			newValue = 1
			if rate.Period.Milliseconds() > 0 {
				options = append(options, olric.PX(rate.Period))
			}
			expiration = time.Now().Add(rate.Period)
		} else {
			old, err := current.Int()
			if err != nil {
				zap.L().Error("Failed to decode counter value", zap.String("key", fullKey), zap.Error(err))
				return 0, time.Time{}, fmt.Errorf("decode counter for %q: %w", fullKey, err)
			}
			expected = current.RawValue()
			if ttl := current.TTL(); ttl > 0 {
				// Same window: increment in place and keep the absolute expiry.
				// GetResponse.TTL reports the stored expiry as an absolute Unix
				// millisecond timestamp; PXAT re-arms it verbatim, where a
				// remaining-duration round-trip can truncate to zero.
				newValue = old + 1
				options = append(options, olric.PXAT(time.Duration(ttl)*time.Millisecond))
				expiration = time.UnixMilli(ttl)
			} else {
				// No TTL left on the key: restart the window at 1.
				newValue = 1
				if rate.Period.Milliseconds() > 0 {
					options = append(options, olric.PX(rate.Period))
				}
				expiration = time.Now().Add(rate.Period)
			}
		}

		swapped, next, err := store.dmap.CompareAndSwap(ctx, fullKey, expected, newValue, options...)
		if err != nil {
			zap.L().Error("Failed to update value", zap.String("key", fullKey), zap.Error(err))
			return 0, time.Time{}, fmt.Errorf("compare-and-swap counter for %q: %w", fullKey, err)
		}
		if swapped {
			return newValue, expiration, nil
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

	return 0, time.Time{}, fmt.Errorf("counter for %q did not stabilize after %d compare-and-swap attempts", fullKey, maxCompareAndSwapAttempts)
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

// getCacheKey returns the full path for an identifier.
func (store *Store) getCacheKey(key string) string {
	return store.Prefix + ":" + key
}
