/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package gateway

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/valyala/bytebufferpool"
)

// poisonReleasedRequestBodies makes released buffers unreadable as JSON before
// they return to the pool. Tests set it so any read of a released buffer fails.
var poisonReleasedRequestBodies atomic.Bool

func releaseRequestBodyBuffer(buffer *bytebufferpool.ByteBuffer) {
	if poisonReleasedRequestBodies.Load() {
		for i := range buffer.B {
			buffer.B[i] = 'X'
		}
	}
	buffer.Reset()
	bytebufferpool.Put(buffer)
}

// sharedRequestBodyBuffer is a pooled buffer that several request bodies read
// from: the primary request body, the body forwarded to the LLM Gateway, and
// each shadow replay body. Each reader holds one reference. The buffer returns
// to the pool when the last reader closes, so no reader sees a reused buffer.
type sharedRequestBodyBuffer struct {
	buffer  *bytebufferpool.ByteBuffer
	refs    atomic.Int64
	release func(*bytebufferpool.ByteBuffer)
}

func (s *sharedRequestBodyBuffer) unref() {
	switch refs := s.refs.Add(-1); {
	case refs == 0:
		s.release(s.buffer)
	case refs < 0:
		panic("request body buffer released more than once")
	}
}

// pooledRequestBody is an io.ReadCloser over pieces of a shared pooled buffer
// and, for rewritten bodies, a replacement model value. Close waits for an
// in-flight Read because http.Transport may close a request body from another
// goroutine while it is still writing it.
type pooledRequestBody struct {
	shared *sharedRequestBodyBuffer
	// raw is the whole buffered body. It is nil for rewritten bodies, whose
	// content differs from the buffer.
	raw    []byte
	length int64

	mu     sync.Mutex
	pieces [][]byte
	closed bool
}

func newPooledRequestBody(buffer *bytebufferpool.ByteBuffer, release func(*bytebufferpool.ByteBuffer)) *pooledRequestBody {
	shared := &sharedRequestBodyBuffer{buffer: buffer, release: release}
	shared.refs.Store(1)
	raw := buffer.Bytes()
	return &pooledRequestBody{
		shared: shared,
		raw:    raw,
		length: int64(len(raw)),
		pieces: [][]byte{raw},
	}
}

// readOpenAIRequestBody reads the full request body into a pooled buffer and
// replaces request.Body with a reader over it.
func readOpenAIRequestBody(request *http.Request, bodyDescription string) (*pooledRequestBody, error) {
	if request.Body == nil {
		return nil, fmt.Errorf("expecting %s", bodyDescription)
	}

	buffer := bytebufferpool.Get()
	_, err := io.Copy(buffer, request.Body)
	_ = request.Body.Close()
	if err != nil {
		releaseRequestBodyBuffer(buffer)
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	body := newPooledRequestBody(buffer, releaseRequestBodyBuffer)
	request.Body = body
	return body, nil
}

// bufferedRequestBody returns request.Body when it is a pooled body over the
// whole buffer and otherwise reads the body into one, so callers can rewrite
// the buffered bytes without copying them.
func bufferedRequestBody(request *http.Request) (*pooledRequestBody, error) {
	if body, ok := request.Body.(*pooledRequestBody); ok && body.raw != nil {
		return body, nil
	}
	return readOpenAIRequestBody(request, "request body")
}

// peek returns the whole buffered body. It is valid only while b is open.
func (b *pooledRequestBody) peek() []byte {
	return b.raw
}

// Len returns the number of bytes the body yields when read from the start.
func (b *pooledRequestBody) Len() int64 {
	return b.length
}

func (b *pooledRequestBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	if len(b.pieces) == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && len(b.pieces) > 0 {
		copied := copy(p[n:], b.pieces[0])
		n += copied
		b.pieces[0] = b.pieces[0][copied:]
		if len(b.pieces[0]) == 0 {
			b.pieces = b.pieces[1:]
		}
	}
	return n, nil
}

func (b *pooledRequestBody) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.pieces = nil
	b.mu.Unlock()
	b.shared.unref()
	return nil
}

// withModel returns a new body that reads b with each span replaced by
// modelValue. The new body holds its own reference to the buffer, so it stays
// readable after b closes.
func (b *pooledRequestBody) withModel(spans []byteSpan, modelValue []byte) (*pooledRequestBody, error) {
	if err := b.retain(); err != nil {
		return nil, err
	}
	return b.splice(spans, modelValue), nil
}

// withTopLevelModel returns a new body that reads b with the value of every
// top-level model member replaced by modelName. See withModel.
func (b *pooledRequestBody) withTopLevelModel(modelName string) (*pooledRequestBody, error) {
	// Hold a reference while scanning so the buffer cannot be released under
	// the scan.
	if err := b.retain(); err != nil {
		return nil, err
	}
	spans, err := topLevelModelSpans(b.peek())
	if err != nil {
		b.shared.unref()
		return nil, err
	}
	modelValue, err := encodeModelValue(modelName)
	if err != nil {
		b.shared.unref()
		return nil, err
	}
	return b.splice(spans, modelValue), nil
}

// retain adds a buffer reference for a new body built from b.
func (b *pooledRequestBody) retain() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return http.ErrBodyReadAfterClose
	}
	b.shared.refs.Add(1)
	return nil
}

// splice builds a body over b.raw with each span replaced by modelValue. The
// caller must already hold the reference the new body takes over.
func (b *pooledRequestBody) splice(spans []byteSpan, modelValue []byte) *pooledRequestBody {
	pieces := make([][]byte, 0, 2*len(spans)+1)
	length := 0
	last := 0
	for _, span := range spans {
		pieces = appendPiece(pieces, b.raw[last:span.start])
		pieces = appendPiece(pieces, modelValue)
		length += span.start - last + len(modelValue)
		last = span.end
	}
	pieces = appendPiece(pieces, b.raw[last:])
	length += len(b.raw) - last
	return &pooledRequestBody{
		shared: b.shared,
		length: int64(length),
		pieces: pieces,
	}
}

func appendPiece(pieces [][]byte, piece []byte) [][]byte {
	if len(piece) == 0 {
		return pieces
	}
	return append(pieces, piece)
}

type byteSpan struct {
	start, end int
}

// topLevelModelSpans returns the position of the value of every top-level
// model member in body. Go decoders match keys without regard to case and
// differ on which duplicate wins, so every key that equals model in any case
// is returned and a rewrite leaves no value the gateway did not resolve. It
// scans body in place, so large bodies are not copied.
func topLevelModelSpans(body []byte) ([]byteSpan, error) {
	if !stdjson.Valid(body) {
		return nil, errInvalidJSONBody
	}
	i := skipJSONSpace(body, 0)
	if body[i] != '{' {
		return nil, errors.New("request body is not a JSON object")
	}
	i = skipJSONSpace(body, i+1)
	var spans []byteSpan
	for i < len(body) && body[i] != '}' {
		keyEnd := jsonStringEnd(body, i)
		isModel, err := isModelKey(body[i:keyEnd])
		if err != nil {
			return nil, err
		}
		// Valid JSON puts a colon after the key and a value after the colon.
		valueStart := skipJSONSpace(body, skipJSONSpace(body, keyEnd)+1)
		valueEnd := jsonValueEnd(body, valueStart)
		if isModel {
			spans = append(spans, byteSpan{start: valueStart, end: valueEnd})
		}
		i = skipJSONSpace(body, valueEnd)
		if i < len(body) && body[i] == ',' {
			i = skipJSONSpace(body, i+1)
		}
	}
	if len(spans) == 0 {
		return nil, errors.New("request body has no model member")
	}
	return spans, nil
}

func isModelKey(key []byte) (bool, error) {
	if bytes.IndexByte(key, '\\') < 0 {
		return bytes.EqualFold(key[1:len(key)-1], []byte("model")), nil
	}
	var decoded string
	if err := stdjson.Unmarshal(key, &decoded); err != nil {
		return false, fmt.Errorf("failed to read request body key: %w", err)
	}
	return bytes.EqualFold([]byte(decoded), []byte("model")), nil
}

func skipJSONSpace(body []byte, i int) int {
	for i < len(body) {
		switch body[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// jsonStringEnd returns the index after the closing quote of the string that
// starts at body[start]. body must be valid JSON.
func jsonStringEnd(body []byte, start int) int {
	for i := start + 1; i < len(body); i++ {
		switch body[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}
	return len(body)
}

// jsonValueEnd returns the index after the value that starts at body[start].
// body must be valid JSON.
func jsonValueEnd(body []byte, start int) int {
	switch body[start] {
	case '"':
		return jsonStringEnd(body, start)
	case '{', '[':
		depth := 0
		for i := start; i < len(body); i++ {
			switch body[i] {
			case '"':
				i = jsonStringEnd(body, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return len(body)
	default:
		i := start
		for i < len(body) {
			switch body[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return i
			}
			i++
		}
		return i
	}
}

// replaceTopLevelModel returns a copy of body with the value of every
// top-level model member replaced by modelName and every other byte kept.
func replaceTopLevelModel(body []byte, modelName string) ([]byte, error) {
	spans, err := topLevelModelSpans(body)
	if err != nil {
		return nil, err
	}
	modelValue, err := encodeModelValue(modelName)
	if err != nil {
		return nil, err
	}
	rewritten := make([]byte, 0, len(body)+len(spans)*len(modelValue))
	last := 0
	for _, span := range spans {
		rewritten = append(rewritten, body[last:span.start]...)
		rewritten = append(rewritten, modelValue...)
		last = span.end
	}
	return append(rewritten, body[last:]...), nil
}

func encodeModelValue(modelName string) ([]byte, error) {
	modelValue, err := stdjson.Marshal(modelName)
	if err != nil {
		return nil, fmt.Errorf("failed to encode model name: %w", err)
	}
	return modelValue, nil
}
