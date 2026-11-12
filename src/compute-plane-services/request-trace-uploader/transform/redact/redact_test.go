// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/jsonvalue"
)

// golden holds what the earlier Python uploader produced: the redacted
// record and part count per fixture, and its helper results per string.
// Span offsets are UTF-8 byte offsets.
type golden struct {
	Records []struct {
		Name  string  `json:"name"`
		In    string  `json:"in"`
		Count int     `json:"count"`
		Out   *string `json:"out"`
	} `json:"records"`
	Scans []struct {
		In             string  `json:"in"`
		Spans          [][]any `json:"spans"`
		LooksLikeMedia bool    `json:"looks_like_media"`
		Kind           string  `json:"kind"`
		Sniff          *string `json:"sniff"`
	} `json:"scans"`
	Sniff []struct {
		In   string  `json:"in"`
		MIME *string `json:"mime"`
	} `json:"sniff"`
}

func loadGolden(t *testing.T) golden {
	t.Helper()
	data, err := os.ReadFile("testdata/python.json")
	if err != nil {
		t.Fatal(err)
	}
	var g golden
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	return g
}

// divergentRecords maps golden records where this uploader differs from the
// earlier one on purpose to the mime every marker gets instead. The earlier
// uploader copied an audio format that is not a media type into the marker;
// this one treats it as absent, so the type is sniffed from the media. Only
// the markers' mime differs; kind, bytes and sha256 still match.
var divergentRecords = map[string]string{"audio format final sigma": "audio/wav"}

var markerMIME = regexp.MustCompile(`"mime":(null|"(?:[^"\\]|\\.)*")`)

// TestRecordMatchesPythonUploader checks that redaction finds the same media
// and writes the same markers as the earlier Python uploader. The record is
// compared by keys, key order and values; its bytes are plain JSON.
func TestRecordMatchesPythonUploader(t *testing.T) {
	found := 0
	for _, tc := range loadGolden(t).Records {
		if _, ok := divergentRecords[tc.Name]; ok {
			found++
		}
		t.Run(tc.Name, func(t *testing.T) {
			record, err := jsonvalue.Decode([]byte(tc.In))
			if strings.Contains(tc.In, "NaN") {
				if err == nil {
					t.Fatal("Decode() accepted NaN, which is not JSON")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := string(jsonvalue.Encode(record))
			got, count := Record(record)
			if count != tc.Count {
				t.Fatalf("count = %d, want %d", count, tc.Count)
			}
			encoded := string(jsonvalue.Encode(got))
			if tc.Out == nil {
				if encoded != before {
					t.Fatalf("record changed with nothing redacted:\n%s", encoded)
				}
				return
			}
			expected := *tc.Out
			if mime, ok := divergentRecords[tc.Name]; ok {
				expected = markerMIME.ReplaceAllLiteralString(*tc.Out, `"mime":"`+mime+`"`)
				if expected == *tc.Out {
					t.Fatal("expected output equals the earlier uploader's; remove it from divergentRecords")
				}
			}
			want, err := jsonvalue.Decode([]byte(expected))
			if err != nil {
				t.Fatal(err)
			}
			if !jsonvalue.Equal(got, want) {
				t.Errorf("redacted =\n%s\nwant\n%s", encoded, expected)
			}
		})
	}
	if found != len(divergentRecords) {
		t.Fatalf("golden set holds %d of %d divergent records", found, len(divergentRecords))
	}
}

func optional(s string, ok bool) *string {
	if !ok {
		return nil
	}
	return &s
}

func TestScannersMatchPython(t *testing.T) {
	for _, tc := range loadGolden(t).Scans {
		name := tc.In
		if len(name) > 60 {
			name = name[:60]
		}
		t.Run(name, func(t *testing.T) {
			spans := mediaSpans(tc.In)
			if len(spans) != len(tc.Spans) {
				t.Fatalf("spans = %+v, want %v", spans, tc.Spans)
			}
			for i, want := range tc.Spans {
				got := spans[i]
				wantMIME, _ := want[3].(string)
				if float64(got.start) != want[0] || float64(got.end) != want[1] || got.kind != want[2] || got.mime != wantMIME || got.hasMIME != (want[3] != nil) {
					t.Errorf("span %d = %+v, want %v", i, got, want)
				}
			}
			if got := looksLikeMedia(tc.In); got != tc.LooksLikeMedia {
				t.Errorf("looksLikeMedia = %v, want %v", got, tc.LooksLikeMedia)
			}
			if got := mediaKind(tc.In, ""); got != tc.Kind {
				t.Errorf("mediaKind = %q, want %q", got, tc.Kind)
			}
			if got := optional(sniffMIME(tc.In)); !equalOptional(got, tc.Sniff) {
				t.Errorf("sniffMIME = %v, want %v", deref(got), deref(tc.Sniff))
			}
		})
	}
}

func TestSniffMIMEMatchesPython(t *testing.T) {
	for _, tc := range loadGolden(t).Sniff {
		if got := optional(sniffMIME(tc.In)); !equalOptional(got, tc.MIME) {
			t.Errorf("sniffMIME(%.30q) = %v, want %v", tc.In, deref(got), deref(tc.MIME))
		}
	}
}

func equalOptional(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestLargeRecordIsRedactedQuickly(t *testing.T) {
	png := base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 5_600_000)...))
	raw := base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), []byte(strings.Repeat("Zq9", 940_000))...))
	line, err := json.Marshal(map[string]any{
		"event_type": "request_payload",
		"payload": map[string]any{"request": map[string]any{"messages": []any{
			map[string]any{"role": "user", "content": "see ![x](data:image/png;base64," + png + ") please"},
			map[string]any{"role": "tool", "content": raw},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := jsonvalue.Decode(line)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	got, count := Record(record)
	elapsed := time.Since(start)

	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if size := len(jsonvalue.Encode(got)); size > 4096 {
		t.Errorf("redacted record is %d bytes, want the media replaced", size)
	}
	if elapsed > 10*time.Second {
		t.Errorf("redaction took %v, want well under the 60 s the earlier uploader allowed", elapsed)
	}
}

func TestPyLowerMatchesPython(t *testing.T) {
	data, err := os.ReadFile("testdata/python.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Lower []struct {
			In  string `json:"in"`
			Out string `json:"out"`
		} `json:"lower"`
	}
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Lower) == 0 {
		t.Fatal("no lower cases in testdata")
	}
	for _, tc := range g.Lower {
		if got := pyLower(tc.In); got != tc.Out {
			t.Errorf("pyLower(%+q) = %+q, want %+q", tc.In, got, tc.Out)
		}
	}
}

// TestDataURLScanIsLinear guards against inputs that make each data URL
// candidate rescan the rest of the string. Linear work takes milliseconds
// here; quadratic work takes hours.
func TestDataURLScanIsLinear(t *testing.T) {
	const size = 4 << 20
	for _, unit := range []string{
		"data:;x=", "data:text/plain,", "data:text/plain;x=", `data:;a="b;c";`,
		"data:text/plain;x=data:text/plain,", "data:a/b;x=y,data:", `data:text/plain;q="data:x;y",`, "data: ;data:\t;",
	} {
		t.Run(unit, func(t *testing.T) {
			text := strings.Repeat(unit, size/len(unit))
			done := make(chan struct{})
			go func() {
				redactString(text)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("redacting %d bytes of %q took over 10 s", len(text), unit)
			}
		})
	}
}

func TestRedactedRecordIsPlainJSON(t *testing.T) {
	png := "data:image/png;base64," + base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 64)...))
	record, err := jsonvalue.Decode([]byte("{\"score\":1.50,\"content\":\"caf\u00e9 " + png + "\"}"))
	if err != nil {
		t.Fatal(err)
	}
	got, count := Record(record)
	encoded := string(jsonvalue.Encode(got))
	if count != 1 || !strings.HasPrefix(encoded, "{\"score\":1.50,\"content\":\"caf\u00e9 [redacted:multimodal kind=data_url mime=image/png bytes=") {
		t.Fatalf("redacted = %s, want UTF-8 text, the source number and an inline token", encoded)
	}
}

// TestPyStripMatchesPython uses expected values from Python's str.strip().
func TestPyStripMatchesPython(t *testing.T) {
	tests := []struct{ in, want string }{
		{in: " a ", want: "a"},
		{in: "\t\n x \r\u000b\f", want: "x"},
		{in: "\u001c\u001d\u001e\u001fy\u0085", want: "y"},
		{in: "\u00a0z\u2028\u3000", want: "z"},
		{in: "\u200bq\u200b", want: "\u200bq\u200b"},
		{in: "", want: ""},
	}
	for _, tt := range tests {
		if got := pyStrip(tt.in); got != tt.want {
			t.Errorf("pyStrip(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// White space after the comma of a base64 data URL does not hide the media.
func TestDataURLWithSpaceAfterTheCommaIsRedacted(t *testing.T) {
	png := base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{7, 99, 201}, 300)...))
	for _, url := range []string{"data:image/png;base64, " + png, "data:image/png;base64,\t" + png} {
		record, err := jsonvalue.Decode([]byte(`{"payload":{"request":{"messages":[{"role":"user","content":"` + strings.ReplaceAll(url, "\t", `\t`) + `"}]}}}`))
		if err != nil {
			t.Fatal(err)
		}
		got, count := Record(record)
		if count != 1 || strings.Contains(string(jsonvalue.Encode(got)), png[:40]) {
			t.Errorf("count = %d, want the data URL redacted: %.80s", count, jsonvalue.Encode(got))
		}
	}
}

// A MIME hint is copied into the marker only when it is a media type, so a
// hint that holds media or other text never survives redaction.
func TestMIMEHintThatIsNotAMediaTypeIsNotKept(t *testing.T) {
	const secret = "U0VDUkVU"
	hidden := "data:application/pdf;base64," + secret
	data := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("A"), 120))
	tests := map[string]string{
		"typed image source": `{"type":"image","source":{"type":"base64","media_type":"` + hidden + `","data":"` + data + `"}}`,
		"image_url hint":     `{"type":"image_url","image_url":{"url":"https://example.com/a.png","mime_type":"` + hidden + `"}}`,
		"input_audio format": `{"type":"input_audio","input_audio":{"data":"` + data + `","format":"` + hidden + `"}}`,
		"inline data":        `{"inline_data":{"mime_type":"` + hidden + `","data":"` + data + `"}}`,
		"overlong hint":      `{"type":"image","source":{"type":"base64","media_type":"image/` + strings.Repeat("x", 300) + secret + `","data":"` + data + `"}}`,
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			record, err := jsonvalue.Decode([]byte(in))
			if err != nil {
				t.Fatal(err)
			}
			got, count := Record(record)
			encoded := string(jsonvalue.Encode(got))
			if count < 1 {
				t.Fatalf("count = %d, want the media redacted: %s", count, encoded)
			}
			if strings.Contains(encoded, secret) || !strings.Contains(encoded, `"mime":null`) {
				t.Errorf("redacted = %s, want a marker with mime null and no hint text", encoded)
			}
		})
	}
}

func TestMIMEHintThatIsAMediaTypeIsKept(t *testing.T) {
	data := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("A"), 120))
	tests := map[string]string{
		`{"inline_data":{"mime_type":"audio/L16;codec=pcm;rate=24000","data":"` + data + `"}}`:       "audio/L16;codec=pcm;rate=24000",
		`{"type":"input_audio","input_audio":{"data":"` + data + `","format":"WAV"}}`:                "audio/wav",
		`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + data + `"}}`: "image/png",
	}
	for in, want := range tests {
		record, err := jsonvalue.Decode([]byte(in))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := Record(record)
		if encoded := string(jsonvalue.Encode(got)); !strings.Contains(encoded, `"mime":"`+want+`"`) {
			t.Errorf("redacted = %s, want mime %q", encoded, want)
		}
	}
}
