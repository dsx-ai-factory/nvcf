// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A small client of etcd's JSON gateway (the /v3/... HTTP API every etcd
// v3.5 serves on its client port), for the lease and key operations a
// Dynamo worker's identity restore needs (identity_etcd.go). The gateway
// keeps the agent free of the etcd client module and its gRPC stack; every
// call is one POST with a JSON body, as `etcdctl` would send over gRPC.
// Numbers that are int64 in etcd's protobufs travel as JSON strings.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// errLeaseExists is etcd refusing a LeaseGrant of an ID that is live:
// another process holds the identity.
var errLeaseExists = errors.New("etcd: lease already exists")

// etcdCallTimeout bounds one gateway call.
const etcdCallTimeout = 5 * time.Second

// etcdKV is one key of a range, with the lease it is attached to (0: none).
type etcdKV struct {
	Key   []byte
	Value []byte
	Lease int64
}

// etcdLeaseInfo is a LeaseTimeToLive answer. TTL is -1 for a lease etcd
// does not hold (expired, revoked or never granted).
type etcdLeaseInfo struct {
	TTL        int64
	GrantedTTL int64
	Keys       [][]byte
}

// identityEtcd is what the identity restore asks of a registry.
type identityEtcd interface {
	RangePrefix(ctx context.Context, prefix []byte) ([]etcdKV, error)
	Get(ctx context.Context, key []byte) (*etcdKV, error)
	TimeToLive(ctx context.Context, id int64, keys bool) (etcdLeaseInfo, error)
	Grant(ctx context.Context, id, ttl int64) error
	Put(ctx context.Context, key, value []byte, lease int64) error
	KeepAlive(ctx context.Context, id int64) (ttl int64, err error)
	Revoke(ctx context.Context, id int64) error
}

// etcdGateway calls etcd's JSON gateway at each of bases in turn until one
// answers.
type etcdGateway struct {
	bases  []string
	client *http.Client
}

// etcdGatewayBases turns a Dynamo ETCD_ENDPOINTS value ("host:port", or a
// comma-separated list, with or without "http://") into gateway base URLs.
// A host without a dot is a Service of namespace ns; "svc.ns" is qualified
// too, since the agent resolves names in its own namespace. A TLS endpoint
// is refused: the agent holds no client certificate for it.
func etcdGatewayBases(endpoints, ns string) ([]string, error) {
	var out []string
	for _, ep := range strings.Split(endpoints, ",") {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		if !strings.Contains(ep, "://") {
			ep = "http://" + ep
		}
		u, err := url.Parse(ep)
		if err != nil {
			return nil, fmt.Errorf("etcd endpoint %q: %w", ep, err)
		}
		if u.Scheme != "http" {
			return nil, fmt.Errorf("etcd endpoint %q: scheme %q is not supported (only plain http)", ep, u.Scheme)
		}
		host, port := u.Hostname(), u.Port()
		if host == "" {
			return nil, fmt.Errorf("etcd endpoint %q has no host", ep)
		}
		if port == "" {
			port = "2379"
		}
		if net.ParseIP(host) == nil {
			switch strings.Count(host, ".") {
			case 0:
				host = host + "." + ns + ".svc"
			case 1:
				host += ".svc"
			}
		}
		out = append(out, "http://"+net.JoinHostPort(host, port))
	}
	if len(out) == 0 {
		return nil, errors.New("no etcd endpoint")
	}
	return out, nil
}

// gatewayError is a non-200 answer of the gateway.
type gatewayError struct {
	Status  int
	Message string
}

func (e *gatewayError) Error() string {
	return fmt.Sprintf("etcd gateway: HTTP %d: %s", e.Status, e.Message)
}

// post sends body to path and decodes the answer into out. A transport
// error moves on to the next base; an answer, good or bad, ends the call.
func (g *etcdGateway) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var errs []error
	for _, base := range g.bases {
		cctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
		req, err := http.NewRequestWithContext(cctx, http.MethodPost, base+path, bytes.NewReader(b))
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := g.client.Do(req)
		if err != nil {
			cancel()
			errs = append(errs, err)
			continue
		}
		data, rerr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		_ = resp.Body.Close()
		cancel()
		if rerr != nil {
			errs = append(errs, rerr)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			var e struct {
				Message string `json:"message"`
				Error   string `json:"error"`
			}
			_ = json.Unmarshal(data, &e)
			msg := e.Message
			if msg == "" {
				msg = e.Error
			}
			if msg == "" {
				msg = strings.TrimSpace(string(data))
			}
			return &gatewayError{Status: resp.StatusCode, Message: msg}
		}
		if out == nil {
			return nil
		}
		// A streaming call (keepalive) answers one JSON object per line.
		if i := bytes.IndexByte(data, '\n'); i >= 0 && path == "/v3/lease/keepalive" {
			data = data[:i]
		}
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("etcd gateway %s: decode: %w", path, err)
		}
		return nil
	}
	return fmt.Errorf("etcd gateway %s: %w", path, errors.Join(errs...))
}

// int64String decodes an int64 that etcd's gateway sends as a JSON string
// (or, from other encoders, as a number).
type int64String int64

func (n *int64String) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*n = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*n = int64String(v)
	return nil
}

type gatewayKV struct {
	Key   string      `json:"key"`
	Value string      `json:"value"`
	Lease int64String `json:"lease"`
}

func (kv gatewayKV) decode() (etcdKV, error) {
	k, err := base64.StdEncoding.DecodeString(kv.Key)
	if err != nil {
		return etcdKV{}, err
	}
	v, err := base64.StdEncoding.DecodeString(kv.Value)
	if err != nil {
		return etcdKV{}, err
	}
	return etcdKV{Key: k, Value: v, Lease: int64(kv.Lease)}, nil
}

// prefixEnd is the range end that selects every key starting with prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte{}, prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}

func (g *etcdGateway) rangeKeys(ctx context.Context, key, end []byte) ([]etcdKV, error) {
	body := map[string]string{"key": base64.StdEncoding.EncodeToString(key)}
	if end != nil {
		body["range_end"] = base64.StdEncoding.EncodeToString(end)
	}
	var out struct {
		Kvs  []gatewayKV `json:"kvs"`
		More bool        `json:"more"`
	}
	if err := g.post(ctx, "/v3/kv/range", body, &out); err != nil {
		return nil, err
	}
	if out.More {
		return nil, errors.New("etcd range: the answer was cut short")
	}
	kvs := make([]etcdKV, 0, len(out.Kvs))
	for _, kv := range out.Kvs {
		d, err := kv.decode()
		if err != nil {
			return nil, fmt.Errorf("etcd range: %w", err)
		}
		kvs = append(kvs, d)
	}
	return kvs, nil
}

func (g *etcdGateway) RangePrefix(ctx context.Context, prefix []byte) ([]etcdKV, error) {
	return g.rangeKeys(ctx, prefix, prefixEnd(prefix))
}

func (g *etcdGateway) Get(ctx context.Context, key []byte) (*etcdKV, error) {
	kvs, err := g.rangeKeys(ctx, key, nil)
	if err != nil || len(kvs) == 0 {
		return nil, err
	}
	return &kvs[0], nil
}

func (g *etcdGateway) TimeToLive(ctx context.Context, id int64, keys bool) (etcdLeaseInfo, error) {
	var out struct {
		TTL        int64String `json:"TTL"`
		GrantedTTL int64String `json:"grantedTTL"`
		Keys       []string    `json:"keys"`
	}
	if err := g.post(ctx, "/v3/lease/timetolive", map[string]any{"ID": strconv.FormatInt(id, 10), "keys": keys}, &out); err != nil {
		return etcdLeaseInfo{}, err
	}
	info := etcdLeaseInfo{TTL: int64(out.TTL), GrantedTTL: int64(out.GrantedTTL)}
	for _, k := range out.Keys {
		b, err := base64.StdEncoding.DecodeString(k)
		if err != nil {
			return etcdLeaseInfo{}, fmt.Errorf("etcd timetolive: %w", err)
		}
		info.Keys = append(info.Keys, b)
	}
	return info, nil
}

func (g *etcdGateway) Grant(ctx context.Context, id, ttl int64) error {
	var out struct {
		ID    int64String `json:"ID"`
		Error string      `json:"error"`
	}
	err := g.post(ctx, "/v3/lease/grant", map[string]string{"ID": strconv.FormatInt(id, 10), "TTL": strconv.FormatInt(ttl, 10)}, &out)
	var ge *gatewayError
	if errors.As(err, &ge) && strings.Contains(ge.Message, "lease already exists") {
		return errLeaseExists
	}
	if err != nil {
		return err
	}
	if out.Error != "" {
		if strings.Contains(out.Error, "lease already exists") {
			return errLeaseExists
		}
		return fmt.Errorf("etcd grant: %s", out.Error)
	}
	if int64(out.ID) != id {
		return fmt.Errorf("etcd grant: granted lease %x, asked for %x", int64(out.ID), id)
	}
	return nil
}

func (g *etcdGateway) Put(ctx context.Context, key, value []byte, lease int64) error {
	return g.post(ctx, "/v3/kv/put", map[string]string{
		"key":   base64.StdEncoding.EncodeToString(key),
		"value": base64.StdEncoding.EncodeToString(value),
		"lease": strconv.FormatInt(lease, 10),
	}, nil)
}

func (g *etcdGateway) KeepAlive(ctx context.Context, id int64) (int64, error) {
	var out struct {
		Result struct {
			TTL int64String `json:"TTL"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := g.post(ctx, "/v3/lease/keepalive", map[string]string{"ID": strconv.FormatInt(id, 10)}, &out); err != nil {
		return 0, err
	}
	if out.Error != nil {
		return 0, fmt.Errorf("etcd keepalive: %s", out.Error.Message)
	}
	return int64(out.Result.TTL), nil
}

func (g *etcdGateway) Revoke(ctx context.Context, id int64) error {
	err := g.post(ctx, "/v3/lease/revoke", map[string]string{"ID": strconv.FormatInt(id, 10)}, nil)
	var ge *gatewayError
	if errors.As(err, &ge) && strings.Contains(ge.Message, "requested lease not found") {
		return nil // gone already: what a revoke wants
	}
	return err
}
