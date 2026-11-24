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
package proxy

import (
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"

	"nvcf-grpc-proxy/proxy/invocation"
	"nvcf-grpc-proxy/proxy/metrics"
	"nvcf-grpc-proxy/proxy/worker"
)

const (
	ledgerSource        = "grpc-proxy"
	ledgerSchemaVersion = 1

	eventTypeWorkDispatched        = "GrpcProxy.Work.Dispatched"
	eventTypeAttachmentEstablished = "GrpcProxy.Attachment.Established"
	eventTypeAttachmentSetupFailed = "GrpcProxy.Attachment.Setup_failed"
	eventTypeAttachmentClosed      = "GrpcProxy.Attachment.Closed"
	eventTypeRpcStarted            = "GrpcProxy.Rpc.Started"
	eventTypeRpcFinished           = "GrpcProxy.Rpc.Finished"
	eventTypeSessionEnded          = "GrpcProxy.Session.Ended"

	// Auth, rate limit and rejoin lookup failures are not published: their
	// callers are unverified or rejected, and every attempt would add a row.
	setupPhaseDispatch   = "dispatch"
	setupPhaseWorkerWait = "worker_wait"
)

// Pre-initialized so the series exist at zero before the first publish.
func init() {
	for _, eventType := range []string{eventTypeWorkDispatched, eventTypeAttachmentEstablished, eventTypeAttachmentSetupFailed, eventTypeAttachmentClosed, eventTypeRpcStarted, eventTypeRpcFinished, eventTypeSessionEnded} {
		for _, result := range []string{"sent", "failed", "dropped"} {
			metrics.EventLedgerPublishCounter.WithLabelValues(eventType, result)
		}
	}
}

type eventHeader struct {
	SchemaVersion int    `json:"schema_version"`
	RequestId     string `json:"request_id"`
}

func header(requestId uuid.UUID) eventHeader {
	return eventHeader{SchemaVersion: ledgerSchemaVersion, RequestId: requestId.String()}
}

type workDispatchedData struct {
	eventHeader
	IsRejoin bool `json:"is_rejoin"`
}

type attachmentEstablishedData struct {
	eventHeader
	AttachmentId   string `json:"attachment_id"`
	IsRejoin       bool   `json:"is_rejoin"`
	SetupLatencyMs int64  `json:"setup_latency_ms"`
}

type attachmentSetupFailedData struct {
	eventHeader
	Phase        string `json:"phase"`
	AttachmentId string `json:"attachment_id,omitempty"`
}

type attachmentClosedData struct {
	eventHeader
	AttachmentId string `json:"attachment_id"`
	Cause        string `json:"cause"`
	HeldForMs    int64  `json:"held_for_ms"`
	// A fixed category. The close detail can carry worker text and is never sent.
	CloseCode string `json:"close_code"`
}

func newLedgerEvent(eventType string, at time.Time, requestId uuid.UUID, functionId, functionVersionId, callerNcaId string, data any) cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetID(uuid.NewString())
	event.SetSource(ledgerSource)
	event.SetType(eventType)
	event.SetTime(at)
	event.SetExtension("functionversionid", functionVersionId)
	event.SetExtension("functionid", functionId)
	event.SetExtension("resourceid", requestId.String())
	event.SetExtension("ncaid", callerNcaId)
	// Marshalling a struct of strings, numbers and bools cannot fail.
	_ = event.SetData(cloudevents.ApplicationJSON, data)
	return event
}

func newWorkDispatched(auth invocation.WorkerAuth, isRejoin bool, at time.Time) cloudevents.Event {
	return newLedgerEvent(eventTypeWorkDispatched, at, auth.RequestId, auth.FunctionId, auth.FunctionVersionId, auth.CallerNcaId,
		workDispatchedData{eventHeader: header(auth.RequestId), IsRejoin: isRejoin})
}

func newDispatchFailed(auth invocation.WorkerAuth, at time.Time) cloudevents.Event {
	return newLedgerEvent(eventTypeAttachmentSetupFailed, at, auth.RequestId, auth.FunctionId, auth.FunctionVersionId, auth.CallerNcaId,
		attachmentSetupFailedData{eventHeader: header(auth.RequestId), Phase: setupPhaseDispatch})
}

// newAttachmentEstablished describes wc gaining its worker at connectedAt.
// The ledger keeps the latest event of each type per request ID, so a rejoin
// replaces the first attachment's event; attachment_id tells them apart.
func newAttachmentEstablished(wc *worker.WorkerConnection, info workerAuthInfo, connectedAt time.Time) cloudevents.Event {
	return newLedgerEvent(eventTypeAttachmentEstablished, connectedAt, info.requestId, info.functionId, info.functionVersionId, info.callerNcaId,
		attachmentEstablishedData{
			eventHeader:    header(info.requestId),
			AttachmentId:   wc.AttachmentId.String(),
			IsRejoin:       info.isRejoin,
			SetupLatencyMs: connectedAt.Sub(info.setupStartedAt).Milliseconds(),
		})
}

// attachmentEndEvent reports how wc left the cache, when anyone is waiting to
// hear: Closed for an attachment reported established, or Setup_failed when no
// worker arrived before the slot's TTL ran out.
func attachmentEndEvent(wc *worker.WorkerConnection, cause string, closedAt time.Time, heldFor time.Duration, closeCode string) (cloudevents.Event, bool) {
	switch {
	case wc.MarkEnded():
		return newLedgerEvent(eventTypeAttachmentClosed, closedAt, wc.RequestId, wc.FunctionId, wc.FunctionVersionId, wc.CallerNcaId,
			attachmentClosedData{
				eventHeader:  header(wc.RequestId),
				AttachmentId: wc.AttachmentId.String(),
				Cause:        cause,
				HeldForMs:    heldFor.Milliseconds(),
				CloseCode:    closeCode,
			}), true
	case !wc.EverConnected() && cause == metrics.CloseReasonTTLExpired:
		return newLedgerEvent(eventTypeAttachmentSetupFailed, closedAt, wc.RequestId, wc.FunctionId, wc.FunctionVersionId, wc.CallerNcaId,
			attachmentSetupFailedData{eventHeader: header(wc.RequestId), Phase: setupPhaseWorkerWait, AttachmentId: wc.AttachmentId.String()}), true
	}
	return cloudevents.Event{}, false
}

// rpcData identifies the RPC an attachment reports. Only the first RPC
// forwarded over each attachment is reported.
type rpcData struct {
	eventHeader
	AttachmentId string `json:"attachment_id"`
	RpcId        string `json:"rpc_id"`
	Method       string `json:"method"`
	Protocol     string `json:"protocol"`
}

type rpcFinishedData struct {
	rpcData
	// The gRPC status for a gRPC call, otherwise the HTTP status. Absent when
	// a gRPC call ended without one.
	StatusCode            *int   `json:"status_code,omitempty"`
	DurationMs            int64  `json:"duration_ms"`
	TimeToFirstResponseMs *int64 `json:"time_to_first_response_ms,omitempty"`
	// Absent for an upgraded connection, whose traffic is not counted.
	BytesIn  *int64 `json:"bytes_in,omitempty"`
	BytesOut *int64 `json:"bytes_out,omitempty"`
	// Forwarding was cut short, or a gRPC call ended without a status.
	Incomplete bool `json:"incomplete"`
}

func newRpcData(wc *worker.WorkerConnection, rpc *rpcObserver) rpcData {
	return rpcData{
		eventHeader:  header(wc.RequestId),
		AttachmentId: wc.AttachmentId.String(),
		RpcId:        rpc.id.String(),
		Method:       rpc.method,
		Protocol:     rpc.protocol,
	}
}

func newRpcStarted(wc *worker.WorkerConnection, rpc *rpcObserver) cloudevents.Event {
	return newLedgerEvent(eventTypeRpcStarted, rpc.startedAt, wc.RequestId, wc.FunctionId, wc.FunctionVersionId, wc.CallerNcaId,
		newRpcData(wc, rpc))
}

// newRpcFinished describes how rpc ended at at. aborted is set when
// forwarding did not return normally.
func newRpcFinished(wc *worker.WorkerConnection, rpc *rpcObserver, aborted bool, at time.Time) cloudevents.Event {
	data := rpcFinishedData{rpcData: newRpcData(wc, rpc), DurationMs: at.Sub(rpc.startedAt).Milliseconds()}
	if code, ok := rpc.statusCode(); ok {
		data.StatusCode = &code
	}
	if !rpc.firstResponse.IsZero() {
		ttfb := rpc.firstResponse.Sub(rpc.startedAt).Milliseconds()
		data.TimeToFirstResponseMs = &ttfb
	}
	if !rpc.hijacked {
		var in int64
		if rpc.body != nil {
			in = rpc.body.n.Load()
		}
		out := rpc.bytesOut
		data.BytesIn, data.BytesOut = &in, &out
	}
	data.Incomplete = aborted || (rpc.protocol == rpcProtocolGRPC && data.StatusCode == nil)
	return newLedgerEvent(eventTypeRpcFinished, at, wc.RequestId, wc.FunctionId, wc.FunctionVersionId, wc.CallerNcaId, data)
}

// newSessionEnded reports that the session wc belonged to can no longer be
// rejoined.
func newSessionEnded(wc *worker.WorkerConnection, at time.Time) cloudevents.Event {
	return newLedgerEvent(eventTypeSessionEnded, at, wc.RequestId, wc.FunctionId, wc.FunctionVersionId, wc.CallerNcaId, header(wc.RequestId))
}
