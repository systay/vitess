/*
Copyright 2017 Google Inc.

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

// Package trace contains a helper interface that allows various tracing
// tools to be plugged in to components using this interface. If no plugin is
// registered, the default one makes all trace calls into no-ops.
package trace

import (
  "io"

  "golang.org/x/net/context"
)

// Span represents a unit of work within a trace. After creating a Span with
// NewSpan(), call one of the Start methods to mark the beginning of the work
// represented by this Span. Call Finish() when that work is done to record the
// Span. A Span may be reused by calling Start again.
type Span interface {
	Finish()
	// Annotate records a key/value pair associated with a Span. It should be
	// called between Start and Finish.
	Annotate(key string, value interface{})
}

type SpanType int

const (
  // This is a span that is called from inside the same process
  Local SpanType = iota

  // Client spans are calls to a server initiated from this process
  Client

  // Server spans cover the activity from the beginning of a server
  Server
)

// NewSpan creates a new Span with the currently installed tracing plugin.
// If no tracing plugin is installed, it returns a fake Span that does nothing.
func NewSpan(inCtx context.Context, label string, spanType SpanType) (Span, context.Context) {
  return theSpanFactory.NewSpan(inCtx, label, spanType)
}

// NewClientSpan returns a span and a context to register calls to dependent services
func NewClientSpan(inCtx context.Context, serviceName, spanLabel string) (Span, context.Context) {
  return theSpanFactory.NewClientSpan(inCtx, serviceName, spanLabel)
}

// CopySpan creates a new context from parentCtx, with only the trace span
// copied over from spanCtx, if it has any. If not, parentCtx is returned.
func CopySpan(parentCtx, spanCtx context.Context) context.Context {
  return theSpanFactory.CopySpan(parentCtx, spanCtx)
}

var theSpanFactory = NoTracing{}

type SpanFactory interface {
  NewSpan(inCtx context.Context, label string, spanType SpanType) (Span, context.Context)
  NewClientSpan(inCtx context.Context, serviceName, spanLabel string) (Span, context.Context)
  CopySpan(parentCtx, spanCtx context.Context) context.Context
}

func StartTracing(_ string) io.Closer {
  // TODO: Here we should be picking up environment or arguments to config open census
  return &nilCloser{}
}

type nilCloser struct {
}

func (c *nilCloser) Close() error { return nil }
