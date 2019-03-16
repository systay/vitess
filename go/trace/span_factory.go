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

package trace

import (
  "fmt"

  "go.opencensus.io/trace"
  "golang.org/x/net/context"
)

type RealTracing struct {
}

func (RealTracing) NewSpan(inCtx context.Context, label string, spanType SpanType) (Span, context.Context) {
  ctx, span := trace.StartSpan(inCtx, label)
  //TODO add span type?
  return RealSpan{ocSpan: span}, ctx
}

func (RealTracing) NewClientSpan(inCtx context.Context, serviceName, spanLabel string) (Span, context.Context) {
  span, ctx := NewSpan(inCtx, spanLabel, Client)
  span.Annotate("peer.service", serviceName)
  return span, ctx
}

func (RealTracing) CopySpan(parentCtx, spanCtx context.Context) context.Context {
  if span := trace.FromContext(spanCtx); span != nil {
    return trace.NewContext(parentCtx, span)
  }
  return parentCtx
}

type RealSpan struct {
  ocSpan *trace.Span
}

func (js RealSpan) Finish() {
  js.ocSpan.End()
}

func (js RealSpan) Annotate(key string, value interface{}) {
  attr := trace.StringAttribute(key, fmt.Sprintf("%v", value))
  js.ocSpan.Annotate([]trace.Attribute{attr}, key)
}

