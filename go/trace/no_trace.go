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
  "golang.org/x/net/context"
)

type NoTracing struct {
}

func (nt *NoTracing) Finish() {}

func (nt *NoTracing) Annotate(_ string, _ interface{}) {}

func (nt *NoTracing) NewSpan(inCtx context.Context, label string, _ SpanType) (Span, context.Context) {
  return nt, inCtx
}

func (nt *NoTracing) NewClientSpan(inCtx context.Context, _, _ string) (Span, context.Context) {
  return nt, inCtx
}

func (nt *NoTracing) CopySpan(parentCtx, _ context.Context) context.Context {
  return parentCtx
}
