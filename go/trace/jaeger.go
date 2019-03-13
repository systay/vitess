package trace

import (
  "io"

  "github.com/opentracing/opentracing-go"
  "github.com/uber/jaeger-client-go/config"
  "golang.org/x/net/context"
)

type JaegerSpan struct {
  otSpan opentracing.Span
}

func (js JaegerSpan) Finish() {
  js.otSpan.Finish()
}

func (js JaegerSpan) Annotate(key string, value interface{}) {
  js.otSpan.SetTag(key, value)
}

type OpenTracingFactory struct {
  Tracer opentracing.Tracer
}

// NewJagerTracerFromEnv will instantiate a SpanFactory implemented by Jaeger,
// taking configuration from environment variables. Available properties are:
// JAEGER_SERVICE_NAME -- If this is set, the service name used in code will be ignored and this value used instead
// JAEGER_RPC_METRICS
// JAEGER_TAGS
// JAEGER_SAMPLER_TYPE
// JAEGER_SAMPLER_PARAM
// JAEGER_SAMPLER_MANAGER_HOST_PORT
// JAEGER_SAMPLER_MAX_OPERATIONS
// JAEGER_SAMPLER_REFRESH_INTERVAL
// JAEGER_REPORTER_MAX_QUEUE_SIZE
// JAEGER_REPORTER_FLUSH_INTERVAL
// JAEGER_REPORTER_LOG_SPANS
// JAEGER_ENDPOINT
// JAEGER_USER
// JAEGER_PASSWORD
// JAEGER_AGENT_HOST
// JAEGER_AGENT_PORT
func NewJagerTracerFromEnv(serviceName string) (opentracing.Tracer, io.Closer, error) {

  cfg, err := config.FromEnv()
  if cfg.ServiceName == "" {
    cfg.ServiceName = serviceName
  }

  tracer, closer, err := cfg.NewTracer()

  if err != nil {
    return nil, &nilCloser{}, err
  }

  return tracer, closer, nil
}

func (jf OpenTracingFactory) New(parent Span, label string, spanType SpanType) Span {
  var innerSpan opentracing.Span
  if parent == nil {
    innerSpan = jf.Tracer.StartSpan(label)
  } else {
    jaegerParent := parent.(JaegerSpan)
    span := jaegerParent.otSpan
    innerSpan = jf.Tracer.StartSpan(label, opentracing.ChildOf(span.Context()))
  }
  return JaegerSpan{otSpan: innerSpan}
}

func (jf OpenTracingFactory) FromContext(ctx context.Context) (Span, bool) {
  innerSpan := opentracing.SpanFromContext(ctx)

  if innerSpan != nil {
    return JaegerSpan{otSpan: innerSpan}, true
  } else {
    return nil, false
  }
}

func (jf OpenTracingFactory) NewContext(parent context.Context, s Span) context.Context {
  span, ok := s.(JaegerSpan)
  if !ok {
    return nil
  }
  return opentracing.ContextWithSpan(parent, span.otSpan)
}

type nilCloser struct {
}

func (c *nilCloser) Close() error { return nil }

func init() {
  tracingBackendFactories["jaeger"] = NewJagerTracerFromEnv
}
