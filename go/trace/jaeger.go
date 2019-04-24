package trace

import (
	"flag"
	"io"

	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/log"
)

var (
	agentHost    = flag.String("jaeger-agent-host", "", "host and port to send spans to. if empty, no tracing will be done")
	samplingRate = flag.Float64("tracing-sampling-rate", 0.1, "sampling rate for the probabilistic jaeger sampler")
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

func (jf OpenTracingFactory) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	log.Info("adding grpc server interceptors")
	theirInterceptor := otgrpc.OpenTracingServerInterceptor(jf.Tracer)
	//myInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	//
	//	myHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
	//		span := trace.FromContext(ctx)
	//		if span == nil {
	//			log.Error("handler called without span in context")
	//		}
	//		return handler(ctx, req)
	//	}
	//	response, err := theirInterceptor(ctx, req, info, myHandler)
	//	return response, err
	//}
	addInterceptors(otgrpc.OpenTracingStreamServerInterceptor(jf.Tracer), theirInterceptor)
}

func (jf OpenTracingFactory) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	addInterceptors(otgrpc.OpenTracingStreamClientInterceptor(jf.Tracer), otgrpc.OpenTracingClientInterceptor(jf.Tracer))
}

// newJagerTracerFromEnv will instantiate a TracingService implemented by Jaeger,
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
func newJagerTracerFromEnv(serviceName string) (TracingService, io.Closer, error) {
	cfg, err := config.FromEnv()
	if cfg.ServiceName == "" {
		cfg.ServiceName = serviceName
	}

	// Allow command line args to override environment variables.
	if *agentHost != "" {
		cfg.Reporter.LocalAgentHostPort = *agentHost
	}
	cfg.Reporter.LogSpans = true
	log.Infof("Tracing to: %v as %v", cfg.Reporter.LocalAgentHostPort, cfg.ServiceName)
	cfg.Sampler = &config.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: *samplingRate,
	}
	log.Infof("Tracing sampling rate: %v", *samplingRate)

	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))

	if err != nil {
		return nil, &nilCloser{}, err
	}

	opentracing.SetGlobalTracer(tracer)

	return OpenTracingFactory{tracer}, closer, nil
}

func (jf OpenTracingFactory) NewClientSpan(parent Span, serviceName, label string) Span {
	span := jf.New(parent, label)
	span.Annotate("peer.service", serviceName)
	return span
}

func (jf OpenTracingFactory) New(parent Span, label string) Span {
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
	tracingBackendFactories["opentracing-jaeger"] = newJagerTracerFromEnv
}
