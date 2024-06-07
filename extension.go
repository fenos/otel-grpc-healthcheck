package grpc_health_check

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var (
	client = http.Client{
		Timeout: 5 * time.Second,
	}
)

type grpcHealthCheckExtension struct {
	config   Config
	logger   *zap.Logger
	server   *grpc.Server
	stopCh   chan struct{}
	settings component.TelemetrySettings
}

type HealthServerWithLog struct {
	logger *zap.Logger
	*health.Server
}

func NewHealthServerWithLog(logger *zap.Logger) *HealthServerWithLog {
	return &HealthServerWithLog{
		logger: logger,
		Server: health.NewServer(),
	}
}

func (h *HealthServerWithLog) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	h.logger.Info("Checking health")
	resp, err := h.Server.Check(ctx, in)

	if err == nil {
		h.logger.Info("Health check result", zap.Any("response", resp.Status.String()))
	}

	if err != nil {
		h.logger.Error("Error checking health", zap.Error(err))
	}

	return resp, err
}

func (h *HealthServerWithLog) Watch(in *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	return h.Server.Watch(in, stream)
}

func (gc *grpcHealthCheckExtension) Start(ctx context.Context, host component.Host) error {
	gc.logger.Info("Starting grpc_health_check extension", zap.Any("config", gc.config))

	interceptorOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		// Add any other option (check functions starting with logging.With).
	}

	server, err := gc.config.Grpc.ToServer(ctx, host, gc.settings, grpc.ChainUnaryInterceptor(
		logging.UnaryServerInterceptor(InterceptorLogger(gc.logger), interceptorOpts...),
	))
	if err != nil {
		return err
	}
	gc.server = server

	ln, err := gc.config.Grpc.NetAddr.Listen(ctx)
	if err != nil {
		return fmt.Errorf("failed to bind to address %s: %w", gc.config.Grpc.NetAddr.Endpoint, err)
	}

	gc.stopCh = make(chan struct{})
	hs := NewHealthServerWithLog(gc.logger)

	// Register the health server with the gRPC server
	healthpb.RegisterHealthServer(gc.server, hs)
	reflection.Register(gc.server)

	go func() {
		time.Sleep(gc.config.StartPeriod)

		for {
			status := healthpb.HealthCheckResponse_SERVING
			response, err := client.Get(gc.config.HealthCheckHttpEndpoint)
			if err != nil {
				gc.logger.Error("Failed to get health check status", zap.Error(err))
				status = healthpb.HealthCheckResponse_NOT_SERVING
			} else if response.StatusCode < 200 || response.StatusCode >= 300 {
				gc.logger.Error("Service seems to be unhealthy", zap.Int("code", response.StatusCode))
				status = healthpb.HealthCheckResponse_NOT_SERVING
			}
			hs.SetServingStatus("", status)

			time.Sleep(gc.config.Interval)
		}
	}()

	go func() {
		defer close(gc.stopCh)

		// The listener ownership goes to the server.
		if err = gc.server.Serve(ln); !errors.Is(err, grpc.ErrServerStopped) && err != nil {
			gc.settings.ReportStatus(component.NewFatalErrorEvent(err))
		}
	}()

	return nil
}

func (gc *grpcHealthCheckExtension) Shutdown(context.Context) error {
	if gc.server == nil {
		return nil
	}
	gc.server.GracefulStop()
	if gc.stopCh != nil {
		<-gc.stopCh
	}
	return nil
}

func newServer(config Config, settings component.TelemetrySettings) *grpcHealthCheckExtension {
	return &grpcHealthCheckExtension{
		config:   config,
		logger:   settings.Logger,
		settings: settings,
	}
}

// InterceptorLogger adapts zap logger to interceptor logger.
// This code is simple enough to be copied and not imported.
func InterceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i]
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key.(string), v))
			case int:
				f = append(f, zap.Int(key.(string), v))
			case bool:
				f = append(f, zap.Bool(key.(string), v))
			default:
				f = append(f, zap.Any(key.(string), v))
			}
		}

		logger := l.WithOptions(zap.AddCallerSkip(1)).With(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}
