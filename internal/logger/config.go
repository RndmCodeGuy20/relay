package logger

import "go.uber.org/zap/zapcore"

type Config struct {
	ServiceName string
	Environment string // "dev" | "prod"

	Level zapcore.Level

	// OpenTelemetry (optional)
	EnableOTel bool
}
