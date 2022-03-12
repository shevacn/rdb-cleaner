package log

import "go.uber.org/zap"

var Logger *zap.Logger

func init() {
	var cfg = zap.NewProductionConfig()

	cfg.OutputPaths = []string{"stdout", "./logs.log"}

	Logger, _ = cfg.Build()
}
