package util

import (
	"context"

	"k8s.io/klog/v2"
)

const LogKey = "dfs-logger"

type LoggerType string

func WithLog(parentCtx context.Context, log klog.Logger) context.Context {
	return context.WithValue(parentCtx, LoggerType(LogKey), log)
}

func GenLog(ctx context.Context, log klog.Logger, name string) klog.Logger {
	if ctx.Value(LoggerType(LogKey)) != nil {
		log = ctx.Value(LoggerType(LogKey)).(klog.Logger)
	}
	if name != "" {
		log = log.WithName(name)
	}
	return log
}
