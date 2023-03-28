package cloudeventtransform

import (
	"context"
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type cloudeventTransformProcessor struct {
        logger *zap.Logger
        config *Config
        consumer consumer.Logs
}

func newProcessor(config *Config, nextConsumer consumer.Logs, logger *zap.Logger) (*cloudeventTransformProcessor, error) {
        p := &cloudeventTransformProcessor{
                logger: logger,
                config:   config,
                consumer: nextConsumer,
        }

        return p, nil
}

func (ce *cloudeventTransformProcessor) Capabilities() consumer.Capabilities {
        return consumer.Capabilities{MutatesData: true}
}

func (ce *cloudeventTransformProcessor) Start(context.Context, component.Host) error {
        log.Println("Starting cloud-event transform processor")

        return nil
}

func (ce *cloudeventTransformProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// Add the logs to the chain

    return nil
}

func (ce *cloudeventTransformProcessor) Shutdown(ctx context.Context) error {
        log.Println("Stopping cloud-event transform processor")

        return nil
}
