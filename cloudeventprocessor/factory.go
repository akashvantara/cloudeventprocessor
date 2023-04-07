package cloudeventtransform

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr   = "cloudeventtransform"
	stability = component.StabilityLevelAlpha
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory creates a factory for the routing processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		CreateDefaultConfig,
		processor.WithLogs(createLogsProcessor, stability),
	)
}

func CreateDefaultConfig() component.Config {
	return &Config{
		Ce: CloudEventSpec{
			SpecVersion: "1.0",
		},
	}
}

func createLogsProcessor(
	ctx context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Logs) (processor.Logs, error) {

	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("could not initialize cloud-event transform processor")
	}

	ceProcessor, err := newProcessor(set.TelemetrySettings, pCfg)
	if err != nil {
		return nil, errors.New("Failed to create cloud-event processor")
	}

	return processorhelper.NewLogsProcessor(
		ctx,
		set,
		cfg,
		nextConsumer,
		ceProcessor.processLogs,
		processorhelper.WithCapabilities(ceProcessor.Capabilities()),
	)
}
