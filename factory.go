package cloudeventtransform

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
        // The value of "type" key in configuration.
        typeStr = "cloudeventtransform"
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
        return &Config{}
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

        if len(pCfg.BaseConfig.Operators) == 0 {
                return nil, errors.New("no operators were configured for this cloud-event transform processor")
        }

        return newProcessor(pCfg, nextConsumer, set.Logger)
}
