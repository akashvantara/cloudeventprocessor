package cloudeventtransform

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Ce CloudEventSpec `mapstructure:"ce"`
}

type CloudEventSpec struct	{
	Id string	`mapstructure:"id"`
	SpecVersion string	`mapstructure:"spec_version"`
	Type string	`mapstructure:"type"`
	Source string	`mapstructure:"source"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	
	if len(cfg.Ce.Id) == 0 	{
		return errors.New("id field can not be empty")
	}

	if len(cfg.Ce.Type) == 0	{
		return errors.New("type field can not be empty")
	}

	if len(cfg.Ce.Source) == 0	{
		return errors.New("source field can not be empty")
	}

	return nil
}
