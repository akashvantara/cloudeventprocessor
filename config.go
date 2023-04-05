package cloudeventtransform

import (
	"errors"
	"unicode"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Ce     CloudEventSpec `mapstructure:"ce"`
	Filter string         `mapstructure:"filter"`
}

type CloudEventSpec struct {
	SpecVersion string `mapstructure:"spec_version"`
	AppendType  string `mapstructure:"append_type"`
	Source      string `mapstructure:"source"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.Ce.AppendType) == 0 {
		return errors.New("append_type field can not be empty")
	} else {
		for _, ch := range cfg.Ce.AppendType {
			if unicode.IsSpace(ch) {
				return errors.New("append_type value can't have spaces")
			}
		}
	}

	if len(cfg.Ce.Source) == 0 {
		return errors.New("source field can not be empty")
	}

	return nil
}
