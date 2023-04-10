package cloudeventexporter

import (
	"errors"
	"net/url"
	"unicode"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	confighttp.HTTPClientSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings  `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings  `mapstructure:"retry_on_failure"`
	Ce                            CloudEventSpec `mapstructure:"ce"`
	Filter                        string         `mapstructure:"filter"`
	Endpoint                      string         `mapstructure:"endpoint"`
}

type CloudEventSpec struct {
	SpecVersion string `mapstructure:"spec_version"`
	AppendType  string `mapstructure:"append_type"`
	Source      string `mapstructure:"source"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// Validate if something is present in append_type as it forms Ce-Type value
	if len(cfg.Ce.AppendType) == 0 {
		return errors.New("append_type field can not be empty")
	} else {
		for _, ch := range cfg.Ce.AppendType {
			if unicode.IsSpace(ch) {
				return errors.New("append_type value can't have spaces")
			}
		}
	}

	// Check if source is present in the configuration
	if len(cfg.Ce.Source) == 0 {
		return errors.New("source field can not be empty")
	}

	// Check if the endpoint format is right
	if cfg.Endpoint != "" {
		_, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return errors.New("endpoint must be a valid URL")
		}
	}

	return nil
}
