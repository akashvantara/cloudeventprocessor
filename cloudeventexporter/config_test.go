package cloudeventexporter

import (
	"log"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	cloudEventConfig := Config{
		Ce: CloudEventSpec{
			SpecVersion: "test_again",
			AppendType:  "test_again_again",
			Source:      "test_again_again_again",
		},
		Filter: "*",
		HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://some_test_url.com:1234"},
	}

	unmarsheledConf := Config{}
	err = cm.Unmarshal(&unmarsheledConf)

	if err != nil {
		log.Panicln("Couldn't convert the passed config.yaml to valid YAML", err)
	}

	assert.Equal(t, unmarsheledConf, cloudEventConfig)
}
