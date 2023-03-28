package cloudeventtransform

import (
	"testing"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestLoadConfig(t *testing.T) {
        factory := NewFactory()
        cfg := factory.CreateDefaultConfig()

        assert.NoError(t, component.UnmarshalConfig(confmap.New(), cfg))
        assert.Equal(t, factory.CreateDefaultConfig(), cfg)
}
