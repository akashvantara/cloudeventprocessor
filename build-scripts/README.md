# Processor build set-up

You'll require the following things when building the processor from source:
* make
* go v1.19+
* builder to build the open-telemetry collector

To download `builder` run the following command:
	``go install go.opentelemetry.io/collector/cmd/builder@latest``

This build-script will bake the following things inside the collector
* Receivers: [filelogreceiver, k8s_events, otlp]
* Processors: [cloudeventtransform]
* Exporters: [logging, kafka]

To build the otel-collector run
`make build`

Things will go into hv-otel folder if everything works fine
