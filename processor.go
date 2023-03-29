package cloudeventtransform

import (
	"context"
	 "fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
        CEBODY = `{"datacontenttype": "application/json; charset=utf-8", "id": "from-kafka", "source": "/catalog-service/job", "specversion": "1.0", "type": "ldc.catalog.job.status.failed.v1", "data": "%s"}`
)

type cloudeventTransformProcessor struct {
        test int
}

func newProcessor(set component.TelemetrySettings, cfg *Config) (*cloudeventTransformProcessor, error) {
        p := &cloudeventTransformProcessor      {
                test: 1,
        }

        return p, nil
}

func (ce *cloudeventTransformProcessor) Capabilities() consumer.Capabilities {
        return consumer.Capabilities{MutatesData: true}
}

func (ce *cloudeventTransformProcessor) processLogs (ctx context.Context, ld plog.Logs) (plog.Logs, error) {
        for i := 0; i < ld.ResourceLogs().Len(); i++    {
                scopeLogs := ld.ResourceLogs().At(i).ScopeLogs()

                for j := 0; j < scopeLogs.Len(); j++    {
                        logRecord := scopeLogs.At(j)
                        records := logRecord.LogRecords()

                        for k := 0; k < records.Len(); k++      {
                                logBody := records.At(k).Body().AsString()
                                // Changing it
                                modifiedLogBody := fmt.Sprintf(CEBODY, logBody)
                                records.At(k).Body().SetStr(modifiedLogBody)
                        }
                }
        }

        return ld, nil
}

