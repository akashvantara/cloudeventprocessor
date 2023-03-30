package cloudeventtransform

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	CE_BODY           = `{"datacontenttype":"application/json; charset=utf-8","id":"%s","source":"%s","specversion":"%s","type":"%s","data":%s}`
	CE_DATA_META_BODY = `{"reason":"%s","start_time":"%s","name":"%s","uid":"%s","namespace":"%s","count":%s,"message":"%s"}`

	ATTR_EVENT_NAME       = "k8s.event.name"
	ATTR_EVENT_NS         = "k8s.event.namespace"
	ATTR_EVENT_COUNT      = "k8s.event.count"
	ATTR_EVENT_START_TIME = "k8s.event.start_time"
	ATTR_EVENT_REASON     = "k8s.event.reason"
	ATTR_EVENT_UID        = "k8s.event.uid"

	FETCH_ATTR = false
)

type cloudeventTransformProcessor struct {
	id          string
	typ         string
	specversion string
	source      string
}

func newProcessor(set component.TelemetrySettings, cfg *Config) (*cloudeventTransformProcessor, error) {
	defaultConfig := CreateDefaultConfig()

	conf := defaultConfig.(*Config)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if len(cfg.Ce.Id) > 0 {
		conf.Ce.Id = cfg.Ce.Id
	}

	if len(cfg.Ce.Type) > 0 {
		conf.Ce.Type = cfg.Ce.Type
	}

	if len(cfg.Ce.Type) > 0 {
		conf.Ce.SpecVersion = cfg.Ce.SpecVersion
	}

	if len(cfg.Ce.Source) > 0 {
		conf.Ce.Source = cfg.Ce.Source
	}

	p := &cloudeventTransformProcessor{
		id:          conf.Ce.Id,
		typ:         conf.Ce.Type,
		specversion: conf.Ce.SpecVersion,
		source:      conf.Ce.Source,
	}

	return p, nil
}

func (ce *cloudeventTransformProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (ce *cloudeventTransformProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if err := converRawMsgtToCloudEvent(ce, &ld); err != nil {
		return ld, err
	}

	return ld, nil
}

func converRawMsgtToCloudEvent(ce *cloudeventTransformProcessor, ld *plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		scopeLogs := ld.ResourceLogs().At(i).ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			logRecord := scopeLogs.At(j)
			records := logRecord.LogRecords()

			for k := 0; k < records.Len(); k++ {
				var cloudEventMetaData string

				// Get all the required attributes
				if FETCH_ATTR {
					attrMap := records.At(k).Attributes()

					reason, reasonOk := attrMap.Get(ATTR_EVENT_REASON)
					startTime, startTimeOk := attrMap.Get(ATTR_EVENT_START_TIME)
					eventName, eventNameOk := attrMap.Get(ATTR_EVENT_NAME)
					eventUid, eventUidOk := attrMap.Get(ATTR_EVENT_UID)
					eventNs, eventNsOk := attrMap.Get(ATTR_EVENT_NS)
					eventCount, eventCountOk := attrMap.Get(ATTR_EVENT_COUNT)

					anyError := !(reasonOk && startTimeOk && eventNameOk && eventUidOk && eventNsOk && eventCountOk)

					if anyError {
						overAllErrStr := ""

						if !reasonOk {
							overAllErrStr += "{" + ATTR_EVENT_REASON + "} "
						}

						if !startTimeOk {
							overAllErrStr += "{" + ATTR_EVENT_START_TIME + "} "
						}

						if !eventNameOk {
							overAllErrStr += "{" + ATTR_EVENT_NAME + "} "
						}

						if !eventUidOk {
							overAllErrStr += "{" + ATTR_EVENT_UID + "} "
						}

						if !eventNsOk {
							overAllErrStr += "{" + ATTR_EVENT_NS + "} "
						}

						if !eventCountOk {
							overAllErrStr += "{" + ATTR_EVENT_COUNT + "} "
						}

						return errors.New(fmt.Sprintf("Couldn't find %sattributes in the log", overAllErrStr))
					}

					cloudEventMetaData = fmt.Sprintf(CE_DATA_META_BODY,
						reason.AsString(),
						startTime.AsString(),
						eventName.AsString(),
						eventUid.AsString(),
						eventNs.AsString(),
						eventCount.AsString(),
						records.At(k).Body().AsString(),
					)
				} else {
					cloudEventMetaData = fmt.Sprintf(CE_DATA_META_BODY,
						"",
						"",
						"",
						"",
						"",
						"0",
						records.At(k).Body().AsString(),
					)
				}

				modifiedLogBody := fmt.Sprintf(CE_BODY,
					ce.id,
					ce.source,
					ce.specversion,
					ce.typ,
					cloudEventMetaData,
				)

				records.At(k).Body().SetStr(modifiedLogBody)
				//fmt.Printf("msg: %s\n", modifiedLogBody)
			}
		}
	}
	return nil
}
