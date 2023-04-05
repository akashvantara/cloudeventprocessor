package cloudeventtransform

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	//CE_BODY           = `{"datacontenttype":"application/json; charset=utf-8","id":"%s","source":"%s","specversion":"%s","type":"%s","data":%s}`
	//CE_DATA_META_BODY = `{"reason":"%s","start_time":"%s","name":"%s","uid":"%s","namespace":"%s","count":%s,"message":"%s"}`

	ATTR_EVENT_COUNT      = "k8s.event.count"
	ATTR_EVENT_NAME       = "k8s.event.name"
	ATTR_EVENT_NS         = "k8s.namespace.name"
	ATTR_EVENT_REASON     = "k8s.event.reason"
	ATTR_EVENT_START_TIME = "k8s.event.start_time"
	ATTR_EVENT_UID        = "k8s.event.uid"

	FETCH_ATTR = true

	BACKSLASH_BYTE   = byte('\\')
	CLOSE_BRACE_BYTE = byte('}')
	COLON_BYTE       = byte(':')
	COMMA_BYTE       = byte(',')
	OPEN_BRACE_BYTE  = byte('{')
	QUOTE_BYTE       = byte('"')
)

var (
	filters        []string         // k8s.event.reason filters
	filterAllowAll bool     = false // if configuration changes this to true, it'll let pass all of the logs

	typeVersion string // typeverson will define the body type of CloudEvent (right now it's v1 specific)
)

type cloudeventTransformProcessor struct {
	id          string
	source      string
	specversion string
	typ         string
}

type cloudeventdata struct {
	count     int
	message   string
	name      string
	namespace string
	reason    string
	startTime string
	uid       string // This field will be converted and passed to cloudeventTransformProcessor.id
}

func newProcessor(set component.TelemetrySettings, cfg *Config) (*cloudeventTransformProcessor, error) {
	defaultConfig := CreateDefaultConfig()
	conf := defaultConfig.(*Config)
	var err error = nil

	if err = cfg.Validate(); err != nil {
		return nil, err
	}

	if len(cfg.Ce.AppendType) > 0 {
		conf.Ce.AppendType = cfg.Ce.AppendType
	}

	if len(cfg.Ce.SpecVersion) > 0 {
		conf.Ce.SpecVersion = cfg.Ce.SpecVersion

		typeVersion = "v" + string(conf.Ce.SpecVersion[0])
	}

	if len(cfg.Ce.Source) > 0 {
		conf.Ce.Source = cfg.Ce.Source
	}

	if len(cfg.Filter) > 0 {
		filters = strings.Split(cfg.Filter, "|")
		filtersLen := len(filters)

		if filtersLen == 0 {
			err = errors.New(
				fmt.Sprintf("some valid fields should be provided in filters ('*', 'Created|Deleted'), provided: %s",
					cfg.Filter,
				),
			)
		} else if filtersLen == 1 && filters[0] == "*" {
			filterAllowAll = true
		}
	}

	p := &cloudeventTransformProcessor{
		source:      conf.Ce.Source,
		specversion: conf.Ce.SpecVersion,
		typ:         conf.Ce.AppendType,
	}

	return p, err
}

func (ce *cloudeventTransformProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (ce *cloudeventTransformProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	return ld, converRawMsgtToCloudEvent(ce, &ld)
}

func converRawMsgtToCloudEvent(ce *cloudeventTransformProcessor, ld *plog.Logs) error {
	var cloudEventData cloudeventdata

	if !filterAllowAll {
		ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
			rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
				sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
					reason, reasonOk := lr.Attributes().Get(ATTR_EVENT_REASON)
					if !reasonOk {
						return false
					}

					reasonFound := false
					for _, r := range filters {
						if r == reason.AsString() {
							reasonFound = true
							break
						}
					}

					return !reasonFound
				})
				return sl.LogRecords().Len() == 0
			})
			return rl.ScopeLogs().Len() == 0
		})
	}

	// Convert the log/s
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		scopeLogs := ld.ResourceLogs().At(i).ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			logRecord := scopeLogs.At(j)
			records := logRecord.LogRecords()

			for k := 0; k < records.Len(); k++ {
				//var cloudEventMetaData string
				currentMessage := records.At(k).Body()

				// Get all the required attributes
				if FETCH_ATTR {
					attrMap := records.At(k).Attributes()

					eventCount, eventCountOk := attrMap.Get(ATTR_EVENT_COUNT)
					eventName, eventNameOk := attrMap.Get(ATTR_EVENT_NAME)
					eventNs, eventNsOk := attrMap.Get(ATTR_EVENT_NS)
					eventUid, eventUidOk := attrMap.Get(ATTR_EVENT_UID)
					reason, reasonOk := attrMap.Get(ATTR_EVENT_REASON)
					startTime, startTimeOk := attrMap.Get(ATTR_EVENT_START_TIME)

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

					cloudEventData = cloudeventdata{
						count:     int(eventCount.Int()),
						message:   currentMessage.AsString(),
						name:      eventName.AsString(),
						namespace: eventNs.AsString(),
						reason:    reason.AsString(),
						startTime: startTime.AsString(),
						uid:       eventUid.AsString(),
					}
				} else {
					cloudEventData = cloudeventdata{
						count:     0,
						message:   "",
						name:      "",
						namespace: "",
						reason:    "",
						startTime: "",
						uid:       "",
					}
				}

				byteData := ce.constructCloudEventJsonBody(&currentMessage, &cloudEventData)
				byteDataLen := len(byteData)

				_ = currentMessage.SetEmptyBytes()
				currentMessage.Bytes().EnsureCapacity(byteDataLen)
				currentMessage.Bytes().Append(byteData...)
			}
		}
	}

	return nil
}

/*
This function takes key and adds quotes around it and leaves value as is
Ex: `key` will become `"key"` and `val` will becom `"val"`
if the values has quotes in it like `"this value"` it'll become `\"this value\"`
So if key is `key` and value is `this is my "phone"`
the return bytearray will look like this `"key":"this is my \"phone\"`
*/
func appendJsonObjStr(key []byte, val []byte, retSlice []byte) []byte {

	retSlice = append(retSlice, QUOTE_BYTE)
	retSlice = append(retSlice, key...)
	retSlice = append(retSlice, QUOTE_BYTE)

	retSlice = append(retSlice, COLON_BYTE)

	retSlice = append(retSlice, QUOTE_BYTE)
	valLen := len(val)
	for i := 0; i < valLen; i++ {
		if val[i] != QUOTE_BYTE {
			retSlice = append(retSlice, val[i])
		} else {
			retSlice = append(retSlice, BACKSLASH_BYTE)
			retSlice = append(retSlice, val[i])
		}
	}
	retSlice = append(retSlice, QUOTE_BYTE)

	return retSlice
}

/*
This function takes key and adds quotes around it and leaves value as is
Ex: `key` will become `"key"` and `val` will remain `val`
The return output will be `"key":val` which gets appended and returned in retSlice
*/
func appendJsonObjElse(key []byte, val []byte, retSlice []byte) []byte {
	retSlice = append(retSlice, QUOTE_BYTE)
	retSlice = append(retSlice, key...)
	retSlice = append(retSlice, QUOTE_BYTE)

	retSlice = append(retSlice, COLON_BYTE)

	retSlice = append(retSlice, val...)

	return retSlice
}

/*
Function takes pretext from the params passed in config append_type and adds the reason to it
Ex: append_type: `com.company.event` and reason: `Created Successfully`
function will return `com.company.event.CreatedSuccessfully`
*/
func configureCeType(pretext string, reason string) string {
	var ret strings.Builder
	ret.Grow(len(pretext) + len(reason))

	ret.WriteString(pretext)
	ret.WriteRune('.')
	ret.WriteString(typeVersion) // It'll define the version
	ret.WriteRune('.')

	for _, ch := range reason {
		if !unicode.IsSpace(ch) {
			ret.WriteRune(ch)
		}
	}

	return ret.String()
}

/*
This function constructs a Cloudevent message that can take multiple things from the passed config and the message that receiver sents
At the end it'll form a JSON which escapes any quotes that's found in the string just to construct a good byte array that's readable
by kafka and is easily parseable
*/
func (ce *cloudeventTransformProcessor) constructCloudEventJsonBody(vals *pcommon.Value, msgData *cloudeventdata) []byte {
	//{"datacontenttype":"application/json; charset=utf-8","id":"%s","source":"%s","specversion":"%s","type":"%s","data":%s}
	retSlice := make([]byte, 0, 512)

	// data body
	retSlice = append(retSlice, OPEN_BRACE_BYTE)
	retSlice = appendJsonObjStr([]byte("datacontenttype"), []byte("application/json; charset=utf-8"), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr([]byte("id"), []byte(msgData.uid), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr([]byte("source"), []byte(ce.source), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr([]byte("specversion"), []byte(ce.specversion), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr([]byte("type"), []byte(configureCeType(ce.typ, msgData.reason)), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = append(retSlice, QUOTE_BYTE)
	retSlice = append(retSlice, []byte("data")...)
	retSlice = append(retSlice, QUOTE_BYTE)
	retSlice = append(retSlice, COLON_BYTE)

	{
		//{"reason":"%s","start_time":"%s","name":"%s","uid":"%s","namespace":"%s","count":%s,"message":"%s"}
		retSlice = append(retSlice, OPEN_BRACE_BYTE)
		retSlice = appendJsonObjStr([]byte("reason"), []byte(msgData.reason), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr([]byte("start_time"), []byte(msgData.startTime), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr([]byte("name"), []byte(msgData.name), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		//retSlice = appendJsonObjStr([]byte("uid"), []byte(msgData.uid), retSlice)
		//retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr([]byte("namespace"), []byte(msgData.namespace), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjElse([]byte("count"), []byte(strconv.Itoa(msgData.count)), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr([]byte("message"), []byte(msgData.message), retSlice)
		retSlice = append(retSlice, CLOSE_BRACE_BYTE)
	}

	retSlice = append(retSlice, CLOSE_BRACE_BYTE)
	return retSlice
}
