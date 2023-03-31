package cloudeventtransform

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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

type cloudeventTransformProcessor struct {
	id          string
	source      string
	specversion string
	typ         string
}

type cloudeventdata struct {
	count     uint
	message   string
	name      string
	namespace string
	reason    string
	startTime string
	uid       string
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
		source:      conf.Ce.Source,
		specversion: conf.Ce.SpecVersion,
		typ:         conf.Ce.Type,
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
				//var cloudEventMetaData string
				var cloudEventData cloudeventdata
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

						//fmt.Println(overAllErrStr)
						return errors.New(fmt.Sprintf("Couldn't find %sattributes in the log", overAllErrStr))
					}

					cloudEventData = cloudeventdata{
						count:     uint(eventCount.Int()),
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

func convertStrToCloudEventByteSlice(s *string) []byte {
	strLen := len(*s)
	slice := make([]byte, 0, 512)

	for i := 0; i < strLen; i++ {
		currentByte := (*s)[i]
		if !(uint8('\\') == currentByte || uint8(0x00) == currentByte) {
			slice = append(slice, currentByte)
		}
	}

	return slice
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
	for i := 0; i < valLen; i++	{
		if val[i] != QUOTE_BYTE	{
			retSlice = append(retSlice, val[i])
		} else	{
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
	This function constructs a Cloudevent message that can take multiple things from the passed config and the message that receiver sents
	At the end it'll form a JSON which escapes any quotes that's found in the string just to construct a good byte array that's readable
	by kafka and is easily parseable
*/
func (ce *cloudeventTransformProcessor) constructCloudEventJsonBody(vals *pcommon.Value, msgData *cloudeventdata) []byte {
	//{"datacontenttype":"application/json; charset=utf-8","id":"%s","source":"%s","specversion":"%s","type":"%s","data":%s}

	retSlice := make([]byte, 0, 512)

	contentType := []byte("datacontenttype")
	id := []byte("id")
	source := []byte("source")
	specVersion := []byte("specversion")
	typ := []byte("type")
	data := []byte("data")

	// data body
	retSlice = append(retSlice, OPEN_BRACE_BYTE)
	retSlice = appendJsonObjStr(contentType, []byte("application/json; charset=utf-8"), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr(id, []byte(ce.id), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr(source, []byte(ce.source), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr(specVersion, []byte(ce.specversion), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)
	retSlice = appendJsonObjStr(typ, []byte(ce.typ), retSlice)
	retSlice = append(retSlice, COMMA_BYTE)

	retSlice = append(retSlice, QUOTE_BYTE)
	retSlice = append(retSlice, data...)
	retSlice = append(retSlice, QUOTE_BYTE)

	retSlice = append(retSlice, COLON_BYTE)

	{
		//{"reason":"%s","start_time":"%s","name":"%s","uid":"%s","namespace":"%s","count":%s,"message":"%s"}

		reason := []byte("reason")
		startTime := []byte("start_time")
		name := []byte("name")
		uid := []byte("uid")
		ns := []byte("namespace")
		count := []byte("count")
		message := []byte("message")

		retSlice = append(retSlice, OPEN_BRACE_BYTE)
		retSlice = appendJsonObjStr(reason, []byte(msgData.reason), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr(startTime, []byte(msgData.startTime), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr(name, []byte(msgData.name), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr(uid, []byte(msgData.uid), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr(ns, []byte(msgData.namespace), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjElse(count, []byte(strconv.Itoa(int(msgData.count))), retSlice)
		retSlice = append(retSlice, COMMA_BYTE)
		retSlice = appendJsonObjStr(message, []byte(msgData.message), retSlice)
		retSlice = append(retSlice, CLOSE_BRACE_BYTE)
	}

	retSlice = append(retSlice, CLOSE_BRACE_BYTE)

	return retSlice
}
