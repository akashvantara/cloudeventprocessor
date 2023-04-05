package cloudeventtransform

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func constructLogs() plog.Logs {
	td := plog.NewLogs()
	rs0 := td.ResourceLogs().AppendEmpty()
	rs0.Resource().Attributes().PutStr("host.name", "localhost")
	rs0ils0 := rs0.ScopeLogs().AppendEmpty()
	rs0ils0.Scope().SetName("scope1")
	fillLogOne(rs0ils0.LogRecords().AppendEmpty())
	fillLogTwo(rs0ils0.LogRecords().AppendEmpty())
	rs0ils1 := rs0.ScopeLogs().AppendEmpty()
	rs0ils1.Scope().SetName("scope2")
	fillLogOne(rs0ils1.LogRecords().AppendEmpty())
	fillLogTwo(rs0ils1.LogRecords().AppendEmpty())
	return td
}

func fillLogOne(log plog.LogRecord) {
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	log.Body().SetStr("Test log")
	log.SetTimestamp(timestamp)
	log.SetObservedTimestamp(timestamp)
	log.SetDroppedAttributesCount(1)
	log.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	log.SetSeverityNumber(1)
	log.Attributes().PutStr(ATTR_EVENT_REASON, "Updated")
	log.Attributes().PutStr(ATTR_EVENT_NS, "testns")
	log.Attributes().PutStr(ATTR_EVENT_UID, "abcdefgh")
	log.Attributes().PutStr(ATTR_EVENT_START_TIME, timestamp.String())
	log.Attributes().PutInt(ATTR_EVENT_COUNT, 1)
}

func fillLogTwo(log plog.LogRecord) {
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	log.Body().SetStr("operationB")
	log.SetTimestamp(timestamp)
	log.SetObservedTimestamp(timestamp)
	log.Attributes().PutStr("http.method", "get")
	log.Attributes().PutStr("http.path", "/health")
	log.Attributes().PutStr("http.url", "http://localhost/health")
	log.Attributes().PutStr("flags", "C|D")

}

func testResourceLogs(lwrs []logWithResource) plog.Logs {
	ld := plog.NewLogs()

	for i, lwr := range lwrs {
		rl := ld.ResourceLogs().AppendEmpty()

		// Add resource level attributes
		//nolint:errcheck
		rl.Resource().Attributes().FromRaw(lwr.resourceAttributes)
		ls := rl.ScopeLogs().AppendEmpty().LogRecords()
		for _, name := range lwr.logNames {
			l := ls.AppendEmpty()
			// Add record level attributes
			//nolint:errcheck
			l.Attributes().FromRaw(lwrs[i].recordAttributes)
			l.Attributes().PutStr("name", name)
			// Set body & severity fields
			l.Body().SetStr(lwr.body)
			l.SetSeverityText(lwr.severityText)
			l.SetSeverityNumber(lwr.severityNumber)
		}
	}
	return ld
}

func TestNilResourceLogs(t *testing.T) {
	logs := plog.NewLogs()
	rls := logs.ResourceLogs()
	rls.AppendEmpty()
	requireNotPanicsLogs(t, logs)
}

func TestNilILL(t *testing.T) {
	logs := plog.NewLogs()
	rls := logs.ResourceLogs()
	rl := rls.AppendEmpty()
	ills := rl.ScopeLogs()
	ills.AppendEmpty()
	requireNotPanicsLogs(t, logs)
}

func TestNilLog(t *testing.T) {
	logs := plog.NewLogs()
	rls := logs.ResourceLogs()
	rl := rls.AppendEmpty()
	ills := rl.ScopeLogs()
	sl := ills.AppendEmpty()
	ls := sl.LogRecords()
	ls.AppendEmpty()
	requireNotPanicsLogs(t, logs)
}

func requireNotPanicsLogs(t *testing.T, logs plog.Logs) {
	/*
	   factory := NewFactory()
	   cfg := factory.CreateDefaultConfig()
	   pcfg := cfg.(*Config)

	   	pcfg.Logs = LogFilters{
	   		Exclude: nil,
	   	}

	   ctx := context.Background()
	   proc, _ := factory.CreateLogsProcessor(

	   	ctx,
	   	processortest.NewNopCreateSettings(),
	   	cfg,
	   	consumertest.NewNop(),

	   )

	   	require.NotPanics(t, func() {
	   		_ = proc.ConsumeLogs(ctx, logs)
	   	})
	*/
}
