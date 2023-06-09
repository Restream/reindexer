package reindexer

import (
	"context"

	otelattr "go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func (db *reindexerImpl) startTracingSpan(ctx context.Context, spanName string, attributes ...otelattr.KeyValue) oteltrace.Span {
	_, span := db.otelTracer.Start(
		ctx,
		spanName,
		oteltrace.WithAttributes(db.otelCommonTraceAttrs...),
		oteltrace.WithAttributes(attributes...),
	)

	return span
}
