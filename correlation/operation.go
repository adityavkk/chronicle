package correlation

import "strings"

// Chronicle's traced operations: the closed vocabulary its own root spans are
// named by and its sampler and configuration speak. Every request to the main
// listener is exactly one of the first six; a webhook delivery attempt is
// OperationDelivery.
const (
	OperationAppend       = "append"
	OperationRead         = "read"
	OperationCreate       = "create"
	OperationDelete       = "delete"
	OperationSubscription = "subscription"
	OperationDelivery     = "delivery"
	OperationOther        = "other"
)

// Operations lists every operation, in documentation order.
var Operations = []string{
	OperationAppend, OperationRead, OperationCreate, OperationDelete,
	OperationSubscription, OperationDelivery, OperationOther,
}

// spanPrefix marks a span Chronicle started for one of its operations, as
// opposed to a span an instrumentation library started on its behalf.
const spanPrefix = "chronicle."

// SpanName is the name of the span Chronicle starts for operation.
func SpanName(operation string) string { return spanPrefix + operation }

// OperationOf inverts SpanName: the operation a span name denotes, and whether
// the name is one of Chronicle's own at all.
func OperationOf(spanName string) (string, bool) {
	operation, ok := strings.CutPrefix(spanName, spanPrefix)
	if !ok || !IsOperation(operation) {
		return "", false
	}
	return operation, true
}

// IsOperation reports whether name is one of Operations.
func IsOperation(name string) bool {
	for _, operation := range Operations {
		if name == operation {
			return true
		}
	}
	return false
}
