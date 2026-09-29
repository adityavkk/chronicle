package correlation

import "testing"

func TestSpanNameNamesEveryOperationAndNothingElse(t *testing.T) {
	for _, operation := range Operations() {
		got, ok := OperationOf(SpanName(operation))
		if !ok || got != operation {
			t.Errorf("OperationOf(SpanName(%q)) = (%q, %v)", operation, got, ok)
		}
	}
	for _, name := range []string{"redis.evalsha", "chronicle.teleport", "chronicle.", "chronicle", "", "Chronicle.append", "chronicle.append.retry"} {
		if got, ok := OperationOf(name); ok {
			t.Errorf("OperationOf(%q) = (%q, true), want not a Chronicle operation", name, got)
		}
	}
}

func TestIsOperationAgreesWithOperations(t *testing.T) {
	for _, operation := range Operations() {
		if !IsOperation(operation) {
			t.Errorf("IsOperation(%q) = false for a listed operation", operation)
		}
	}
	for _, name := range []string{"", "Append", "appends", "redis"} {
		if IsOperation(name) {
			t.Errorf("IsOperation(%q) = true, want false", name)
		}
	}
}
