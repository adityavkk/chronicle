package correlation

import "testing"

func TestSpanNameNamesEveryOperationAndNothingElse(t *testing.T) {
	for _, operation := range Operations {
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
