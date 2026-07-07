package auth

import "testing"

func TestParseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    Mode
		wantErr bool
	}{
		{"", ModeInsecure, false},
		{"insecure", ModeInsecure, false},
		{"enforce", ModeEnforce, false},
		{"ENFORCE", ModeEnforce, false},
		{"  enforce  ", ModeEnforce, false},
		{"Insecure", ModeInsecure, false},
		{"enforced", ModeInsecure, true},
		{"telemetry", ModeInsecure, true},
		{"1", ModeInsecure, true},
		{"off", ModeInsecure, true},
	}
	for _, c := range cases {
		got, err := ParseMode(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("ParseMode(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
		}
		if got != c.want {
			t.Errorf("ParseMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestParseModeFailsClosedOnGarbage pins the config posture: an unparseable
// mode is an error (the process refuses to start), never a silent fallback
// into either mode.
func TestParseModeFailsClosedOnGarbage(t *testing.T) {
	if _, err := ParseMode("enfroce"); err == nil {
		t.Fatal("a typo'd mode must be an error, not a silent default")
	}
}

func TestModeString(t *testing.T) {
	if ModeInsecure.String() != "insecure" || ModeEnforce.String() != "enforce" {
		t.Fatalf("mode strings: %q %q", ModeInsecure.String(), ModeEnforce.String())
	}
}

func TestDecisionConstructors(t *testing.T) {
	a := Allow()
	if !a.Allowed() || a.Reason() != ReasonNone || a.Detail() != "" {
		t.Fatalf("Allow() = %+v", a)
	}

	d := Deny(ReasonForbidden, "out of scope")
	if d.Allowed() || d.Reason() != ReasonForbidden || d.Detail() != "out of scope" {
		t.Fatalf("Deny(forbidden) = allowed=%v reason=%v detail=%q", d.Allowed(), d.Reason(), d.Detail())
	}

	// A denial can never be unclassified: ReasonNone coerces to the 401 class.
	u := Deny(ReasonNone, "unclassified")
	if u.Allowed() || u.Reason() != ReasonUnauthenticated {
		t.Fatalf("Deny(ReasonNone) must fail closed to unauthenticated, got %v", u.Reason())
	}
}

func TestZeroDecisionDenies(t *testing.T) {
	// The zero value of Decision must deny: forgetting to decide is a deny.
	var d Decision
	if d.Allowed() {
		t.Fatal("zero Decision must not allow")
	}
}

func TestPrincipalZeroIsAnonymous(t *testing.T) {
	var p Principal
	if !p.IsAnonymous() || p.Kind() != KindAnonymous || p.Subject() != "" {
		t.Fatalf("zero Principal = kind=%v subject=%q", p.Kind(), p.Subject())
	}
}

func TestEnumStrings(t *testing.T) {
	actions := map[Action]string{
		ActionRead: "read", ActionAppend: "append", ActionCreate: "create",
		ActionDelete: "delete", ActionSubscribe: "subscribe", ActionLink: "link",
		ActionClaim: "claim",
	}
	for a, want := range actions {
		if a.String() != want {
			t.Errorf("Action(%d).String() = %q, want %q", int(a), a.String(), want)
		}
	}
	reasons := map[DenyReason]string{
		ReasonNone: "none", ReasonUnauthenticated: "unauthenticated", ReasonForbidden: "forbidden", ReasonFenced: "fenced",
	}
	for r, want := range reasons {
		if r.String() != want {
			t.Errorf("DenyReason(%d).String() = %q, want %q", int(r), r.String(), want)
		}
	}
	kinds := map[PrincipalKind]string{
		KindAnonymous: "anonymous", KindAgent: "agent", KindUser: "user",
		KindService: "service", KindSystem: "system",
	}
	for k, want := range kinds {
		if k.String() != want {
			t.Errorf("PrincipalKind(%d).String() = %q, want %q", int(k), k.String(), want)
		}
	}
}
