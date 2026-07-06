package auth

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestParseServiceBearerConfig(t *testing.T) {
	cases := []struct {
		in      string
		want    []struct{ name, token string }
		wantErr bool
	}{
		{"tok123", []struct{ name, token string }{{"service", "tok123"}}, false},
		{"agents:tok123", []struct{ name, token string }{{"agents", "tok123"}}, false},
		{"agents:tok:with:colons", []struct{ name, token string }{{"agents", "tok:with:colons"}}, false},
		{"agents:old, agents:new", []struct{ name, token string }{{"agents", "old"}, {"agents", "new"}}, false},
		{"a:1,b:2", []struct{ name, token string }{{"a", "1"}, {"b", "2"}}, false},
		{"", nil, true},
		{"   ", nil, true},
		{"a:1,", nil, true},
		{":tok", nil, true},
		{"name:", nil, true},
		{"a:1,,b:2", nil, true},
	}
	for _, c := range cases {
		got, err := ParseServiceBearerConfig(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("Parse(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if c.wantErr {
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("Parse(%q) len = %d, want %d", c.in, len(got), len(c.want))
			continue
		}
		for i := range got {
			if got[i].name != c.want[i].name || got[i].token != c.want[i].token {
				t.Errorf("Parse(%q)[%d] = (%q,token), want (%q,%q)", c.in, i, got[i].name, c.want[i].name, c.want[i].token)
			}
		}
	}
}

// TestParseServiceBearerErrorsNeverLeakTokens: config parse errors go to
// startup logs, so they must carry positions, never the raw value.
func TestParseServiceBearerErrorsNeverLeakTokens(t *testing.T) {
	secret := "supersecrettoken"
	for _, in := range []string{secret + ":", ":" + secret, secret + ",", "a:1,," + secret} {
		_, err := ParseServiceBearerConfig(in)
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error %q leaks token material", err.Error())
		}
	}
}

func TestServiceCredentialRedactsToken(t *testing.T) {
	creds, err := ParseServiceBearerConfig("agents:supersecret")
	if err != nil {
		t.Fatal(err)
	}
	for _, rendered := range []string{creds[0].String(), creds[0].GoString()} {
		if strings.Contains(rendered, "supersecret") {
			t.Fatalf("formatted credential %q leaks the token", rendered)
		}
	}
}

func TestVerifyServiceBearer(t *testing.T) {
	creds, err := ParseServiceBearerConfig("agents:tok-A,other:tok-B")
	if err != nil {
		t.Fatal(err)
	}

	p, ok := VerifyServiceBearer("tok-A", creds)
	if !ok || p.Kind() != KindService || p.Subject() != "agents" {
		t.Fatalf("tok-A -> (%v,%v,%v)", ok, p.Kind(), p.Subject())
	}
	if p, ok := VerifyServiceBearer("tok-B", creds); !ok || p.Subject() != "other" {
		t.Fatalf("tok-B -> (%v,%v)", ok, p.Subject())
	}

	for _, bad := range []string{"", "tok", "tok-A ", " tok-A", "tok-Ax", "tok-a", "TOK-A", "tok-B\x00"} {
		if _, ok := VerifyServiceBearer(bad, creds); ok {
			t.Errorf("presented %q must not verify", bad)
		}
	}
	if _, ok := VerifyServiceBearer("tok-A", nil); ok {
		t.Error("empty credential set must never verify")
	}
}

// TestVerifyServiceBearerProperty: only the exact configured byte string
// verifies; any prefix, suffix, extension, or mutation is rejected.
func TestVerifyServiceBearerProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		token := rapid.StringMatching(`[!-~]{8,64}`).Filter(func(s string) bool {
			return !strings.ContainsAny(s, ",:")
		}).Draw(t, "token")
		creds, err := ParseServiceBearerConfig("svc:" + token)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := VerifyServiceBearer(token, creds); !ok {
			t.Fatal("exact token must verify")
		}
		if len(token) > 1 {
			if _, ok := VerifyServiceBearer(token[:len(token)-1], creds); ok {
				t.Fatal("prefix verified")
			}
			if _, ok := VerifyServiceBearer(token[1:], creds); ok {
				t.Fatal("suffix verified")
			}
		}
		if _, ok := VerifyServiceBearer(token+"x", creds); ok {
			t.Fatal("extension verified")
		}
		idx := rapid.IntRange(0, len(token)-1).Draw(t, "idx")
		mut := []byte(token)
		mut[idx] ^= 0x01
		if string(mut) != token {
			if _, ok := VerifyServiceBearer(string(mut), creds); ok {
				t.Fatal("mutation verified")
			}
		}
	})
}

func TestParseTrustedSPIFFEIDs(t *testing.T) {
	ids, err := ParseTrustedSPIFFEIDs("spiffe://cluster.local/ns/electric/sa/agents-server, spiffe://cluster.local/ns/x/sa/y")
	if err != nil || len(ids) != 2 || ids[0] != "spiffe://cluster.local/ns/electric/sa/agents-server" {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	for _, bad := range []string{"", "  ", "https://not-spiffe", "spiffe://ok,,spiffe://also", "spiffe://ok,"} {
		if _, err := ParseTrustedSPIFFEIDs(bad); err == nil {
			t.Errorf("ParseTrustedSPIFFEIDs(%q) must error", bad)
		}
	}
}

const (
	agentsID = "spiffe://cluster.local/ns/electric/sa/agents-server"
	otherID  = "spiffe://cluster.local/ns/other/sa/other"
)

func TestVerifyXFCC(t *testing.T) {
	trusted := []string{agentsID}

	cases := []struct {
		name    string
		header  string
		ok      bool
		subject string
	}{
		{
			"real envoy single element",
			`By=spiffe://cluster.local/ns/chronicle/sa/chronicle;Hash=abcd1234;Subject="CN=agents,OU=x";URI=` + agentsID,
			true, agentsID,
		},
		{
			"quoted URI value",
			`Hash=ff;URI="` + agentsID + `"`,
			true, agentsID,
		},
		{
			"subject with escaped quotes and separators",
			`Subject="CN=\"we;ird\",O=a,b";URI=` + agentsID,
			true, agentsID,
		},
		{
			"multi-element: last element wins",
			`Hash=aa;URI=` + otherID + `,Hash=bb;URI=` + agentsID,
			true, agentsID,
		},
		{
			"attacker plants trusted id in earlier element",
			`Hash=aa;URI=` + agentsID + `,Hash=bb;URI=` + otherID,
			false, "",
		},
		{
			"attacker plants trusted id in quoted subject of last element",
			`Subject="URI=` + agentsID + `";URI=` + otherID,
			false, "",
		},
		{"untrusted id", `URI=` + otherID, false, ""},
		{"no URI in last element", `By=x;Hash=aa`, false, ""},
		{"empty header", "", false, ""},
		{"case-mangled value", `URI=` + strings.ToUpper(agentsID), false, ""},
		{"lowercase key accepted", `uri=` + agentsID, true, agentsID},
	}
	for _, c := range cases {
		p, ok := VerifyXFCC(c.header, trusted)
		if ok != c.ok {
			t.Errorf("%s: ok=%v, want %v", c.name, ok, c.ok)
			continue
		}
		if ok && (p.Kind() != KindService || p.Subject() != c.subject) {
			t.Errorf("%s: principal (%v,%q)", c.name, p.Kind(), p.Subject())
		}
	}

	if _, ok := VerifyXFCC(`URI=`+agentsID, nil); ok {
		t.Error("empty allowlist must never authenticate")
	}
}

// TestVerifyXFCCProperty: a verified subject is always a member of the
// trusted allowlist, whatever the header contents.
func TestVerifyXFCCProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		header := rapid.String().Draw(t, "header")
		trusted := rapid.SliceOfN(rapid.StringMatching(`spiffe://[a-z.]{1,20}/[a-z/]{1,30}`), 0, 3).Draw(t, "trusted")
		p, ok := VerifyXFCC(header, trusted)
		if !ok {
			return
		}
		for _, id := range trusted {
			if p.Subject() == id {
				return
			}
		}
		t.Fatalf("verified subject %q not in allowlist %v", p.Subject(), trusted)
	})
}
