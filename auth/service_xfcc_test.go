package auth

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// The APPEND_FORWARD threat model: the client controls every byte before the
// comma Envoy inserts in front of its own attested element. Chronicle must
// authenticate only what that appended element says, whatever the prefix is.
const (
	sidecarTrusted   = "URI=" + agentsID
	sidecarUntrusted = "URI=" + otherID
)

func TestParseXFCCWellFormed(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []xfccElement
	}{
		{"single unquoted pair", `URI=` + agentsID, []xfccElement{{{"URI", agentsID}}}},
		{
			"istio element with an empty quoted subject",
			`By=spiffe://cluster.local/ns/chronicle/sa/chronicle;Hash=abcd;Subject="";URI=` + agentsID,
			[]xfccElement{{{"By", "spiffe://cluster.local/ns/chronicle/sa/chronicle"}, {"Hash", "abcd"}, {"Subject", ""}, {"URI", agentsID}}},
		},
		{
			"quoted value with escaped quotes and separators",
			`Subject="CN=\"we;ird\",O=a,b";URI=` + agentsID,
			[]xfccElement{{{"Subject", `CN="we;ird",O=a,b`}, {"URI", agentsID}}},
		},
		{"rfc2253 backslash escapes are read as pairs", `Subject="CN=a\,b\\c";URI=x`, []xfccElement{{{"Subject", `CN=a,b\c`}, {"URI", "x"}}}},
		{"quoted URI", `Hash=ff;URI="` + agentsID + `"`, []xfccElement{{{"Hash", "ff"}, {"URI", agentsID}}}},
		{
			"two elements",
			`Hash=aa;URI=` + otherID + `,Hash=bb;URI=` + agentsID,
			[]xfccElement{{{"Hash", "aa"}, {"URI", otherID}}, {{"Hash", "bb"}, {"URI", agentsID}}},
		},
		{"blanks around a quoted value", `URI= "x" ;Hash=1`, []xfccElement{{{"URI", "x"}, {"Hash", "1"}}}},
		{"pair without a value is ignored", `junk;URI=x`, []xfccElement{{{"URI", "x"}}}},
		{"unquoted value may contain '='", `Cert=abc=def;URI=x`, []xfccElement{{{"Cert", "abc=def"}, {"URI", "x"}}}},
		{"backslash outside quotes is literal", `URI=a\b`, []xfccElement{{{"URI", `a\b`}}}},
		{"trailing comma keeps an empty last element", `URI=x,`, []xfccElement{{{"URI", "x"}}, nil}},
		{"empty header is one empty element", ``, []xfccElement{nil}},
	}
	for _, c := range cases {
		got, err := parseXFCC(c.in)
		if err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: parsed %#v, want %#v", c.name, got, c.want)
		}
	}
}

func TestParseXFCCRefusesMalformedQuoting(t *testing.T) {
	trusted := []string{agentsID}
	cases := []struct{ name, in, reason string }{
		{"unterminated quote", `Subject="oops`, xfccReasonUnterminatedQuote},
		{"unterminated quote running across the sidecar's comma", `URI=` + agentsID + `;Subject="x,` + sidecarUntrusted, xfccReasonUnterminatedQuote},
		{"trailing lone backslash inside quotes", `Subject="abc\`, xfccReasonUnterminatedQuote},
		{"escaped closing quote never closes", `Subject="abc\"`, xfccReasonUnterminatedQuote},
		{"quote inside an unquoted value", `URI=spiffe://a"b`, xfccReasonMisplacedQuote},
		{"quote at the end of an unquoted value", `URI=` + agentsID + `"`, xfccReasonMisplacedQuote},
		{"quote inside a key", `"URI=` + agentsID, xfccReasonMisplacedQuote},
		{"quote as the whole key", `"=x`, xfccReasonMisplacedQuote},
		{"quote in a value-less pair", `junk";URI=` + agentsID, xfccReasonMisplacedQuote},
		{"data after a closing quote", `URI="x"y;URI=` + agentsID, xfccReasonTrailingAfterQuote},
		{"second quoted run after a closing quote", `Subject="a""b";URI=` + agentsID, xfccReasonTrailingAfterQuote},
	}
	for _, c := range cases {
		_, err := parseXFCC(c.in)
		if err == nil {
			t.Errorf("%s: parseXFCC(%q) = nil error, want %s", c.name, c.in, c.reason)
			continue
		}
		if !errors.Is(err, ErrMalformedXFCC) {
			t.Errorf("%s: error %v does not wrap ErrMalformedXFCC", c.name, err)
		}
		if want := ErrMalformedXFCC.Error() + ": " + c.reason; err.Error() != want {
			t.Errorf("%s: error %q, want %q", c.name, err.Error(), want)
		}
		if strings.Contains(err.Error(), "spiffe") {
			t.Errorf("%s: error %q quotes header content", c.name, err.Error())
		}
		if _, ok := VerifyXFCC(c.in, trusted); ok {
			t.Errorf("%s: VerifyXFCC authenticated a malformed header", c.name)
		}
	}
}

// TestVerifyXFCCForgedPrefixCannotAbsorbSidecarElement is the 2026-09 review
// finding against the former splitXFCC: a client prefix with an odd number
// of quotes ran across the comma before the sidecar's appended element, so
// the client's trusted URI counted as part of the "last" element. Every such
// prefix is now refused, a well-formed prefix stays hearsay, and a malformed
// prefix fails closed even when the appended element is trusted.
func TestVerifyXFCCForgedPrefixCannotAbsorbSidecarElement(t *testing.T) {
	trusted := []string{agentsID}
	malformed := []struct{ name, prefix string }{
		{"unterminated quote after a trusted URI", `URI=` + agentsID + `;Subject="x`},
		{"unterminated quote before a trusted URI", `Subject="x;URI=` + agentsID},
		{"trailing backslash inside quotes", `URI=` + agentsID + `;Subject="\`},
		{"escaped closing quote", `URI=` + agentsID + `;Subject="\"`},
		{"quote in an unquoted URI value", `URI=` + agentsID + `"`},
		{"quote in a key", `"URI=` + agentsID},
		{"data after a closing quote", `URI="` + agentsID + `"x`},
		{"unterminated quote in a second element", `Hash=aa,URI=` + agentsID + `;Subject="x`},
	}
	wellFormed := []struct{ name, prefix string }{
		{"plain trusted element", `Hash=aa;URI=` + agentsID},
		{"balanced quotes hiding a comma and a trusted URI", `Subject="a,URI=` + agentsID + `"`},
		{"balanced escaped quotes", `Subject="a\",URI=` + agentsID + `\"b";URI=` + agentsID},
		{"istio-shaped element", `By=x;Hash=y;Subject="";URI=` + agentsID},
		{"two trusted elements", `URI=` + agentsID + `,URI=` + agentsID},
	}
	for _, c := range malformed {
		if _, ok := VerifyXFCC(c.prefix+","+sidecarUntrusted, trusted); ok {
			t.Errorf("%s: forged prefix authenticated against an untrusted sidecar element", c.name)
		}
		if _, ok := VerifyXFCC(c.prefix+","+sidecarTrusted, trusted); ok {
			t.Errorf("%s: malformed header must fail closed even with a trusted sidecar element", c.name)
		}
	}
	for _, c := range wellFormed {
		if _, ok := VerifyXFCC(c.prefix+","+sidecarUntrusted, trusted); ok {
			t.Errorf("%s: well-formed prefix authenticated against an untrusted sidecar element", c.name)
		}
		p, ok := VerifyXFCC(c.prefix+","+sidecarTrusted, trusted)
		if !ok || p.Subject() != agentsID {
			t.Errorf("%s: trusted sidecar element after a well-formed prefix = (%v,%q), want %q", c.name, ok, p.Subject(), agentsID)
		}
	}
}

// TestVerifyXFCCNoPrefixCanForgeSidecarElement: no client prefix — well
// formed, malformed, or arbitrary bytes drawn from the grammar's own alphabet
// — makes VerifyXFCC authenticate when the sidecar's appended element is
// untrusted.
func TestVerifyXFCCNoPrefixCanForgeSidecarElement(t *testing.T) {
	trusted := []string{agentsID}
	tokens := []string{`"`, `\`, `,`, `;`, `=`, ` `, `URI=`, `Subject=`, `Hash=`, agentsID, otherID, `x`, `\"`, `""`}
	rapid.Check(t, func(t *rapid.T) {
		var prefix string
		if rapid.Bool().Draw(t, "grammar-alphabet") {
			prefix = strings.Join(rapid.SliceOfN(rapid.SampledFrom(tokens), 0, 16).Draw(t, "tokens"), "")
		} else {
			prefix = rapid.String().Draw(t, "prefix")
		}
		if p, ok := VerifyXFCC(prefix+","+sidecarUntrusted, trusted); ok {
			t.Fatalf("prefix %q authenticated as %q", prefix, p.Subject())
		}
	})
}

// TestVerifyXFCCWellFormedPrefixKeepsSidecarElement: a prefix emitted the way
// Envoy emits XFCC — quoted values escaped, unquoted values free of the
// separators — never breaks the appended element: trusted still verifies,
// untrusted still fails, whatever trusted URIs the prefix plants.
func TestVerifyXFCCWellFormedPrefixKeepsSidecarElement(t *testing.T) {
	trusted := []string{agentsID}
	rapid.Check(t, func(t *rapid.T) {
		prefix := drawWellFormedXFCC(t)
		if p, ok := VerifyXFCC(prefix+","+sidecarUntrusted, trusted); ok {
			t.Fatalf("well-formed prefix %q authenticated as %q", prefix, p.Subject())
		}
		p, ok := VerifyXFCC(prefix+","+sidecarTrusted, trusted)
		if !ok || p.Subject() != agentsID {
			t.Fatalf("well-formed prefix %q broke the trusted sidecar element: (%v,%q)", prefix, ok, p.Subject())
		}
	})
}

// drawWellFormedXFCC generates one to three Envoy-shaped elements. Quoted
// values may hold anything, with '\' and '"' escaped as Envoy does; unquoted
// values contain none of ',', ';', '"' or '='. Half the elements plant the
// trusted URI as a plain pair.
func drawWellFormedXFCC(t *rapid.T) string {
	keys := []string{"By", "Hash", "Cert", "Chain", "Subject", "URI", "DNS"}
	escape := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	unquotable := func(r rune) rune {
		if strings.ContainsRune(`,;"=`, r) {
			return -1
		}
		return r
	}
	var elements []string
	for e, n := 0, rapid.IntRange(1, 3).Draw(t, "elements"); e < n; e++ {
		var pairs []string
		for p, m := 0, rapid.IntRange(1, 4).Draw(t, "pairs"); p < m; p++ {
			key := rapid.SampledFrom(keys).Draw(t, "key")
			raw := rapid.String().Draw(t, "raw")
			if rapid.Bool().Draw(t, "quoted") {
				pairs = append(pairs, key+`="`+escape.Replace(raw)+`"`)
			} else {
				pairs = append(pairs, key+"="+strings.Map(unquotable, raw))
			}
		}
		if rapid.Bool().Draw(t, "plant-trusted") {
			pairs = append(pairs, "URI="+agentsID)
		}
		elements = append(elements, strings.Join(pairs, ";"))
	}
	return strings.Join(elements, ",")
}

// TestAuthenticateDetailClassifiesXFCCRefusals: the marker gate still runs
// first and reports the generic detail; a header the parser refused reports
// the distinct detail with a fixed reason and no header or marker material;
// an allowlist miss stays generic; success and the bearer path carry none.
func TestAuthenticateDetailClassifiesXFCCRefusals(t *testing.T) {
	const marker = "verified"
	access := &ServiceAccess{
		TrustedSPIFFEIDs:   []string{agentsID},
		SidecarMarkerName:  "X-Chronicle-Sidecar",
		SidecarMarkerValue: marker,
	}
	malformed := `URI=` + agentsID + `;Subject="x,` + sidecarUntrusted

	_, status, detail := access.AuthenticateDetail("", malformed, "")
	if status != ServiceRejected || detail != serviceRejectedDetail {
		t.Fatalf("missing marker + malformed header = (%v,%q); the gate must run first and stay generic", status, detail)
	}

	_, status, detail = access.AuthenticateDetail("", malformed, marker)
	if status != ServiceRejected {
		t.Fatalf("malformed header with marker: status %v, want ServiceRejected", status)
	}
	if want := serviceRejectedDetail + ": " + ErrMalformedXFCC.Error() + ": " + xfccReasonUnterminatedQuote; detail != want {
		t.Fatalf("malformed header detail %q, want %q", detail, want)
	}
	for _, leak := range []string{"spiffe", marker, "Subject"} {
		if strings.Contains(detail, leak) {
			t.Fatalf("detail %q leaks %q", detail, leak)
		}
	}
	if _, st := access.Authenticate("", malformed, marker); st != ServiceRejected {
		t.Fatalf("Authenticate disagrees with AuthenticateDetail: %v", st)
	}

	_, status, detail = access.AuthenticateDetail("", sidecarUntrusted, marker)
	if status != ServiceRejected || detail != serviceRejectedDetail {
		t.Fatalf("allowlist miss = (%v,%q), want generic rejection", status, detail)
	}

	p, status, detail := access.AuthenticateDetail("", `Hash=aa;URI=`+otherID+","+sidecarTrusted, marker)
	if status != ServiceAuthenticated || p.Subject() != agentsID || detail != "" {
		t.Fatalf("well-formed trusted header = (%v,%q,%q)", status, p.Subject(), detail)
	}

	if _, status, detail := access.AuthenticateDetail("", "", ""); status != ServiceNotAttempted || detail != "" {
		t.Fatalf("no XFCC and no bearer = (%v,%q), want not attempted", status, detail)
	}
	var nilAccess *ServiceAccess
	if _, status, detail := nilAccess.AuthenticateDetail("", malformed, marker); status != ServiceNotAttempted || detail != "" {
		t.Fatalf("nil receiver = (%v,%q)", status, detail)
	}
}
