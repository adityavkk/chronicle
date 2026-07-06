package webhook

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/auth"
)

// This file holds the webhook signing and token primitives. They are pure given
// their inputs (key material, clock, randomness); persistence of the key
// material across restarts lives in the Redis store (ds:{__ds}:jwks and
// :tokenkey), which is what makes the kid stable and tokens survivable across a
// restart (PROTOCOL §6.5: keys SHOULD persist; §12.9: tokens are HMAC-signed).

// SigningKey is an Ed25519 webhook signing key plus its stable kid.
type SigningKey struct {
	Kid       string
	Public    ed25519.PublicKey
	Private   ed25519.PrivateKey
	CreatedAt time.Time
	Status    string // "active" or "retiring"
}

// GenerateSigningKey creates a fresh Ed25519 signing key. The kid is the JWK
// thumbprint (RFC 7638) prefixed with "ds_", matching the Caddy webhook engine
// and the conformance receiver's key-selection logic, and is stable for the
// life of the key (PROTOCOL §6.5).
func GenerateSigningKey(rand io.Reader, now time.Time) (SigningKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand)
	if err != nil {
		return SigningKey{}, fmt.Errorf("generate ed25519 key: %w", err)
	}
	return SigningKey{
		Kid:       deriveKid(pub),
		Public:    pub,
		Private:   priv,
		CreatedAt: now,
		Status:    "active",
	}, nil
}

func deriveKid(pub ed25519.PublicKey) string {
	x := base64.RawURLEncoding.EncodeToString(pub)
	thumbInput := fmt.Sprintf(`{"crv":"Ed25519","kty":"OKP","x":"%s"}`, x)
	sum := sha256.Sum256([]byte(thumbInput))
	return "ds_" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// PublicJWK is one Ed25519 public key in JWK form (PROTOCOL §6.5).
type PublicJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	X   string `json:"x"`
}

// JWKS is the JSON Web Key Set served at __ds/jwks.json.
type JWKS struct {
	Keys []PublicJWK `json:"keys"`
}

// JWK renders a signing key's public half as a JWK. Note alg is "EdDSA" here
// (the JWK algorithm), distinct from the "ed25519" in subscription metadata.
func (k SigningKey) JWK() PublicJWK {
	return PublicJWK{
		Kty: "OKP",
		Crv: "Ed25519",
		Kid: k.Kid,
		Use: "sig",
		Alg: "EdDSA",
		X:   base64.RawURLEncoding.EncodeToString(k.Public),
	}
}

// BuildJWKS renders a key set, active keys first.
func BuildJWKS(keys []SigningKey) JWKS {
	out := JWKS{Keys: make([]PublicJWK, 0, len(keys))}
	for _, k := range keys {
		out.Keys = append(out.Keys, k.JWK())
	}
	return out
}

// SignWebhookPayload signs a webhook body, returning the Webhook-Signature
// header value "t=<unix>,kid=<kid>,ed25519=<base64url-sig>" where the signature
// is Ed25519 over "<unix>.<raw_body>" (PROTOCOL §7.1).
func SignWebhookPayload(key SigningKey, body []byte, now time.Time) string {
	ts := now.Unix()
	signed := fmt.Sprintf("%d.%s", ts, body)
	sig := ed25519.Sign(key.Private, []byte(signed))
	return fmt.Sprintf("t=%d,kid=%s,ed25519=%s", ts, key.Kid, base64.RawURLEncoding.EncodeToString(sig))
}

// tokenPayload is the decoded body of a callback/claim token. Generation lets
// the fence reject a token minted for a superseded wake (PROTOCOL §7.3, §12.9).
type tokenPayload struct {
	Sub        string `json:"sub"`
	Generation int64  `json:"gen"`
	Exp        int64  `json:"exp"`
	Jti        string `json:"jti"`
}

// GenerateToken mints an HMAC-signed callback or claim token bound to a
// subscription and generation, expiring at now+ttl.
func GenerateToken(tokenKey []byte, subID string, generation int64, now time.Time, ttl time.Duration, rand io.Reader) (string, error) {
	jti := make([]byte, 8)
	if _, err := io.ReadFull(rand, jti); err != nil {
		return "", fmt.Errorf("token jti: %w", err)
	}
	payload := tokenPayload{
		Sub:        subID,
		Generation: generation,
		Exp:        now.Add(ttl).Unix(),
		Jti:        hex.EncodeToString(jti),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	return body + "." + hmacSig(tokenKey, body), nil
}

// tokenRefreshThreshold is how close to expiry a presented (still-valid)
// callback/claim token must be before a successful callback re-mints it in-band,
// so a long-lived heartbeating pull-wake worker is never locked out by an expiry
// it cannot recover from (issue #77). 300s matches the reference
// tokenRefreshThreshold.
const tokenRefreshThreshold = 300 * time.Second

// TokenValidation is the outcome of validating a callback/claim token. Exp is the
// token's expiry (unix seconds) whenever the token is well-formed and ours (Valid
// or Expired), so the shell can drive the in-band refresh decision (issue #77).
type TokenValidation struct {
	Valid      bool
	Expired    bool
	Generation int64
	Exp        int64
}

// TokenExpired reports whether a token expiring at exp (unix seconds) is expired
// at now (unix seconds). Pure: it is the exact expiry predicate ValidateToken
// applies, factored out so the refresh math and its tests share one boundary
// (valid while now <= exp; expired once now > exp).
func TokenExpired(exp, now int64) bool { return now > exp }

// ShouldRefreshToken reports whether a still-valid token expiring at exp (unix
// seconds) should be re-minted in-band at now because it is within threshold of
// expiry (issue #77). Pure: the callback shell passes the validated token's exp
// and the wall clock, so the "should refresh?" decision is unit-testable without
// a clock or Redis. An already-expired token returns false — it is handled by the
// distinct TOKEN_EXPIRED retry path, not the success-response refresh.
func ShouldRefreshToken(exp, now int64, threshold time.Duration) bool {
	if TokenExpired(exp, now) {
		return false
	}
	return exp-now <= int64(threshold.Seconds())
}

// ValidateToken verifies an HMAC token for a subscription. It checks the
// signature in constant time, the subject binding, and expiry, and returns the
// token's generation for the fence comparison.
func ValidateToken(tokenKey []byte, token, subID string, now time.Time) TokenValidation {
	body, sig, ok := strings.Cut(token, ".")
	if !ok {
		return TokenValidation{}
	}
	if !hmac.Equal([]byte(sig), []byte(hmacSig(tokenKey, body))) {
		return TokenValidation{}
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return TokenValidation{}
	}
	var p tokenPayload
	if err := json.Unmarshal(raw, &p); err != nil || p.Sub != subID {
		return TokenValidation{}
	}
	if TokenExpired(p.Exp, now.Unix()) {
		return TokenValidation{Expired: true, Generation: p.Generation, Exp: p.Exp}
	}
	return TokenValidation{Valid: true, Generation: p.Generation, Exp: p.Exp}
}

func hmacSig(key []byte, body string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// writeTokenDomain domain-separates the claim-scoped write token's MAC from
// the callback/claim token's (which signs the bare body). The same tokenKey
// signs both, but neither token can ever validate as the other: presenting a
// write token to ValidateToken (or a callback token to ValidateWriteToken)
// fails the MAC before any payload field is even read.
const writeTokenDomain = "ds-write-v1"

// writeTokenTyp pins the payload shape as defense in depth alongside the
// MAC domain separation.
const writeTokenTyp = "write"

// writeTokenPayload is the decoded body of a claim-scoped write token — the
// append capability minted on an authorized pull-wake claim (issue #126,
// Electric's electric-claim-token contract). Streams is the explicit
// allow-list of normalized stream paths the holder may append to for the life
// of the claim; Sub/Generation bind it to the claim that minted it.
type writeTokenPayload struct {
	Typ        string   `json:"typ"`
	Sub        string   `json:"sub"`
	Generation int64    `json:"gen"`
	Exp        int64    `json:"exp"`
	Jti        string   `json:"jti"`
	Streams    []string `json:"streams"`
}

// GenerateWriteToken mints the claim-scoped write token for a claim on subID
// at generation, scoped to exactly streams, expiring at now+ttl (the lease
// band — the token never outlives the claim by more than the lease). The
// token is opaque to clients, matching Electric's electric-claim-token.
func GenerateWriteToken(tokenKey []byte, subID string, generation int64, streams []auth.StreamPath, now time.Time, ttl time.Duration, rand io.Reader) (string, error) {
	jti := make([]byte, 8)
	if _, err := io.ReadFull(rand, jti); err != nil {
		return "", fmt.Errorf("write token jti: %w", err)
	}
	paths := make([]string, len(streams))
	for i, s := range streams {
		paths[i] = s.String()
	}
	payload := writeTokenPayload{
		Typ:        writeTokenTyp,
		Sub:        subID,
		Generation: generation,
		Exp:        now.Add(ttl).Unix(),
		Jti:        hex.EncodeToString(jti),
		Streams:    paths,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	return body + "." + hmacSig(tokenKey, writeTokenDomain+"."+body), nil
}

// WriteTokenStatus classifies a write-token validation outcome. The shell
// maps it to HTTP: Invalid and Expired are a 401 (no usable credential),
// WrongPath is a 403 (a real credential that does not grant this stream).
type WriteTokenStatus int

const (
	// WriteTokenInvalid is a malformed token, a foreign MAC, or a payload that
	// is not a write token. The zero value: any unproven token is invalid.
	WriteTokenInvalid WriteTokenStatus = iota
	// WriteTokenExpired is ours and well-formed, but past exp.
	WriteTokenExpired
	// WriteTokenWrongPath is ours and unexpired, but the checked path is
	// outside the token's stream scope.
	WriteTokenWrongPath
	// WriteTokenValid authorizes an append at the checked path.
	WriteTokenValid
)

// WriteTokenValidation is the outcome of ValidateWriteToken. SubID and
// Generation are set whenever the MAC proved the token ours (Expired,
// WrongPath, Valid) so the shell can log attribution without re-parsing.
type WriteTokenValidation struct {
	Status     WriteTokenStatus
	SubID      string
	Generation int64
}

// ValidateWriteToken verifies a claim-scoped write token for an append at
// path. The constant-time MAC check runs first, then shape, expiry, and the
// path scope — a caller who could not have minted the token learns nothing
// about its scope from the response.
func ValidateWriteToken(tokenKey []byte, token string, path auth.StreamPath, now time.Time) WriteTokenValidation {
	body, sig, ok := strings.Cut(token, ".")
	if !ok {
		return WriteTokenValidation{}
	}
	if !hmac.Equal([]byte(sig), []byte(hmacSig(tokenKey, writeTokenDomain+"."+body))) {
		return WriteTokenValidation{}
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return WriteTokenValidation{}
	}
	var p writeTokenPayload
	if err := json.Unmarshal(raw, &p); err != nil || p.Typ != writeTokenTyp {
		return WriteTokenValidation{}
	}
	if TokenExpired(p.Exp, now.Unix()) {
		return WriteTokenValidation{Status: WriteTokenExpired, SubID: p.Sub, Generation: p.Generation}
	}
	for _, s := range p.Streams {
		if s == path.String() {
			return WriteTokenValidation{Status: WriteTokenValid, SubID: p.Sub, Generation: p.Generation}
		}
	}
	return WriteTokenValidation{Status: WriteTokenWrongPath, SubID: p.Sub, Generation: p.Generation}
}

// GenerateWakeID returns a unique wake id "w_<hex>" (PROTOCOL §7).
func GenerateWakeID(rand io.Reader) (string, error) {
	b := make([]byte, 12)
	if _, err := io.ReadFull(rand, b); err != nil {
		return "", fmt.Errorf("wake id: %w", err)
	}
	return "w_" + hex.EncodeToString(b), nil
}
