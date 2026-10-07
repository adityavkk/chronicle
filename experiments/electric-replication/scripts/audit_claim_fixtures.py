"""Read-only audit of the published subscription-fault Authorization fixtures.

Requires the retired local lab WALs to independently verify signatures. Never
prints tokens, key bytes, or decoded claims. Exits nonzero on any failed check.
This is a specific fixture audit, not a general-purpose secret scanner.
"""
import base64
import gzip
import hashlib
import hmac
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys

from lab import ROOT, partition


def git(*args):
    return subprocess.check_output(["/usr/bin/git", *args], cwd=ROOT)


def decode(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def token_fields(value):
    payload, signature = value.split(".")  # Actual format: NOT a JWT.
    claims = json.loads(decode(payload))
    assert set(claims) == {"path", "incarnation", "generation", "wake", "kind"}
    assert claims["path"].startswith("/r/__ds/subscriptions/")
    assert claims["kind"] in ("webhook", "pull-wake")
    return payload.encode(), signature, claims


def audit(commit):
    prefix = "experiments/electric-replication/evidence/"
    rows = []
    private_keys = set()
    seen_tokens = set()
    pids = 0
    for number in range(1, 5):
        run = f"subscription-fault-{number:03}"
        path = prefix + run + "/"
        history = [json.loads(line) for line in git("show", commit + ":" + path + "history.jsonl").splitlines()]
        deliveries = [json.loads(line) for line in git("show", commit + ":" + path + "deliveries.jsonl").splitlines()]
        keys = {0: set(), 1: set()}
        for node in (1, 2, 3):
            config = json.loads(git("show", commit + ":" + path + f"node-{node}.json"))
            assert config["cluster"] == run and config["node"] == node
            assert ipaddress.ip_address(config["listen"].rsplit(":", 1)[0]).is_loopback
            assert all(ipaddress.ip_address(n["addr"].rsplit(":", 1)[0]).is_loopback for n in config["genesis"].values())
            data = Path(config["dir"])
            assert data == ROOT / ".tmp/electric-labs" / run / str(node)
            pid = int(git("show", commit + ":" + path + f"node-{node}.pid"))
            assert not Path(f"/proc/{pid}").exists(), "recorded lab process still exists"
            pids += 1
            # Keys are Action::Keys JSON inside real native WAL command frames.
            # Match complete arrays, then verify with independent HMAC-SHA256.
            for group in (0, 1):
                for wal in (data / str(group) / "wal").glob("*.wal"):
                    for match in re.finditer(rb'\{"Keys":\{"signing":\[[0-9,]+\],"token":\[[0-9,]+\]\}\}', wal.read_bytes()):
                        pair = json.loads(match[0])["Keys"]
                        assert len(pair["signing"]) == len(pair["token"]) == 32
                        keys[group].add(bytes(pair["token"]))
                        private_keys.update(bytes(pair[k]) for k in ("signing", "token"))
        assert all(len(k) == 1 for k in keys.values()), "missing or ambiguous retired-run key"

        issued = set()
        for event in history + deliveries:
            field = "result" if "result" in event else "body"
            try:
                value = json.loads(base64.b64decode(event.get(field, "")))
            except (ValueError, UnicodeDecodeError):
                continue
            if isinstance(value, dict):
                issued.update(value[k] for k in ("token", "callback_token") if k in value)
        valid, tampered = 0, 0
        for event in history:
            raw = event.get("headers", {}).get("authorization")
            if raw is None:
                continue
            assert raw.startswith("Bearer ") and event["node"] in (1, 2, 3)
            token = raw.removeprefix("Bearer ")
            payload, signature, claims = token_fields(token)
            assert event["method"] == "POST"
            assert event["path"] in (claims["path"] + "/" + operation for operation in ("ack", "release", "callback"))
            key = next(iter(keys[partition(claims["path"], 2)]))
            expected = base64.urlsafe_b64encode(hmac.digest(key, payload, "sha256")).rstrip(b"=").decode()
            if token in issued:
                assert hmac.compare_digest(signature, expected), "issued token signature does not match retired lab"
                valid += 1
            else:
                assert token.endswith("tampered") and token[:-8] in issued
                assert event["status"] == 401 and not hmac.compare_digest(signature, expected)
                tampered += 1
            seen_tokens.add(token)
        assert (valid, tampered) == (14, 1), "unexpected fixture ledger"
        rows.append(dict(run=run, authorization_matches=valid+tampered, verified_issued=valid,
                         intentionally_tampered_401=tampered, owner_groups_with_distinct_keys=len(keys)))

    assert len(private_keys) == 16, "test runs unexpectedly reused signing/HMAC keys"
    # Search ALL blobs in the originally unpublished history, not only HEAD.
    # Include raw bytes, hex/base64 and JSON/Rust decimal arrays, including
    # the nested byte-array form of a serde JSON command printed by Debug.
    needles = set()
    for key in private_keys:
        forms = {key, key.hex().encode(), key.hex().upper().encode(),
                 base64.b64encode(key), base64.urlsafe_b64encode(key),
                 base64.b64encode(key).rstrip(b"="), base64.urlsafe_b64encode(key).rstrip(b"="),
                 json.dumps(list(key)).encode(), json.dumps(list(key), separators=(",", ":")).encode()}
        for form in list(forms):
            if form.startswith(b"["):
                forms.add(json.dumps(list(form)).encode())
                forms.add(json.dumps(list(form), separators=(",", ":")).encode())
        needles.update(forms)
    objects = git("rev-list", "--objects", "origin/main.." + commit).splitlines()
    count = 0
    compressed = 0
    for obj in objects:
        sha, _, path = obj.partition(b" ")
        if git("cat-file", "-t", sha.decode()).strip() != b"blob":
            continue
        content = git("cat-file", "blob", sha.decode())
        if path.endswith(b".gz"):
            content = gzip.decompress(content)
            compressed += 1
        assert not any(needle in content for needle in needles), "private lab key representation published in " + path.decode()
        count += 1
    services = subprocess.check_output(["amp", "orb", "service", "list"], cwd=ROOT, text=True)
    assert "No orb services are running." in services, "check active services before concluding fixtures retired"
    return dict(verdict="PASS", published_commit=commit, runs=rows,
                validator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                authorization_matches=sum(r["authorization_matches"] for r in rows),
                unique_token_values=len(seen_tokens),
                retired_recorded_pids_absent=pids, distinct_private_lab_keys_checked=len(private_keys),
                historical_blobs_scanned=count, gzip_blobs_decompressed=compressed,
                private_lab_key_matches=0, token_format="base64url(JSON claims).base64url(HMAC-SHA256)",
                issuer="per-group getrandom-generated Keys; replicated Action::Keys; persisted only in local lab WAL/snapshots",
                consumer="loopback lab subscription ack/release/callback; signature plus path/incarnation/generation/wake/lease fencing",
                limitations="Specific 60-match audit plus finite key-encoding scan; not proof against arbitrary encodings. Retired untracked lab data remains local, not for deployment reuse.")


if __name__ == "__main__":
    print(json.dumps(audit(sys.argv[1]), indent=2))
