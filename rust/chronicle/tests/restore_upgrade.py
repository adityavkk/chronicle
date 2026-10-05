#!/usr/bin/env python3
"""External history hook: restore candidate voters 1/2/3, then retire node 4.

Only the disposable chronicle-upgrade cluster is supported. No mutation is retried;
an ambiguous response leaves retained evidence for inspection, not a reset.
"""
import argparse
import json
import subprocess
import time
import urllib.request

from leadership_balance import SERVER, api, kube


def owners():
    pods = json.loads(kube("-n", "chronicle", "get", "pods", "-l", "app=chronicle-raft", "-o", "json"))["items"]
    result = {}
    for pod in pods:
        status = pod["status"]["containerStatuses"][0]
        if not status["ready"] or pod["spec"]["containers"][0]["image"] != "chronicle-raft:leadership4":
            raise RuntimeError("unexpected candidate image or unready process")
        result[pod["metadata"]["name"]] = [pod["metadata"]["uid"], status["containerID"], status["restartCount"]]
    if len(result) != 5:
        raise RuntimeError("requires five candidate processes")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    inspected = json.loads(subprocess.check_output(["sudo", "docker", "inspect", SERVER]))[0]
    address = inspected["NetworkSettings"]["Networks"]["k3d-chronicle-upgrade"]["IPAddress"]
    url = f"http://{address}:30644"
    with open(args.output, "x") as log:
        def note(phase, **fields):
            log.write(json.dumps({"phase": phase, "time_ns": time.monotonic_ns(), **fields}) + "\n")
            log.flush()

        before = owners()
        state = api(url, "/admin/control")
        note("before", owners=before, control=state)
        if not all(p["complete"] and p["voters"] == [1, 2, 4] for p in state["placements"].values()):
            raise RuntimeError("requires completed 1/2/4 placement")
        if api(url, "/admin/retirement/3") is not True:
            raise RuntimeError("node 3 must already be retired")
        for identity, draining in [(3, False), (4, True)]:
            node = dict(state["nodes"][str(identity)], draining=draining)
            note("register-invoke", identity=identity, node=node)
            request = urllib.request.Request(url + "/admin/register", data=json.dumps([identity, node]).encode(),
                                             headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    note("register-return", identity=identity, status=response.status, result=json.load(response))
            except Exception as error:
                note("register-unknown", identity=identity, error=repr(error))
                raise
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            state = api(url, "/admin/control")
            note("observe", control=state)
            if all(p["complete"] and p["voters"] == [1, 2, 3] for p in state["placements"].values()) and api(url, "/admin/retirement/4") is True:
                after = owners()
                note("restored", owners=after, control=state)
                if before != after:
                    raise RuntimeError("a process restart masked restoration")
                return
            time.sleep(1)
        raise TimeoutError("restoration/retirement did not complete")


if __name__ == "__main__":
    main()
