#!/usr/bin/env bash
# Packet loss inside disposable k3d nodes. Do not disconnect Docker's network:
# removing eth0 also destroys flannel.1 and does not model a healed partition.
set -euo pipefail
action=${1:?usage: partition.sh isolate|heal agent-number}
ordinal=${2:?agent number}
[[ "$ordinal" =~ ^[0-9]+(-[0-9]+)?$ ]] || exit 2
node="k3d-chronicle-rust-agent-$ordinal"
sudo docker inspect "$node" >/dev/null
rule() { sudo docker exec "$node" iptables "$@"; }
case "$action" in
  isolate)
    rule -N CHRONICLE_FAULT
    rule -I INPUT 1 -j CHRONICLE_FAULT
    rule -I OUTPUT 1 -j CHRONICLE_FAULT
    for peer in $(sudo docker network inspect k3d-chronicle-rust --format '{{range .Containers}}{{.Name}}{{println}}{{end}}'); do
      [[ "$peer" == k3d-chronicle-rust-agent-* && "$peer" != "$node" ]] || continue
      ip=$(sudo docker inspect "$peer" --format '{{(index .NetworkSettings.Networks "k3d-chronicle-rust").IPAddress}}')
      rule -A CHRONICLE_FAULT -s "$ip" -j DROP
      rule -A CHRONICLE_FAULT -d "$ip" -j DROP
    done
    ;;
  heal)
    # Injection may fail between chain creation and either jump. Inspect once,
    # attempt every remaining removal, and report failures rather than hiding them.
    rules=$(rule -S)
    failed=0
    if grep -q '^-N CHRONICLE_FAULT$' <<<"$rules"; then
      rule -nvL CHRONICLE_FAULT || failed=1
      for hook in INPUT OUTPUT; do
        if grep -q "^-A $hook -j CHRONICLE_FAULT$" <<<"$rules"; then
          rule -D "$hook" -j CHRONICLE_FAULT || failed=1
        fi
      done
      rule -F CHRONICLE_FAULT || failed=1
      rule -X CHRONICLE_FAULT || failed=1
    fi
    exit "$failed"
    ;;
  *) exit 2;;
esac
