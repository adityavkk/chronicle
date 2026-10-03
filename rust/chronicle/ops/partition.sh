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
    rule -nvL CHRONICLE_FAULT
    rule -D INPUT -j CHRONICLE_FAULT
    rule -D OUTPUT -j CHRONICLE_FAULT
    rule -F CHRONICLE_FAULT
    rule -X CHRONICLE_FAULT
    ;;
  *) exit 2;;
esac
