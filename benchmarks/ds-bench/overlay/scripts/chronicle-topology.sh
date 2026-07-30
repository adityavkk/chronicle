#!/usr/bin/env bash
# Chronicle topology helpers. This file is sourced by scripts/lib-bench.sh.

_CHRONICLE_REDIS_POOL_SIZE_OVERRIDE="${CHRONICLE_REDIS_POOL_SIZE:-}"

chronicle_configure() {
  local config="$1"
  local fsync f2 f3 f4 f5 f6 f7 f8 extra
  IFS=: read -r fsync f2 f3 f4 f5 f6 f7 f8 extra <<EOF
${config}
EOF
  case "$fsync" in
    always|everysec|no) ;;
    *) echo "invalid Chronicle appendfsync '$fsync'" >&2; return 1 ;;
  esac

  if [ -z "$f4" ]; then
    [ -z "$extra" ] || {
      echo "legacy Chronicle config has too many fields: '$config'" >&2
      return 1
    }
    case "${f2}:${f3}" in
      *[!0-9:]*|:|*:) echo "invalid Chronicle CPU split '${f2}:${f3}'" >&2; return 1 ;;
    esac
    [ $((f2 + f3)) -eq "${SERVER_CPU}" ] || {
      echo "Chronicle CPU split ${f2}+${f3} must equal SERVER_CPU=${SERVER_CPU}" >&2
      return 1
    }
    export CHRONICLE_TOPOLOGY="colocated"
    export CHRONICLE_APPEND_FSYNC="$fsync"
    export CHRONICLE_CPU="$f2" REDIS_CPU="$f3"
    if [ "${DS_TARGET}" = "local" ]; then
      export CHRONICLE_MEM="${CHRONICLE_MEM:-512Mi}" REDIS_MEM="${REDIS_MEM:-1536Mi}"
      export REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-1gb}"
    else
      export CHRONICLE_MEM="${CHRONICLE_MEM:-4Gi}" REDIS_MEM="${REDIS_MEM:-12Gi}"
      export REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-10gb}"
    fi
    export CHRONICLE_REPLICAS=1 REDIS_MASTERS=1
    export CHRONICLE_SSE_WAIT_MODE=legacy CHRONICLE_SSE_PERSISTENT_WAIT=false
    export CHRONICLE_ACTIVE_CONFIG="$config"
    return 0
  fi

  [ -z "$extra" ] || {
    echo "Chronicle topology config has too many fields: '$config'" >&2
    return 1
  }
  case "${f2}:${f3}:${f4}:${f5}:${f6}:${f7}" in
    *[!0-9:]*|:|*:) echo "Chronicle topology contains a nonnumeric field" >&2; return 1 ;;
  esac
  case "$f2" in 1|2|4) ;; *) echo "Chronicle replicas must be 1, 2, or 4" >&2; return 1 ;; esac
  case "$f3" in 1|3) ;; *) echo "Redis masters must be 1 or 3" >&2; return 1 ;; esac
  case "$f8" in
    legacy) export CHRONICLE_SSE_PERSISTENT_WAIT=false ;;
    persistent) export CHRONICLE_SSE_PERSISTENT_WAIT=true ;;
    *) echo "SSE wait mode must be legacy or persistent" >&2; return 1 ;;
  esac
  [ "$f4" -gt 0 ] && [ "$f5" -gt 0 ] &&
    [ "$f6" -gt 0 ] && [ "$f7" -gt 0 ] || {
      echo "Chronicle topology resources must be positive" >&2
      return 1
    }
  [ $((f4 + f5)) -eq $((SERVER_CPU * 1000)) ] || {
    echo "Chronicle topology CPU ${f4}+${f5} must equal $((SERVER_CPU * 1000))m" >&2
    return 1
  }
  local expected_memory_mib="${CHRONICLE_SUT_MEMORY_MIB:-16384}"
  [ $((f6 + f7)) -eq "$expected_memory_mib" ] || {
    echo "Chronicle topology memory ${f6}+${f7} must equal ${expected_memory_mib}MiB" >&2
    return 1
  }
  [ $((f4 / f2)) -gt 0 ] && [ $((f5 / f3)) -gt 0 ] &&
    [ $((f6 / f2)) -gt 0 ] && [ $((f7 / f3)) -gt 0 ] || {
      echo "Chronicle topology gives a pod no resources" >&2
      return 1
    }
  [ $((SERVER_CPU * 1000 - (f4 / f2 * f2) - (f5 / f3 * f3))) -lt 3 ] || {
    echo "Chronicle topology leaves three or more millicores unused" >&2
    return 1
  }
  [ $((expected_memory_mib - (f6 / f2 * f2) - (f7 / f3 * f3))) -lt 3 ] || {
    echo "Chronicle topology leaves three or more MiB unused" >&2
    return 1
  }

  export CHRONICLE_REPLICAS="$f2" REDIS_MASTERS="$f3"
  export CHRONICLE_CPU_TOTAL_MILLIS="$f4" REDIS_CPU_TOTAL_MILLIS="$f5"
  export CHRONICLE_MEMORY_TOTAL_MIB="$f6" REDIS_MEMORY_TOTAL_MIB="$f7"
  export CHRONICLE_CPU_PER_POD=$((f4 / f2))
  export REDIS_CPU_PER_POD=$((f5 / f3))
  export CHRONICLE_MEMORY_PER_POD=$((f6 / f2))
  export REDIS_MEMORY_PER_POD=$((f7 / f3))
  export REDIS_MAXMEMORY_PER_POD=$((REDIS_MEMORY_PER_POD * 5 / 6))
  export CHRONICLE_APPEND_FSYNC="$fsync"
  export CHRONICLE_SSE_WAIT_MODE="$f8"
  export CHRONICLE_TOPOLOGY="shared"
  export CHRONICLE_REDIS_READY_HOST="chronicle-redis"
  export CHRONICLE_REDIS_URL="redis://chronicle-redis:6379/15"
  if [ "$REDIS_MASTERS" = "3" ]; then
    export CHRONICLE_TOPOLOGY="cluster"
    export CHRONICLE_REDIS_READY_HOST="chronicle-redis-cluster-0.chronicle-redis-cluster"
    export CHRONICLE_REDIS_URL="redis+cluster://chronicle-redis-cluster-0.chronicle-redis-cluster:6379,chronicle-redis-cluster-1.chronicle-redis-cluster:6379,chronicle-redis-cluster-2.chronicle-redis-cluster:6379"
  fi
  export CHRONICLE_ACTIVE_CONFIG="$config"
}

chronicle_delete_topology() {
  K delete deployment chronicle chronicle-redis \
    --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  K delete statefulset chronicle-redis-cluster \
    --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  K delete job chronicle-redis-cluster-init \
    --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  K delete service chronicle chronicle-redis chronicle-redis-cluster \
    --ignore-not-found >/dev/null 2>&1 || true
}

chronicle_manifest_vars() {
  printf '%s' "${MANIFEST_VARS} \
\${CHRONICLE_APPEND_FSYNC} \${CHRONICLE_REPLICAS} \
\${CHRONICLE_TOPOLOGY} \${CHRONICLE_SSE_WAIT_MODE} \
\${CHRONICLE_SSE_PERSISTENT_WAIT} \${CHRONICLE_REDIS_URL} \
\${CHRONICLE_REDIS_READY_HOST} \${CHRONICLE_REDIS_POOL_SIZE} \
\${CHRONICLE_CPU_PER_POD} \${REDIS_CPU_PER_POD} \
\${CHRONICLE_MEMORY_PER_POD} \${REDIS_MEMORY_PER_POD} \
\${REDIS_MAXMEMORY_PER_POD}"
}

chronicle_wait_topology() {
  if [ "$CHRONICLE_TOPOLOGY" = "cluster" ]; then
    K rollout status statefulset/chronicle-redis-cluster --timeout=600s >&2 || return 1
    K wait --for=condition=complete job/chronicle-redis-cluster-init \
      --timeout=600s >&2 || return 1
    local cluster_info
    cluster_info="$(K exec chronicle-redis-cluster-0 -c redis -- \
      redis-cli cluster info 2>/dev/null)" || return 1
    echo "$cluster_info" | grep -q '^cluster_state:ok' || return 1
    echo "$cluster_info" | grep -q '^cluster_slots_assigned:16384' || return 1
  else
    K rollout status deployment/chronicle-redis --timeout=600s >&2 || return 1
  fi
  K rollout status deployment/chronicle --timeout=600s >&2 || return 1

  local ready="" endpoints=""
  local i
  for i in $(seq 1 120); do
    ready="$(K get deployment chronicle \
      -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [ "${ready:-0}" = "$CHRONICLE_REPLICAS" ] && break
    sleep 1
  done
  [ "${ready:-0}" = "$CHRONICLE_REPLICAS" ] || {
    echo "Chronicle ready replicas ${ready:-0}/${CHRONICLE_REPLICAS}" >&2
    return 1
  }
  for i in $(seq 1 120); do
    endpoints="$(K get endpoints chronicle \
      -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' \
      2>/dev/null | awk 'NF { count++ } END { print count+0 }')"
    [ "${endpoints:-0}" = "$CHRONICLE_REPLICAS" ] && break
    sleep 1
  done
  [ "${endpoints:-0}" = "$CHRONICLE_REPLICAS" ] || {
    echo "Chronicle Service endpoints ${endpoints:-0}/${CHRONICLE_REPLICAS}" >&2
    return 1
  }
  _wait_server_http chronicle:4437
}

chronicle_deploy() {
  local config="$1"
  chronicle_configure "$config" || return 1
  if [ "$CHRONICLE_TOPOLOGY" = "colocated" ]; then
    export CHRONICLE_REDIS_POOL_SIZE="${_CHRONICLE_REDIS_POOL_SIZE_OVERRIDE:-1024}"
  else
    # The persistent SSE diagnostic can hold one Pub/Sub connection per
    # reader. Keep the pool above the largest 2,048-connection discriminator.
    export CHRONICLE_REDIS_POOL_SIZE="${_CHRONICLE_REDIS_POOL_SIZE_OVERRIDE:-4096}"
  fi

  if [ "$CHRONICLE_TOPOLOGY" = "colocated" ]; then
    envsubst "${MANIFEST_VARS} \${CHRONICLE_APPEND_FSYNC} \${CHRONICLE_CPU} \${REDIS_CPU} \${CHRONICLE_MEM} \${REDIS_MEM} \${REDIS_MAXMEMORY} \${CHRONICLE_REDIS_POOL_SIZE}" \
      < gke/chronicle.yaml | K apply -f - >&2 || return 1
    K rollout status deployment/chronicle --timeout=600s >&2 || return 1
    return 0
  fi

  chronicle_delete_topology
  local variables
  variables="$(chronicle_manifest_vars)"
  if [ "$CHRONICLE_TOPOLOGY" = "cluster" ]; then
    envsubst "$variables" < gke/chronicle-redis-cluster.yaml |
      K apply -f - >&2 || return 1
  else
    envsubst "$variables" < gke/chronicle-redis-shared.yaml |
      K apply -f - >&2 || return 1
  fi
  envsubst "$variables" < gke/chronicle-app.yaml |
    K apply -f - >&2 || return 1
  chronicle_wait_topology
}

chronicle_reset() {
  if [ "${RESET_DRYRUN:-0}" = "1" ]; then
    if [ "${CHRONICLE_TOPOLOGY:-colocated}" = "colocated" ]; then
      echo "kubectl --context $KCTX -n ds-bench rollout restart deploy/chronicle"
    else
      echo "chronicle redeploy ${CHRONICLE_ACTIVE_CONFIG:?}"
    fi
    return 0
  fi
  if [ "${CHRONICLE_TOPOLOGY:-colocated}" = "colocated" ]; then
    K rollout restart deployment/chronicle
    K rollout status deployment/chronicle --timeout=120s
    _wait_server_http chronicle:4437
    return 0
  fi
  chronicle_deploy "${CHRONICLE_ACTIVE_CONFIG:?}"
}

chronicle_reset_sidecar_samples() {
  local pods pod component attempt reset_ok
  pods="$(K get pod -l ds-bench-sut=chronicle \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' \
    2>/dev/null || true)"
  [ -n "$pods" ] || {
    echo "WARN: no Chronicle SUT pods found for metrics reset" >&2
    return 1
  }
  for pod in $pods; do
    reset_ok=0
    K wait --for=condition=Ready "pod/${pod}" --timeout=120s >/dev/null 2>&1 || true
    for attempt in 1 2 3 4 5; do
      if K exec "$pod" -c metrics -- sh -c \
          'echo "ts_ms,rss_bytes,cpu_ticks,write_bytes,pod_ws_bytes" > /metrics/samples.csv'; then
        reset_ok=1
        break
      fi
      sleep 2
    done
    [ "$reset_ok" = "1" ] || {
      echo "WARN: metrics reset failed on ${pod}" >&2
      return 1
    }
    component="$(K get pod "$pod" \
      -o jsonpath='{.metadata.labels.ds-bench-component}' 2>/dev/null || true)"
    if [ "$component" = "redis" ] &&
        [ "${CHRONICLE_TOPOLOGY:-}" = "shared" ]; then
      K exec "$pod" -c commandstats -- sh -c \
        'echo "ts_ms,zrangebylex,publish,subscribe,unsubscribe,hgetall,evalsha,hset,pexpire,connected_clients,pubsub_clients" > /metrics/redis-commandstats.csv' || {
        echo "WARN: Redis commandstats reset failed on ${pod}" >&2
        return 1
      }
    elif [ "$component" = "chronicle" ] &&
        [ "${CHRONICLE_CAPTURE_PPROF:-1}" = "1" ]; then
      K exec "$pod" -c metrics -- sh -c '
        rm -rf /metrics/pprof
        mkdir -p /metrics/pprof
        seconds="$1"
        (
          if curl -fsS --max-time "$((seconds + 15))" \
              "http://127.0.0.1:9090/debug/pprof/profile?seconds=${seconds}" \
              -o /metrics/pprof/cpu.pprof.tmp; then
            mv /metrics/pprof/cpu.pprof.tmp /metrics/pprof/cpu.pprof
            echo 0 > /metrics/pprof/cpu.done
          else
            echo 1 > /metrics/pprof/cpu.done
          fi
        ) >/metrics/pprof/cpu.log 2>&1 </dev/null &
      ' sh "${CHRONICLE_PPROF_SECONDS:-40}" || {
        echo "WARN: could not start CPU profile on ${pod}" >&2
        return 1
      }
    fi
  done
}

chronicle_prepare_collect_dest() {
  local dest="$1"
  local raw="${dest}/sut-samples" info_dir="${dest}/redis-info"
  local commandstats_dir="${dest}/redis-commandstats"
  mkdir -p "$raw" "$info_dir" "$commandstats_dir"
  # Saturation confirmation reuses p<pods>-r1 after the exploratory walk. Remove
  # the prior pod identities and aggregate before collecting the fresh repeat.
  # Otherwise old and new samples have disjoint time buckets and can leave a
  # stale samples.csv behind after aggregation fails.
  find "$raw" -maxdepth 1 -type f -name '*.csv' -delete
  find "$info_dir" -maxdepth 1 -type f -name '*.txt' -delete
  find "$commandstats_dir" -maxdepth 1 -type f -name '*.csv' -delete
  find "$dest" -maxdepth 1 -type f \
    \( -name 'samples.csv' -o -name 'sockets-*.txt' \
       -o -name 'chronicle-endpoints.json' \) -delete
  rm -rf "${dest}/pprof"
}

chronicle_collect_sidecars() {
  local dest="$1"
  local raw="${dest}/sut-samples" info_dir="${dest}/redis-info"
  local commandstats_dir="${dest}/redis-commandstats"
  local pods pod component attempt remote_sha local_sha target copied count=0
  chronicle_prepare_collect_dest "$dest"
  pods="$(K get pod -l ds-bench-sut=chronicle \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' \
    2>/dev/null || true)"
  for pod in $pods; do
    component="$(K get pod "$pod" \
      -o jsonpath='{.metadata.labels.ds-bench-component}' 2>/dev/null || true)"
    [ -n "$component" ] || component=unknown
    target="${raw}/${component}-${pod}.csv"
    copied=0
    for attempt in 1 2 3; do
      remote_sha="$(K exec "$pod" -c metrics -- sh -c \
        'cp /metrics/samples.csv /metrics/samples.snapshot.csv && sha256sum /metrics/samples.snapshot.csv' \
        2>/dev/null | awk '{print $1}')"
      K cp "ds-bench/${pod}:/metrics/samples.snapshot.csv" "$target" -c metrics || true
      local_sha="$(python3 -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' \
        "$target" 2>/dev/null || true)"
      if [ -n "$remote_sha" ] && [ "$local_sha" = "$remote_sha" ] &&
          _samples_complete "$target"; then
        copied=1
        count=$((count + 1))
        break
      fi
      sleep 2
    done
    [ "$copied" = "1" ] || {
      echo "WARN: could not collect complete samples from ${pod}" >&2
      return 1
    }

    if [ "$component" = "redis" ]; then
      {
        K exec "$pod" -c redis -- redis-cli info commandstats
        K exec "$pod" -c redis -- redis-cli info clients
        K exec "$pod" -c redis -- redis-cli info persistence
        [ "${CHRONICLE_TOPOLOGY:-}" = "cluster" ] &&
          K exec "$pod" -c redis -- redis-cli cluster info
      } > "${info_dir}/${pod}.txt" 2>&1 || true
      if [ "${CHRONICLE_TOPOLOGY:-}" = "shared" ]; then
        target="${commandstats_dir}/${pod}.csv"
        copied=0
        for attempt in 1 2 3; do
          remote_sha="$(K exec "$pod" -c commandstats -- sh -c \
            'cp /metrics/redis-commandstats.csv /metrics/redis-commandstats.snapshot.csv && sha256sum /metrics/redis-commandstats.snapshot.csv' \
            2>/dev/null | awk '{print $1}')"
          K cp "ds-bench/${pod}:/metrics/redis-commandstats.snapshot.csv" \
            "$target" -c commandstats || true
          local_sha="$(python3 -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' \
            "$target" 2>/dev/null || true)"
          if [ -n "$remote_sha" ] && [ "$local_sha" = "$remote_sha" ]; then
            copied=1
            break
          fi
          sleep 2
        done
        [ "$copied" = "1" ] || {
          echo "WARN: could not collect Redis commandstats samples from ${pod}" >&2
          return 1
        }
      fi
    elif [ "$component" = "chronicle" ]; then
      K exec "$pod" -c metrics -- sh -c \
        'for pid in $(pgrep -x chronicle 2>/dev/null); do printf "%s " "$pid"; find "/proc/$pid/fd" -type l -lname "socket:*" 2>/dev/null | wc -l; done' \
        > "${dest}/sockets-${pod}.txt" 2>&1 || true
      if [ "${CHRONICLE_CAPTURE_PPROF:-1}" = "1" ]; then
        local profile profile_dir="${dest}/pprof" profile_target
        mkdir -p "$profile_dir"
        for attempt in $(seq 1 90); do
          [ "$(K exec "$pod" -c metrics -- sh -c \
            'cat /metrics/pprof/cpu.done 2>/dev/null || true' 2>/dev/null)" = "0" ] && break
          sleep 1
        done
        [ "$(K exec "$pod" -c metrics -- sh -c \
          'cat /metrics/pprof/cpu.done 2>/dev/null || true' 2>/dev/null)" = "0" ] || {
          echo "WARN: CPU profile did not complete on ${pod}" >&2
          return 1
        }
        K exec "$pod" -c metrics -- sh -ec '
          base=http://127.0.0.1:9090/debug/pprof
          curl -fsS "${base}/heap?gc=1" -o /metrics/pprof/heap.pprof
          curl -fsS "${base}/allocs" -o /metrics/pprof/allocs.pprof
          curl -fsS "${base}/block" -o /metrics/pprof/block.pprof
          curl -fsS "${base}/mutex" -o /metrics/pprof/mutex.pprof
          curl -fsS "${base}/goroutine" -o /metrics/pprof/goroutine.pprof
          curl -fsS http://127.0.0.1:9090/metrics -o /metrics/pprof/metrics.txt
        ' || {
          echo "WARN: could not snapshot runtime profiles on ${pod}" >&2
          return 1
        }
        for profile in cpu.pprof heap.pprof allocs.pprof block.pprof mutex.pprof goroutine.pprof metrics.txt; do
          profile_target="${profile_dir}/${pod}-${profile}"
          copied=0
          for attempt in 1 2 3; do
            remote_sha="$(K exec "$pod" -c metrics -- \
              sha256sum "/metrics/pprof/${profile}" 2>/dev/null | awk '{print $1}')"
            K cp "ds-bench/${pod}:/metrics/pprof/${profile}" \
              "$profile_target" -c metrics || true
            local_sha="$(python3 -c 'import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' \
              "$profile_target" 2>/dev/null || true)"
            if [ -n "$remote_sha" ] && [ "$local_sha" = "$remote_sha" ]; then
              copied=1
              break
            fi
            sleep 2
          done
          [ "$copied" = "1" ] || {
            echo "WARN: could not collect verified ${profile} from ${pod}" >&2
            return 1
          }
        done
      fi
    fi
  done
  local expected=$((CHRONICLE_REPLICAS + REDIS_MASTERS))
  [ "$count" -eq "$expected" ] || {
    echo "Chronicle SUT sample count ${count}/${expected}" >&2
    return 1
  }
  K get endpoints chronicle -o json > "${dest}/chronicle-endpoints.json" 2>/dev/null || true
  python3 scripts/aggregate_chronicle_samples.py "$raw" "${dest}/samples.csv"
}

chronicle_server_images() {
  K get deployment/chronicle \
    -o jsonpath='{range .spec.template.spec.containers[*]}chronicle/{.name}={.image}{"\n"}{end}'
  if [ "${CHRONICLE_TOPOLOGY:-colocated}" = "shared" ]; then
    K get deployment/chronicle-redis \
      -o jsonpath='{range .spec.template.spec.containers[*]}redis/{.name}={.image}{"\n"}{end}'
  elif [ "${CHRONICLE_TOPOLOGY:-colocated}" = "cluster" ]; then
    K get statefulset/chronicle-redis-cluster \
      -o jsonpath='{range .spec.template.spec.containers[*]}redis-cluster/{.name}={.image}{"\n"}{end}'
  fi
}
