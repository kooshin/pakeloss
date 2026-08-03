#!/usr/bin/env bash

set -Eeuo pipefail

duration_seconds="${LOAD_TEST_DURATION_SECONDS:-15}"
min_packets_per_second="${LOAD_TEST_MIN_PACKETS_PER_SECOND:-500}"
log_root="${LOAD_TEST_LOG_DIR:-$(mktemp -d -t pakeloss-load-test-logs.XXXXXX)}"

if [[ ! "${duration_seconds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "LOAD_TEST_DURATION_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "${min_packets_per_second}" =~ ^[1-9][0-9]*$ ]]; then
  echo "LOAD_TEST_MIN_PACKETS_PER_SECOND must be a positive integer" >&2
  exit 2
fi

required_packets=$((duration_seconds * min_packets_per_second))
work_dir="$(mktemp -d -t pakeloss-load-test.XXXXXX)"
run_dir="${log_root}/run-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "${work_dir}/bin" "${run_dir}"

readonly grpc_addr="127.0.0.1:18443"
readonly http_addr="127.0.0.1:18080"
readonly api_base="http://${http_addr}/api/v1"
readonly token="pakeloss-load-test-token"

pids=()

api_get() {
  curl --fail --silent --show-error \
    --header "Authorization: Bearer ${token}" \
    "${api_base}/$1"
}

api_post() {
  curl --fail --silent --show-error \
    --request POST \
    --header "Authorization: Bearer ${token}" \
    "${api_base}/$1"
}

processes_alive() {
  local pid
  for pid in "${pids[@]}"; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      return 1
    fi
  done
}

dump_diagnostics() {
  api_get agents >"${run_dir}/agents-final.json" 2>/dev/null || true
  api_get flows >"${run_dir}/flows-final.json" 2>/dev/null || true
  echo "load test diagnostics: ${run_dir}" >&2
  for log_file in "${run_dir}"/*.log; do
    [[ -e "${log_file}" ]] || continue
    echo "===== ${log_file} =====" >&2
    tail -n 200 "${log_file}" >&2 || true
  done
  if [[ -s "${run_dir}/agents-final.json" ]]; then
    jq 'map({agent_id, status, active_flows, active_config_version, desired_config_version})' \
      "${run_dir}/agents-final.json" >&2 || true
  fi
  if [[ -s "${run_dir}/flows-final.json" ]]; then
    jq 'map({flow_id, desired_state, actual_state, tx_total, rx_total, lost_total, last_error})' \
      "${run_dir}/flows-final.json" >&2 || true
  fi
}

cleanup() {
  local exit_code=$?
  local pid
  trap - EXIT INT TERM

  if ((exit_code != 0)); then
    dump_diagnostics
  fi

  for pid in "${pids[@]}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${pids[@]}"; do
    wait "${pid}" 2>/dev/null || true
  done

  if [[ -d "${work_dir}" && "${work_dir}" == /tmp/pakeloss-load-test.* ]]; then
    rm -rf -- "${work_dir}"
  fi
  exit "${exit_code}"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

wait_for() {
  local description="$1"
  local attempt
  shift

  for attempt in $(seq 1 60); do
    if ! processes_alive; then
      echo "a load test process exited while waiting for ${description}" >&2
      return 1
    fi
    if "$@"; then
      return 0
    fi
    sleep 0.5
  done

  echo "timed out waiting for ${description}" >&2
  return 1
}

controller_ready() {
  api_get status >/dev/null 2>&1
}

agents_ready() {
  api_get agents | jq -e 'length == 3 and all(.[]; .status == "online")' >/dev/null
}

flows_discovered() {
  api_get flows | jq -e 'length == 6' >/dev/null
}

flows_running() {
  api_get flows | jq -e \
    'length == 6 and all(.[]; .desired_state == "running" and .actual_state == "running")' \
    >/dev/null
}

traffic_threshold_reached() {
  api_get flows >"${run_dir}/flows-final.json" &&
    jq -e --argjson minimum "${required_packets}" \
      'length == 6 and all(.[]; .actual_state == "running" and .tx_total >= $minimum and .rx_total >= $minimum)' \
      "${run_dir}/flows-final.json" >/dev/null
}

echo "Building load test binaries..."
go build -o "${work_dir}/bin/pakeloss-controller" ./cmd/pakeloss-controller
go build -o "${work_dir}/bin/pakeloss-agent" ./cmd/pakeloss-agent

echo "Starting controller..."
"${work_dir}/bin/pakeloss-controller" \
  --grpc-addr "${grpc_addr}" \
  --http-addr "${http_addr}" \
  --token "${token}" \
  --flow-interval-ms 1 \
  --flow-packet-size 96 \
  --flow-state stopped \
  >"${run_dir}/controller.log" 2>&1 &
pids+=("$!")

wait_for "controller API" controller_ready

for agent_index in 1 2 3; do
  agent_id="node-${agent_index}"
  udp_port=$((14000 + agent_index))
  echo "Starting ${agent_id}..."
  "${work_dir}/bin/pakeloss-agent" \
    --agent-id "${agent_id}" \
    --controller-addr "${grpc_addr}" \
    --token "${token}" \
    --udp-listen "127.0.0.1:${udp_port}" \
    --udp-advertise "127.0.0.1:${udp_port}" \
    >"${run_dir}/${agent_id}.log" 2>&1 &
  pids+=("$!")
done

wait_for "three online agents" agents_ready
wait_for "six discovered flows" flows_discovered

echo "Starting six flows at 1 ms intervals..."
api_post flows/start >/dev/null
wait_for "six running flows" flows_running

echo "Running load for ${duration_seconds} seconds..."
sleep "${duration_seconds}"

if ! processes_alive; then
  echo "a load test process exited during the load interval" >&2
  exit 1
fi

echo "Checking at least ${required_packets} TX/RX packets per flow..."
if ! traffic_threshold_reached; then
  echo "one or more flows did not reach the traffic threshold" >&2
  exit 1
fi
api_get agents >"${run_dir}/agents-final.json"

jq -r '.[] | "\(.flow_id): tx=\(.tx_total) rx=\(.rx_total) lost=\(.lost_total)"' \
  "${run_dir}/flows-final.json"
echo "Load test passed. Logs: ${run_dir}"
