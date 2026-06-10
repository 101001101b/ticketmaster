#!/bin/bash
# run_benchmarks.sh
# Orchestrate benchmark runs targeting remote AWS architecture from localhost

set -e

# --------------------------
# Configuration
# --------------------------
MODE=${1:-direct}
TYPE=${2:-unnumbered}
CLIENTS=${3:-1}
WORKERS=${4:-1}

RESULTS_DIR="./results"

# --- CONFIGURACIÓN DE RED (AWS) ---
AWS_INFRA_IP="98.93.244.221"
AWS_WORKER_IP="54.84.11.44"
AWS_WORKER_PRIV_IP="172.31.18.128"

# --- CONFIGURACIÓN SSH PARA AWS ---
SSH_KEY="sd-aws.pem"
SSH_USER="ubuntu"
AWS_PROJECT_DIR="/home/ubuntu/worker"
AWS_INFRA_DIR="/home/ubuntu/infra"

# Validate arguments
if [[ "$MODE" != "direct" && "$MODE" != "indirect" ]]; then
    echo "Error: MODE must be 'direct' or 'indirect'"
    echo "Usage: $0 [direct|indirect] [numbered|unnumbered|contention] [clients_local] [workers_in_aws]"
    exit 1
fi

if [[ "$TYPE" != "numbered" && "$TYPE" != "unnumbered" && "$TYPE" != "contention" ]]; then
    echo "Error: TYPE must be 'numbered', 'unnumbered', or 'contention'"
    echo "Usage: $0 [direct|indirect] [numbered|unnumbered|contention] [clients_local] [workers_in_aws]"
    exit 1
fi

echo "=========================================="
echo "REMOTE BENCHMARK CONFIGURATION"
echo "=========================================="
echo "Target AWS Infra:  $AWS_INFRA_IP"
echo "Mode:              $MODE"
echo "Type:              $TYPE"
echo "Clients (Local):   $CLIENTS"
echo "Workers (AWS):     $WORKERS"
echo "Benchmark:         benchmarks/benchmark_${TYPE}.txt"
echo "=========================================="

# --------------------------
# [1/5] Clean & Scale Workers Remotely via SSH
# --------------------------
echo "[1/5] Cleaning old containers and scaling remote workers to $WORKERS..."

ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_WORKER_IP}" "
    docker stop \$(docker ps -q) 2>/dev/null || true
    docker rm \$(docker ps -aq) 2>/dev/null || true
"

if [[ "$MODE" == "indirect" ]]; then
    ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_WORKER_IP}" \
        "cd $AWS_PROJECT_DIR && docker-compose up -d --scale worker-indirect=$WORKERS"
else
    ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_WORKER_IP}" \
        "cd $AWS_PROJECT_DIR && docker-compose up -d --scale worker-direct=$WORKERS"
fi

# Wait until all workers are running (max 60s)
echo "Waiting for all $WORKERS workers to be ready..."
FILTER="worker-direct"
[[ "$MODE" == "indirect" ]] && FILTER="worker-indirect"

for i in $(seq 1 30); do
    RUNNING=$(ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_WORKER_IP}" \
        "docker ps --filter 'name=$FILTER' --filter 'status=running' -q | wc -l")
    if [[ "$RUNNING" -ge "$WORKERS" ]]; then
        echo "All $WORKERS workers running."
        break
    fi
    echo "  $RUNNING/$WORKERS ready, waiting..."
    sleep 2
    if [[ "$i" -eq 30 ]]; then
        echo "Error: Timeout waiting for workers to start."
        exit 1
    fi
done

# --------------------------
# [2/5] Dynamically Reconfigure NGINX Upstream
# --------------------------
if [[ "$MODE" == "direct" ]]; then
    echo "[2/5] Reading active ports from AWS Worker machine..."

    PORTS=$(ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_WORKER_IP}" \
        "docker ps --filter 'name=worker-direct' --format '{{.Ports}}' \
        | grep -oP '(?<=0\.0\.0\.0:)\d+(?=->)'")

    if [[ -z "$PORTS" ]]; then
        echo "Error: No active worker ports found. Make sure workers started correctly."
        exit 1
    fi

    UPSTREAM_SERVERS=""
    for PORT in $PORTS; do
        echo "  Detected worker port: $PORT"
        UPSTREAM_SERVERS="${UPSTREAM_SERVERS}        server ${AWS_WORKER_PRIV_IP}:${PORT};\n"
    done

    NGINX_CONF=$(cat <<NGINX
worker_processes auto;

events {
    worker_connections 4096;
    use epoll;
    multi_accept on;
}

http {
    upstream backend_servers {
$(printf "$UPSTREAM_SERVERS")
        keepalive 200;
        keepalive_requests 10000;
        keepalive_timeout 65s;
    }

    server {
        listen 80 reuseport;

        location / {
            proxy_pass http://backend_servers;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host \$host;

            proxy_connect_timeout 10s;
            proxy_read_timeout 60s;
            proxy_send_timeout 60s;
        }
    }
}
NGINX
)

    echo "[2/5] Uploading nginx.conf to AWS Infra machine..."
    echo "$NGINX_CONF" | ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_INFRA_IP}" \
        "cat > ${AWS_INFRA_DIR}/nginx.conf"

    echo "[2/5] Restarting NGINX..."
    ssh -i "$SSH_KEY" "${SSH_USER}@${AWS_INFRA_IP}" \
        "cd ${AWS_INFRA_DIR} && docker compose restart nginx || sudo systemctl restart nginx"
else
    echo "[2/5] Indirect mode — skipping NGINX reconfiguration."
fi

# --------------------------
# [3/5] Reset System State
# --------------------------
echo "[3/5] Resetting system state on AWS..."
curl -s -X POST "http://${AWS_INFRA_IP}:80/reset" > /dev/null || {
    echo "Warning: Could not reset remote state. Ensure AWS stack is running and reachable."
}

# --------------------------
# [4/5] Run Benchmark
# --------------------------
echo "[4/5] Running benchmark with $CLIENTS injection threads..."

if [[ "$MODE" == "direct" ]]; then
    docker compose --profile benchmark-direct run --rm --build \
        -e MODE=direct \
        -e API_URL="http://$AWS_INFRA_IP:80" \
        -e TICKET_TYPE="$TYPE" \
        -e BENCHMARK_FILE="/app/benchmarks/benchmark_${TYPE}.txt" \
        -e CLIENTS="$CLIENTS" \
        -e WORKERS="$WORKERS" \
        -e RESULTS_FILE="/app/results/benchmark_${MODE}_${TYPE}.jsonl" \
        producer-direct
else
    docker compose --profile benchmark-indirect run --rm --build \
        -e MODE=indirect \
        -e API_URL="http://$AWS_INFRA_IP" \
        -e REDIS_HOST="$AWS_INFRA_IP" \
        -e RABBITMQ_HOST="$AWS_INFRA_IP" \
        -e TICKET_TYPE="$TYPE" \
        -e BENCHMARK_FILE="/app/benchmarks/benchmark_${TYPE}.txt" \
        -e CLIENTS="$CLIENTS" \
        -e WORKERS="$WORKERS" \
        -e RESULTS_FILE="/app/results/benchmark_${MODE}_${TYPE}.jsonl" \
        producer-indirect
fi

# --------------------------
# Finalizing
# --------------------------
echo "Collecting results..."
sleep 5

echo ""
echo "=========================================="
echo "BENCHMARK COMPLETE"
echo "=========================================="
echo "Mode:              $MODE"
echo "Type:              $TYPE"
echo "Clients (Local):   $CLIENTS"
echo "Workers (AWS):     $WORKERS"
echo ""

if [[ -f "$RESULTS_DIR/benchmark_${MODE}_${TYPE}.jsonl" ]]; then
    echo "Local Results Preview:"
    tail -n 5 "$RESULTS_DIR/benchmark_${MODE}_${TYPE}.jsonl"
else
    echo "Warning: No results file found at $RESULTS_DIR/"
fi

echo ""
echo "Detailed results saved to: $RESULTS_DIR/"
echo "=========================================="