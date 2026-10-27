#!/usr/bin/env bash
# Start, stop or check EMBRACE's LLM: an Ollama server of its own, listening on the local network so the chat-bot
# (on another machine) can use it. It runs on its own port, so an Ollama you already use (the Mac app, or a system
# service on 11434) is left alone - and it runs as you, so no sudo is needed.
#
# usage: scripts/llm/llm.sh start|stop|status [SSH_HOST]
#   scripts/llm/llm.sh start              # on this machine
#   scripts/llm/llm.sh start gpu-box      # on another machine over ssh (this script is sent along; no repo needed)
#
# env: EMBRACE_LLM_PORT (default 11435), EMBRACE_LLM_MODEL (default qwen3:8b), EMBRACE_EMBED_MODEL (default
#      qwen3-embedding:0.6b, for search by meaning) - both pulled on first start
# see docs/modules/chat.md
set -euo pipefail

ACTION=${1:-status}
REMOTE=${2:-}
if [ -n "$REMOTE" ]; then
    exec ssh "$REMOTE" "EMBRACE_LLM_PORT=${EMBRACE_LLM_PORT:-} EMBRACE_LLM_MODEL=${EMBRACE_LLM_MODEL:-} EMBRACE_EMBED_MODEL=${EMBRACE_EMBED_MODEL:-} bash -s -- $ACTION" < "$0"
fi

PORT=${EMBRACE_LLM_PORT:-11435}
MODEL=${EMBRACE_LLM_MODEL:-qwen3:8b}
EMBED_MODEL=${EMBRACE_EMBED_MODEL:-qwen3-embedding:0.6b}
STATE_DIR="$HOME/.cache/embrace-llm"
PID_FILE="$STATE_DIR/ollama.pid"
LOG_FILE="$STATE_DIR/ollama.log"
LOCAL="http://127.0.0.1:$PORT"

find_ollama() {
    for c in "$(command -v ollama 2>/dev/null || true)" /usr/local/bin/ollama /opt/homebrew/bin/ollama /snap/bin/ollama \
             /Applications/Ollama.app/Contents/Resources/ollama; do
        if [ -n "$c" ] && [ -x "$c" ]; then echo "$c"; return; fi
    done
    echo "ollama not found - install it from https://ollama.com" >&2
    exit 1
}

lan_ip() {
    if [ "$(uname)" = Darwin ]; then
        ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo localhost
    else
        hostname -I 2>/dev/null | awk '{print $1}'
    fi
}

up() { curl -fsS -m 2 "$LOCAL/api/version" >/dev/null 2>&1; }

our_pid() { [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null && cat "$PID_FILE"; }

status() {
    if ! up; then
        echo "EMBRACE LLM: not running on $(hostname -s)"
        return
    fi
    loaded=$(curl -fsS -m 2 "$LOCAL/api/ps" | grep -o '"name":"[^"]*"' | cut -d'"' -f4 | paste -sd, - || true)
    echo "EMBRACE LLM: running on $(hostname -s) at http://$(lan_ip):$PORT - model in memory: ${loaded:-none (loads on the next message)}"
    if command -v nvidia-smi >/dev/null; then
        echo "GPU memory: $(nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader)"
    fi
}

start() {
    if up; then
        status
        return
    fi
    mkdir -p "$STATE_DIR"
    ollama=$(find_ollama)
    # the snap's launcher replaces OLLAMA_HOST with the snap's own setting (127.0.0.1:11434, the system service), so
    # run the binary inside it directly: it finds its GPU libraries next to itself, and keeps models in ~/.ollama
    if [ "$ollama" = /snap/bin/ollama ] && [ -x /snap/ollama/current/bin/ollama ]; then
        ollama=/snap/ollama/current/bin/ollama
    fi
    echo "$(date '+%F %T') starting $ollama on 0.0.0.0:$PORT" >>"$LOG_FILE"
    OLLAMA_HOST="0.0.0.0:$PORT" nohup "$ollama" serve </dev/null >>"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    for _ in $(seq 1 30); do
        up && break
        sleep 1
    done
    if ! up; then
        echo "Ollama didn't start - last lines of $LOG_FILE:" >&2
        tail -5 "$LOG_FILE" >&2
        exit 1
    fi
    for m in "$MODEL" "$EMBED_MODEL"; do
        if ! OLLAMA_HOST="$LOCAL" "$ollama" show "$m" >/dev/null 2>&1; then
            echo "Pulling $m (first start only)..."
            OLLAMA_HOST="$LOCAL" "$ollama" pull "$m"
        fi
    done
    # load the model now, so the first message isn't slow
    curl -fsS -m 300 "$LOCAL/api/generate" -d "{\"model\": \"$MODEL\", \"keep_alive\": \"15m\"}" >/dev/null
    status
}

stop() {
    if pid=$(our_pid); then
        kill "$pid"
        for _ in $(seq 1 15); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        rm -f "$PID_FILE"
        echo "EMBRACE LLM: stopped on $(hostname -s)"
    elif up; then
        echo "Something else is serving port $PORT (not started by this script) - left alone" >&2
        exit 1
    else
        echo "EMBRACE LLM: not running on $(hostname -s)"
    fi
}

case "$ACTION" in
    start) start ;;
    stop) stop ;;
    status) status ;;
    *) echo "usage: $0 start|stop|status [SSH_HOST]" >&2; exit 2 ;;
esac
