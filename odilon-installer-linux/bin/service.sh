#!/bin/bash
# =============================================================================
#  bin/service.sh — start, stop, restart and status for an Odilon instance
#
#  Works on Linux (systemd) and macOS (launchd).
#  All paths are derived automatically from the script's own location —
#  no arguments needed for the default instance.
#
#  Usage:
#    sudo ./bin/service.sh start
#    sudo ./bin/service.sh stop
#    sudo ./bin/service.sh restart
#         ./bin/service.sh status
#
#  The script self-orients: running it from /opt/odilon-prod/bin/ manages
#  the odilon-prod instance; /opt/odilon-dev/bin/ manages odilon-dev.
# =============================================================================

set -euo pipefail

# ── bootstrap ─────────────────────────────────────────────────────────────────
export ODILON_HOME=$(cd "$(dirname "$0")/.." && pwd -P)
source "$ODILON_HOME/bin/config.sh"

# ── colours ───────────────────────────────────────────────────────────────────
C_GREEN='\033[1;32m'; C_YELLOW='\033[1;33m'; C_RED='\033[1;31m'
C_BLUE='\033[1;34m';  C_RESET='\033[0m'

ok()   { printf "${C_GREEN}[ OK ]${C_RESET}  %s\n" "$*"; }
info() { printf "${C_BLUE}[INFO]${C_RESET}  %s\n"  "$*"; }
warn() { printf "${C_YELLOW}[WARN]${C_RESET}  %s\n" "$*"; }
die()  { printf "${C_RED}[FAIL]${C_RESET}  %s\n"   "$*" >&2; exit 1; }
hr()   { printf '%.0s─' {1..64}; printf '\n'; }

# ── OS detection ──────────────────────────────────────────────────────────────
OS_KERNEL="$(uname -s)"
case "$OS_KERNEL" in
    Linux)  OS_TYPE="linux"  ;;
    Darwin) OS_TYPE="macos"  ;;
    *)      OS_TYPE="unknown" ;;
esac

# ── service identifiers ───────────────────────────────────────────────────────
SERVICE_NAME="${INSTANCE_NAME}"

# macOS: read port from odilon.properties to build info URL
PLIST="/Library/LaunchDaemons/io.odilon.${SERVICE_NAME}.plist"
LAUNCHD_LABEL="io.odilon.${SERVICE_NAME}"

# Read port from config (default 9234)
PORT=9234
if [[ -f "${ODILON_CONF}/odilon.properties" ]]; then
    _p="$(grep -E '^server\.port=' "${ODILON_CONF}/odilon.properties" 2>/dev/null | cut -d= -f2- | tr -d ' \r')"
    if [[ -n "$_p" ]]; then PORT="$_p"; fi
fi

# ── helpers ───────────────────────────────────────────────────────────────────
is_active() {
    if [[ "$OS_TYPE" == "linux" ]]; then
        [[ "$(systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true)" == "active" ]]
    elif [[ "$OS_TYPE" == "macos" ]]; then
        launchctl print "system/${LAUNCHD_LABEL}" >/dev/null 2>&1
    else
        return 1
    fi
}

do_start() {
    if is_active; then
        warn "Service '${SERVICE_NAME}' is already running."
        return 0
    fi

    if [[ "$OS_TYPE" == "linux" ]]; then
        [[ "$(id -u)" -eq 0 ]] || die "start requires root — use: sudo $0 start"
        systemctl start "${SERVICE_NAME}"

    elif [[ "$OS_TYPE" == "macos" ]]; then
        [[ "$(id -u)" -eq 0 ]] || die "start requires root — use: sudo $0 start"
        [[ -f "$PLIST" ]] || die "Plist not found: ${PLIST}\nRe-run install-odilon.sh to register the service."
        launchctl bootstrap system "${PLIST}" 2>/dev/null || \
            launchctl load "${PLIST}" 2>/dev/null || true
        launchctl enable "system/${LAUNCHD_LABEL}" 2>/dev/null || true
    else
        die "Unsupported OS '${OS_KERNEL}'. Start manually with: ${ODILON_HOME}/bin/start.sh"
    fi

    ok "Start command sent to '${SERVICE_NAME}'."

    # Wait up to 15 s for the server to come up
    printf "${C_BLUE}[INFO]${C_RESET}  Waiting for server to start"
    for _ in $(seq 1 15); do
        if is_active; then break; fi
        printf "."
        sleep 1
    done
    printf "\n"
}

do_stop() {
    if ! is_active; then
        warn "Service '${SERVICE_NAME}' is not running."
        return 0
    fi

    [[ "$(id -u)" -eq 0 ]] || die "stop requires root — use: sudo $0 stop"

    if [[ "$OS_TYPE" == "linux" ]]; then
        systemctl stop "${SERVICE_NAME}"
    elif [[ "$OS_TYPE" == "macos" ]]; then
        launchctl bootout "system/${LAUNCHD_LABEL}" 2>/dev/null || true
    fi

    ok "Stop command sent to '${SERVICE_NAME}'."
}

do_status() {
    hr
    printf "  Odilon — %s\n" "${SERVICE_NAME}"
    hr
    info "ODILON_HOME : ${ODILON_HOME}"
    info "ODILON_CONF : ${ODILON_CONF}"
    info "ODILON_LOGS : ${ODILON_LOGS}"
    info "Port        : ${PORT}"
    echo

    # ── service state ─────────────────────────────────────────────────────────
    if is_active; then
        ok "Service state   : running"
    else
        warn "Service state   : not running"
    fi

    # ── OS-specific service detail ────────────────────────────────────────────
    echo
    if [[ "$OS_TYPE" == "linux" ]]; then
        systemctl status "${SERVICE_NAME}" --no-pager -l 2>/dev/null || true
    elif [[ "$OS_TYPE" == "macos" ]]; then
        launchctl print "system/${LAUNCHD_LABEL}" 2>/dev/null \
            | grep -E "state|exit code|runs|path|pid" || true
    fi

    # ── API ping ──────────────────────────────────────────────────────────────
    echo
    if is_active; then
        info "Trying API ping → http://localhost:${PORT}/info ..."
        echo
        # Read credentials from odilon.properties (default odilon:odilon)
        ACCESS_KEY="odilon"; SECRET_KEY="odilon"
        if [[ -f "${ODILON_CONF}/odilon.properties" ]]; then
            _ak="$(grep -E '^accessKey=' "${ODILON_CONF}/odilon.properties" 2>/dev/null | cut -d= -f2- | tr -d ' \r')"
            _sk="$(grep -E '^secretKey=' "${ODILON_CONF}/odilon.properties" 2>/dev/null | cut -d= -f2- | tr -d ' \r')"
            if [[ -n "$_ak" ]]; then ACCESS_KEY="$_ak"; fi
            if [[ -n "$_sk" ]]; then SECRET_KEY="$_sk"; fi
        fi
        _ping_ok=false
        for _i in $(seq 1 10); do
            if curl --silent --max-time 3 -u "${ACCESS_KEY}:${SECRET_KEY}" \
                    "http://localhost:${PORT}/info" 2>/dev/null; then
                echo
                _ping_ok=true
                break
            fi
            info "API not ready yet, retrying in 3s (${_i}/10)..."
            sleep 3
        done
        $_ping_ok || warn "API did not respond on port ${PORT} after 30s — check logs in ${ODILON_LOGS}"
    fi
    hr
}

# ── dispatch ──────────────────────────────────────────────────────────────────
CMD="${1:-status}"

case "$CMD" in
    start)
        do_start
        echo
        do_status
        ;;
    stop)
        do_stop
        echo
        if [[ "$OS_TYPE" == "linux" ]]; then
            info "Check: systemctl status ${SERVICE_NAME}"
        else
            info "Check: launchctl print system/${LAUNCHD_LABEL}"
        fi
        ;;
    restart)
        do_stop
        sleep 2
        do_start
        echo
        do_status
        ;;
    status)
        do_status
        ;;
    *)
        echo "Usage: $(basename "$0") {start|stop|restart|status}" >&2
        exit 1
        ;;
esac
