#!/bin/bash
# =============================================================================
#  bin/enable-encryption.sh — initialise encryption for an Odilon instance
#
#  Usage:
#    sudo /opt/odilon/bin/enable-encryption.sh
#
#  The script must be run as root (or a sudo-capable user).
#  It will automatically re-execute itself as APP_USER (default: odilon).
# =============================================================================
set -euo pipefail

export ODILON_HOME=$(cd "$(dirname "$0")/.." && pwd -P)
source "$ODILON_HOME/bin/config.sh"

# ── re-execute as APP_USER if needed ─────────────────────────────────────────
if [[ "$(whoami)" != "$APP_USER" ]]; then
    if sudo -n -u "$APP_USER" true 2>/dev/null; then
        exec sudo -u "$APP_USER" "$0" "$@"
    else
        echo "[ERROR] This script must run as '$APP_USER'."
        echo "        Try: sudo $0"
        exit 1
    fi
fi

# ── pre-flight ────────────────────────────────────────────────────────────────
echo "Odilon Home -> $ODILON_HOME"
echo "Odilon Conf -> $ODILON_CONF"

[[ -f "$ODILON_CONF/odilon.properties" ]] \
    || { echo "[ERROR] $ODILON_CONF/odilon.properties not found."; exit 1; }
[[ -f "$ODILON_CONF/log4j2.xml" ]] \
    || { echo "[ERROR] $ODILON_CONF/log4j2.xml not found."; exit 1; }

# ── check not already running ─────────────────────────────────────────────────
pid=$(ps aux | grep -E ".*[j]ava.*odilon-server" | grep "$OID" | awk '{print $2}' || true)
if [[ -n "$pid" ]]; then
    echo "[ERROR] Odilon is already running on pid $pid. Stop it before enabling encryption."
    exit 1
fi

# ── java ──────────────────────────────────────────────────────────────────────
if [[ -z "${JAVA_HOME:-}" ]]; then
    JAVA_CMD="java"
    echo "JAVA_HOME not set. Using default java ($(command -v java))"
else
    JAVA_CMD="$JAVA_HOME"
    "$JAVA_HOME" --version
    echo "Using java from JAVA_HOME ($JAVA_HOME)"
fi

echo "Changing current directory to $ODILON_HOME"
cd "$ODILON_HOME"

# shellcheck disable=SC2086
"$JAVA_CMD" $DEBUG_PROP $ODILON_PROPS \
    -cp "$ODILON_CONF:$APP" \
    org.springframework.boot.loader.launch.JarLauncher \
    --initializeEncryption=true
