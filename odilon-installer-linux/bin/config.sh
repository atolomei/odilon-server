#!/bin/bash
# =============================================================================
#  bin/config.sh — environment bootstrap for all Odilon shell scripts
#
#  Variable resolution order (first non-empty value wins):
#    1. Already set in the environment  ← systemd / launchd sets these
#    2. Derived from this script's own location on disk
#    3. FHS defaults based on INSTANCE_NAME
#
#  Compatible with Linux (systemd) and macOS (launchd).
# =============================================================================

# ── portable resolve_path (readlink -f is GNU-only; not on macOS stock bash) ─
_resolve_path() {
    if command -v realpath &>/dev/null; then
        realpath "$1"
    elif command -v greadlink &>/dev/null; then
        greadlink -f "$1"
    else
        local t="$1"
        local d
        d="$(cd "$(dirname "$t")" 2>/dev/null && pwd -P)" || d="$(dirname "$t")"
        echo "${d}/$(basename "$t")"
    fi
}

# ── locate home directory from the script's own path ─────────────────────────
if [[ -z "${ODILON_HOME:-}" ]]; then
    export ODILON_HOME
    ODILON_HOME="$(cd "$(dirname "$(_resolve_path "${BASH_SOURCE[0]}")")/.." && pwd)"
fi

# ── derive instance name ──────────────────────────────────────────────────────
#   Resolution order:
#   1. INSTANCE_NAME already in environment  ← set by systemd/launchd service unit
#   2. /etc/odilon*/.odilon-instance scan    ← written by install-odilon.sh;
#      matches the entry whose INSTALL_DIR resolves to ODILON_HOME
#   3. basename of ODILON_HOME               ← last resort for custom layouts
if [[ -z "${INSTANCE_NAME:-}" ]]; then
    _found_meta=""
    for _meta in /etc/odilon*/.odilon-instance; do
        [[ -f "$_meta" ]] || continue
        _inst_dir="$(grep -E '^INSTALL_DIR=' "$_meta" 2>/dev/null | cut -d= -f2-)"
        _inst_dir_real="$(cd "$_inst_dir" 2>/dev/null && pwd -P)" || continue
        if [[ "$_inst_dir_real" == "$ODILON_HOME" ]]; then
            _found_meta="$_meta"
            break
        fi
    done

    if [[ -n "$_found_meta" ]]; then
        export INSTANCE_NAME="$(grep -E '^INSTANCE_NAME=' "$_found_meta" | cut -d= -f2-)"
        # Load LOG_DIR from metadata if not already overridden
        if [[ -z "${ODILON_LOGS:-}" ]]; then
            _meta_logs="$(grep -E '^LOG_DIR=' "$_found_meta" | cut -d= -f2-)"
            [[ -n "$_meta_logs" ]] && export ODILON_LOGS="$_meta_logs"
        fi
    else
        export INSTANCE_NAME="$(basename "${ODILON_HOME}")"
    fi
fi

# ── configuration and log directories ────────────────────────────────────────
#   Always /etc/<INSTANCE_NAME>  — written by install-odilon.sh.
#   Never fall back to ODILON_HOME/config (that is the legacy layout that
#   install-odilon.sh migrates away from).
export ODILON_CONF="${ODILON_CONF:-/etc/${INSTANCE_NAME}}"
export ODILON_LOGS="${ODILON_LOGS:-/var/log/${INSTANCE_NAME}}"

# ── java ──────────────────────────────────────────────────────────────────────
if [[ -z "${JAVA_HOME:-}" ]]; then
    export JAVA_HOME
    JAVA_HOME="$(_resolve_path "$(which java)")"
fi

# ── app user ──────────────────────────────────────────────────────────────────
export APP_USER="${APP_USER:-odilon}"
export OID="${OID:-od1}"

# ── locate the server JAR ─────────────────────────────────────────────────────
_jar="$(find "${ODILON_HOME}/app" -maxdepth 1 -type f -name "odilon-server*.jar" 2>/dev/null | head -n1)"
if [[ -z "$_jar" ]]; then
    echo "[ERROR] No odilon-server*.jar found in ${ODILON_HOME}/app/" >&2
    echo "[ERROR] ODILON_HOME=${ODILON_HOME}" >&2
    exit 1
fi
export APP="$_jar"

# ── JVM options ───────────────────────────────────────────────────────────────
export JETTY_STOP_PWD="OdilonShutd0wn"

export ODILON_PROPS="
-Xms1G
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-Dwork=${ODILON_HOME}/tmp/
-Dlog-path=${ODILON_LOGS}
-Dlog4j.configurationFile=${ODILON_CONF}/log4j2.xml
-Dlogging.config=${ODILON_CONF}/log4j2.xml
-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
-Djava.net.preferIPv4Stack=true
-Dfile.encoding=UTF-8
-DOID=${OID}
-Dsun.jnu.encoding=UTF-8
-Dodilon.conf=${ODILON_CONF}"

export DEBUG_PROP=""
export MEM_PROPS=""