#!/bin/bash
# =============================================================================
#  bin/config.sh — environment bootstrap for all Odilon shell scripts
#
#  Variable resolution order (first non-empty value wins):
#    1. Already set in the environment  ← systemd sets these via Environment=
#    2. Derived from this script's own location on disk
#    3. FHS defaults based on INSTANCE_NAME
#
#  This means every script works correctly whether invoked:
#    • by systemd  (ODILON_HOME / ODILON_CONF / ODILON_LOGS pre-set)
#    • manually    (all paths auto-derived from the install directory)
#    • in a multi-instance setup — each bin/ self-orients to its own instance
# =============================================================================

# ── locate home directory from the script's own path ─────────────────────────
# Works whether called directly or sourced. readlink -f resolves symlinks.
if [[ -z "${ODILON_HOME:-}" ]]; then
    export ODILON_HOME
    ODILON_HOME="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"
fi

# ── derive instance name from the install directory basename ──────────────────
#   /opt/odilon        → odilon
#   /opt/odilon-prod   → odilon-prod
#   /opt/odilon-dev    → odilon-dev
export INSTANCE_NAME="${INSTANCE_NAME:-$(basename "${ODILON_HOME}")}"

# ── configuration and log directories ────────────────────────────────────────
export ODILON_CONF="${ODILON_CONF:-/etc/${INSTANCE_NAME}}"
export ODILON_LOGS="${ODILON_LOGS:-/var/log/${INSTANCE_NAME}}"

# ── java ──────────────────────────────────────────────────────────────────────
if [[ -z "${JAVA_HOME:-}" ]]; then
    export JAVA_HOME
    JAVA_HOME="$(readlink -f "$(which java)")"
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
-Xbootclasspath/a:${ODILON_HOME}/resources:${ODILON_CONF}
-Xms1G
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=16m
-Dwork=${ODILON_HOME}/tmp/
-Dlog-path=${ODILON_LOGS}
-Dlog4j.configurationFile=log4j2.xml
-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
-Djava.net.preferIPv4Stack=true
-Dfile.encoding=UTF-8
-DOID=${OID}
-Dsun.jnu.encoding=UTF-8"

export DEBUG_PROP=""