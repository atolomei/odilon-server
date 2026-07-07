#!/bin/bash
# =============================================================================
#  install-odilon.sh — Odilon Object Storage Server installer / upgrader
#
#  Filesystem layout (defaults, all overridable via flags)
#  -------------------------------------------------------
#    /opt/<name>                  Binaries, scripts, resources   (ODILON_HOME)
#    /etc/<name>                  Configuration, keystore        (ODILON_CONF)
#    /var/log/<name>              Log files                       (ODILON_LOGS)
#    /opt/<name>-data             Default data directory
#    /opt/odilon-backups/<name>   Timestamped rollback snapshots
#
#  Usage
#  -----
#    Extract the distribution tarball, cd into the resulting directory, then:
#
#      sudo ./install-odilon.sh                       # defaults: name=odilon
#      sudo ./install-odilon.sh --dry-run             # preview without changes
#      sudo ./install-odilon.sh --name odilon-dev --port 9235
#
#  Multiple instances on the same host
#  ------------------------------------
#      sudo ./install-odilon.sh --name odilon-prod --port 9234
#      sudo ./install-odilon.sh --name odilon-dev  --port 9235
#
#    Each instance gets its own:
#      /opt/<name>   /etc/<name>   /var/log/<name>   systemctl start <name>
#
#  Mode detection (automatic)
#  --------------------------
#    /etc/<name> exists                  → upgrade  (binaries replaced, config untouched)
#    /opt/<name>/config/odilon.properties exists → migrate  (legacy layout → FHS layout)
#    neither                             → fresh install
#
#  Upgrade (auto-detected when <conf-dir> already exists)
#  -------------------------------------------------------
#    1. Stops the running service (up to 30 s).
#    2. Snapshots app/ + bin/ + resources/ → /opt/odilon-backups/<name>/<ts>/
#       Keeps the 3 most recent; oldest pruned automatically.
#    3. Replaces ONLY app/, bin/, resources/.  <conf-dir> is NEVER touched.
#    4. Refreshes the systemd unit and restarts.
#
#  Migrate — legacy layout (config inside /opt/<name>/config/)
#  ------------------------------------------------------------
#    1. Stops the running service (up to 30 s).
#    2. Full snapshot of /opt/<name>/ including config/ → /opt/odilon-backups/<name>/<ts>/
#    3. Copies config files to /etc/<name>/  (nothing deleted yet).
#    4. Renames old config/ → config.migrated.<ts>/  (kept as safety net).
#    5. Replaces binaries, generates new systemd unit, restarts.
#
#  Rollback
#  --------
#    sudo systemctl stop <name>
#    sudo cp -a /opt/odilon-backups/<name>/<timestamp>/. /opt/<name>/
#    sudo systemctl start <name>
# =============================================================================

set -euo pipefail

# ── colours ───────────────────────────────────────────────────────────────────
C_BLUE='\033[1;34m'; C_GREEN='\033[1;32m'; C_YELLOW='\033[1;33m'
C_RED='\033[1;31m';  C_GREY='\033[0;90m';  C_RESET='\033[0m'

info()  { printf "\n${C_BLUE}[INFO]${C_RESET}  %s\n"   "$*"; }
ok()    { printf   "${C_GREEN}[ OK ]${C_RESET}  %s\n"   "$*"; }
warn()  { printf   "${C_YELLOW}[WARN]${C_RESET}  %s\n"  "$*"; }
die()   { printf   "${C_RED}[FAIL]${C_RESET}  %s\n"     "$*" >&2; exit 1; }
hr()    { printf '%.0s─' {1..64}; printf '\n'; }

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

# ── option defaults ───────────────────────────────────────────────────────────
INSTANCE_NAME="odilon"
OPT_PREFIX=""
OPT_CONF=""
OPT_LOGS=""
OPT_USER=""
OPT_PORT=""
DRY_RUN=false
MAX_ROLLBACK_SNAPSHOTS=3

# ── usage ─────────────────────────────────────────────────────────────────────
usage() {
    cat <<USAGE

Usage: sudo $(basename "$0") [OPTIONS]

  --name   NAME   Instance / service name       [default: odilon]
  --prefix DIR    Binary installation root      [default: /opt/<name>]
  --conf   DIR    Configuration directory       [default: /etc/<name>]
  --logs   DIR    Log directory                 [default: /var/log/<name>]
  --user   USER   System user to run as         [default: odilon]
  --port   PORT   HTTP listener port            [default: 9234]
  --dry-run       Preview actions, no changes
  -h, --help      Show this help and exit

USAGE
}

# ── parse flags ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --name)    INSTANCE_NAME="$2"; shift 2 ;;
        --prefix)  OPT_PREFIX="$2";    shift 2 ;;
        --conf)    OPT_CONF="$2";      shift 2 ;;
        --logs)    OPT_LOGS="$2";      shift 2 ;;
        --user)    OPT_USER="$2";      shift 2 ;;
        --port)    OPT_PORT="$2";      shift 2 ;;
        --dry-run) DRY_RUN=true;       shift   ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1  (use --help)" ;;
    esac
done

# ── dry-run wrapper ───────────────────────────────────────────────────────────
run() {
    if $DRY_RUN; then
        printf "  ${C_GREY}[dry-run]${C_RESET}  %s\n" "$*"
    else
        "$@"
    fi
}

# ── path resolution (called twice: before and after load_meta) ────────────────
apply_paths() {
    INSTALL_DIR="${OPT_PREFIX:-/opt/${INSTANCE_NAME}}"
    CONF_DIR="${OPT_CONF:-/etc/${INSTANCE_NAME}}"
    LOG_DIR="${OPT_LOGS:-/var/log/${INSTANCE_NAME}}"
    ODILON_USER="${OPT_USER:-odilon}"
    ODILON_GROUP="${ODILON_USER}"
    PORT="${OPT_PORT:-9234}"
    DATA_DIR="${INSTALL_DIR}-data"
    BACKUP_ROOT="/opt/odilon-backups/${INSTANCE_NAME}"
    SERVICE_NAME="${INSTANCE_NAME}"
    SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
}

# ── load saved instance metadata ─────────────────────────────────────────────
# Allows "sudo install-odilon.sh --name odilon-dev" to upgrade without
# repeating every path flag used at install time. CLI flags always win.
load_meta() {
    local meta="${CONF_DIR}/.odilon-instance"
    [[ -f "$meta" ]] || return 0
    info "Loading saved instance metadata → ${meta}"
    local _prefix _conf _logs _user _port
    _prefix="$(grep -E '^INSTALL_DIR=' "$meta" | cut -d= -f2-)" || true
    _conf="$(  grep -E '^CONF_DIR='    "$meta" | cut -d= -f2-)" || true
    _logs="$(  grep -E '^LOG_DIR='     "$meta" | cut -d= -f2-)" || true
    _user="$(  grep -E '^ODILON_USER=' "$meta" | cut -d= -f2-)" || true
    _port="$(  grep -E '^PORT='        "$meta" | cut -d= -f2-)" || true
    # only fill blanks — CLI flags already set OPT_* variables
    [[ -z "$OPT_PREFIX" && -n "$_prefix" ]] && OPT_PREFIX="$_prefix"
    [[ -z "$OPT_CONF"   && -n "$_conf"   ]] && OPT_CONF="$_conf"
    [[ -z "$OPT_LOGS"   && -n "$_logs"   ]] && OPT_LOGS="$_logs"
    [[ -z "$OPT_USER"   && -n "$_user"   ]] && OPT_USER="$_user"
    [[ -z "$OPT_PORT"   && -n "$_port"   ]] && OPT_PORT="$_port"
}

# First pass (need CONF_DIR to locate metadata)
apply_paths
[[ -d "$CONF_DIR" ]] && load_meta
# Second pass (CLI flags win over saved values)
apply_paths

# ── pre-flight ────────────────────────────────────────────────────────────────
[[ "$(id -u)" -eq 0 ]] || die "Must be run as root.  Try: sudo $0"

if ! command -v java &>/dev/null; then
    die "Java not found. Install Java 17 or newer before running this installer."
fi
JAVA_BIN="$(readlink -f "$(which java)")"
JAVA_VER="$("$JAVA_BIN" -version 2>&1 | awk -F'"' '/version/{print $2}' | cut -d. -f1)"
if [[ "${JAVA_VER:-0}" -lt 17 ]]; then
    warn "Java 17+ is required. Detected: $("$JAVA_BIN" -version 2>&1 | head -1)"
    warn "Installation will proceed, but Odilon may not start correctly."
else
    ok "Java ${JAVA_VER} found at ${JAVA_BIN}"
fi

JAR="$(find "$SCRIPT_DIR/app" -maxdepth 1 -type f -name "odilon-server*.jar" 2>/dev/null | head -n1)"
[[ -n "$JAR" ]] || die "odilon-server*.jar not found in $SCRIPT_DIR/app — is the tarball complete?"
JAR_NAME="$(basename "$JAR")"
VERSION="${JAR_NAME#odilon-server-}"; VERSION="${VERSION%.jar}"

# ── detect mode ───────────────────────────────────────────────────────────────
LEGACY_CONF_DIR="${INSTALL_DIR}/config"

if [[ -d "$CONF_DIR" ]]; then
    MODE="upgrade"
elif [[ -f "${LEGACY_CONF_DIR}/odilon.properties" ]]; then
    MODE="migrate"
else
    MODE="install"
fi

# ── print plan ────────────────────────────────────────────────────────────────
hr
printf "  Odilon Object Storage Server — installer  (v%s)\n" "$VERSION"
hr
info "Mode              : ${MODE}"
info "Instance / service: ${INSTANCE_NAME}"
info "Installer package : ${SCRIPT_DIR}"
info "Binaries          : ${INSTALL_DIR}"
if [[ "$MODE" == "upgrade" ]]; then
    info "Configuration     : ${CONF_DIR}  (preserved — not modified)"
elif [[ "$MODE" == "migrate" ]]; then
    info "Configuration     : ${LEGACY_CONF_DIR}  → ${CONF_DIR}  (migrating)"
else
    info "Configuration     : ${CONF_DIR}"
    info "Data directory    : ${DATA_DIR}  (default — update odilon.properties)"
fi
info "Logs              : ${LOG_DIR}"
info "Port              : ${PORT}"
info "Rollback snapshots: ${BACKUP_ROOT}"
$DRY_RUN && warn "DRY RUN — no changes will be made."
echo

# =============================================================================
#  SHARED HELPERS
# =============================================================================

# Generate a per-instance systemd unit (inline — no static .service file needed)
generate_service_unit() {
    info "Writing ${SERVICE_FILE} ..."
    if $DRY_RUN; then
        printf "  ${C_GREY}[dry-run]${C_RESET}  would write %s\n" "$SERVICE_FILE"
        return
    fi
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Odilon Object Storage Server (${INSTANCE_NAME})
Documentation=https://odilon.io
Wants=network-online.target
After=network-online.target

[Service]
User=${ODILON_USER}
Group=${ODILON_GROUP}
WorkingDirectory=${INSTALL_DIR}

# All three variables are read by every script in bin/
Environment="ODILON_HOME=${INSTALL_DIR}"
Environment="ODILON_CONF=${CONF_DIR}"
Environment="ODILON_LOGS=${LOG_DIR}"
Environment="INSTANCE_NAME=${INSTANCE_NAME}"

ExecStart=${INSTALL_DIR}/bin/start-service.sh

Restart=on-success
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${INSTANCE_NAME}

LimitNOFILE=65536
TimeoutStopSec=60
KillSignal=SIGTERM
SendSIGKILL=no
SuccessExitStatus=0

[Install]
WantedBy=multi-user.target
EOF
    ok "Service unit written."
}

# Persist installation parameters for future upgrades
save_meta() {
    if $DRY_RUN; then
        printf "  ${C_GREY}[dry-run]${C_RESET}  would write %s/.odilon-instance\n" "$CONF_DIR"
        return
    fi
    cat > "${CONF_DIR}/.odilon-instance" <<EOF
# Written by install-odilon.sh on $(date -u '+%Y-%m-%dT%H:%M:%SZ')
# Re-run the installer with --name ${INSTANCE_NAME} to upgrade.
INSTANCE_NAME=${INSTANCE_NAME}
INSTALL_DIR=${INSTALL_DIR}
CONF_DIR=${CONF_DIR}
LOG_DIR=${LOG_DIR}
ODILON_USER=${ODILON_USER}
PORT=${PORT}
EOF
    chmod 600 "${CONF_DIR}/.odilon-instance"
    chown "${ODILON_USER}:${ODILON_GROUP}" "${CONF_DIR}/.odilon-instance"
    ok "Instance metadata saved → ${CONF_DIR}/.odilon-instance"
}

# =============================================================================
#  UPGRADE
# =============================================================================
if [[ "$MODE" == "upgrade" ]]; then

    info "Existing installation detected — starting upgrade."

    # ── 1. Stop service ───────────────────────────────────────────────────────
    if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
        info "Stopping ${SERVICE_NAME} ..."
        run systemctl stop "${SERVICE_NAME}"
        if ! $DRY_RUN; then
            for i in $(seq 1 30); do
                systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null || break
                sleep 1
            done
            systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null && \
                die "${SERVICE_NAME} did not stop within 30 s — aborting."
        fi
        ok "Service stopped."
    else
        warn "${SERVICE_NAME} was not running."
    fi

    # ── 2. Snapshot (rollback safety) ─────────────────────────────────────────
    ROLLBACK_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
    run mkdir -p "${ROLLBACK_DIR}"
    for sub in app bin resources; do
        [[ -d "${INSTALL_DIR}/${sub}" ]] && run cp -a "${INSTALL_DIR}/${sub}" "${ROLLBACK_DIR}/"
    done
    ok "Snapshot saved → ${ROLLBACK_DIR}"

    # Prune oldest snapshots, keep N most recent
    if ! $DRY_RUN; then
        mapfile -t SNAPS < <(find "${BACKUP_ROOT}" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort)
        EXCESS=$(( ${#SNAPS[@]} - MAX_ROLLBACK_SNAPSHOTS ))
        if (( EXCESS > 0 )); then
            for snap in "${SNAPS[@]:0:$EXCESS}"; do
                rm -rf "$snap"
                info "Pruned old snapshot: $snap"
            done
            ok "Snapshots pruned (keeping last ${MAX_ROLLBACK_SNAPSHOTS})."
        fi
    fi

    # ── 3. Replace binaries — never touch CONF_DIR ───────────────────────────
    info "Replacing binaries (${CONF_DIR} untouched) ..."

    run mkdir -p "${INSTALL_DIR}/app"
    run rm -f "${INSTALL_DIR}"/app/odilon-server*.jar
    run cp "$JAR" "${INSTALL_DIR}/app/"
    run chown "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/app/${JAR_NAME}"
    ok "Server JAR → ${INSTALL_DIR}/app/${JAR_NAME}"

    run cp -r "${SCRIPT_DIR}/bin/". "${INSTALL_DIR}/bin/"
    run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/bin/"
    run chmod 750 "${INSTALL_DIR}"/bin/*.sh
    ok "Scripts → ${INSTALL_DIR}/bin/"

    if [[ -d "${SCRIPT_DIR}/resources" ]]; then
        run rm -rf "${INSTALL_DIR}/resources"
        run cp -r  "${SCRIPT_DIR}/resources" "${INSTALL_DIR}/"
        run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/resources"
        ok "Resources → ${INSTALL_DIR}/resources/"
    fi

    # ── 4. Refresh systemd unit and restart ───────────────────────────────────
    generate_service_unit
    run systemctl daemon-reload
    info "Starting ${SERVICE_NAME} ..."
    run systemctl start "${SERVICE_NAME}"
    if ! $DRY_RUN; then
        sleep 3
        systemctl status "${SERVICE_NAME}" --no-pager -l || true
    fi

    # ── summary ───────────────────────────────────────────────────────────────
    echo
    hr
    ok "Upgrade to v${VERSION} complete  [${INSTANCE_NAME}]"
    info "Configuration unchanged : ${CONF_DIR}"
    info "Rollback if needed:"
    echo
    echo "    sudo systemctl stop ${SERVICE_NAME}"
    echo "    sudo cp -a ${ROLLBACK_DIR}/. ${INSTALL_DIR}/"
    echo "    sudo systemctl start ${SERVICE_NAME}"
    echo
    hr
    exit 0
fi

# =============================================================================
#  MIGRATE — legacy install (config in $INSTALL_DIR/config/)
#  Moves config to /etc/<name>, replaces binaries, regenerates service unit.
# =============================================================================
if [[ "$MODE" == "migrate" ]]; then

    warn "Legacy installation detected — config found in ${LEGACY_CONF_DIR}"
    warn "Migrating configuration → ${CONF_DIR}"
    echo
    info "What will happen:"
    echo "  1. Service stopped (if running)"
    echo "  2. Full snapshot saved  → ${BACKUP_ROOT}/${TIMESTAMP}  (config included)"
    echo "  3. Config copied        → ${CONF_DIR}  (originals kept as config.migrated.${TIMESTAMP}/)"
    echo "  4. Binaries replaced       (app/, bin/, resources/)"
    echo "  5. New systemd unit written with ODILON_CONF=${CONF_DIR}"
    echo "  6. Service restarted"
    echo

    # ── 1. Stop service ───────────────────────────────────────────────────────
    if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
        info "Stopping ${SERVICE_NAME} ..."
        run systemctl stop "${SERVICE_NAME}"
        if ! $DRY_RUN; then
            for i in $(seq 1 30); do
                systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null || break
                sleep 1
            done
            systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null && \
                die "${SERVICE_NAME} did not stop within 30 s — aborting."
        fi
        ok "Service stopped."
    else
        warn "${SERVICE_NAME} was not running."
    fi

    # ── 2. Full snapshot (config included — more complete than upgrade) ───────
    ROLLBACK_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
    run mkdir -p "${ROLLBACK_DIR}"
    for sub in app bin resources config; do
        [[ -d "${INSTALL_DIR}/${sub}" ]] && run cp -a "${INSTALL_DIR}/${sub}" "${ROLLBACK_DIR}/"
    done
    ok "Full snapshot saved → ${ROLLBACK_DIR}"

    # Prune oldest snapshots, keep N most recent
    if ! $DRY_RUN; then
        mapfile -t SNAPS < <(find "${BACKUP_ROOT}" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort)
        EXCESS=$(( ${#SNAPS[@]} - MAX_ROLLBACK_SNAPSHOTS ))
        if (( EXCESS > 0 )); then
            for snap in "${SNAPS[@]:0:$EXCESS}"; do
                rm -rf "$snap"
                info "Pruned old snapshot: $snap"
            done
            ok "Snapshots pruned (keeping last ${MAX_ROLLBACK_SNAPSHOTS})."
        fi
    fi

    # ── 3. Migrate config — copy first, rename old dir (never delete) ─────────
    info "Migrating config files → ${CONF_DIR} ..."
    run mkdir -p "${CONF_DIR}"
    if ! $DRY_RUN; then
        cp -a "${LEGACY_CONF_DIR}/." "${CONF_DIR}/"
        # Rename the old config dir so bin/config.sh no longer picks it up,
        # but it is still present on disk as a safety net.
        mv "${LEGACY_CONF_DIR}" "${INSTALL_DIR}/config.migrated.${TIMESTAMP}"
    else
        printf "  ${C_GREY}[dry-run]${C_RESET}  would copy  %s → %s\n"   "$LEGACY_CONF_DIR" "$CONF_DIR"
        printf "  ${C_GREY}[dry-run]${C_RESET}  would rename %s → %s\n"  \
            "$LEGACY_CONF_DIR" "${INSTALL_DIR}/config.migrated.${TIMESTAMP}"
    fi
    run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${CONF_DIR}"
    run chmod 750 "${CONF_DIR}"
    if ! $DRY_RUN; then
        find "${CONF_DIR}" -type f -exec chmod 640 {} \;
    fi
    ok "Config migrated → ${CONF_DIR}"

    # ── 4. Replace binaries ───────────────────────────────────────────────────
    info "Replacing binaries ..."

    run mkdir -p "${INSTALL_DIR}/app"
    run rm -f "${INSTALL_DIR}"/app/odilon-server*.jar
    run cp "$JAR" "${INSTALL_DIR}/app/"
    run chown "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/app/${JAR_NAME}"
    ok "Server JAR → ${INSTALL_DIR}/app/${JAR_NAME}"

    run cp -r "${SCRIPT_DIR}/bin/." "${INSTALL_DIR}/bin/"
    run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/bin/"
    run chmod 750 "${INSTALL_DIR}"/bin/*.sh
    ok "Scripts → ${INSTALL_DIR}/bin/"

    if [[ -d "${SCRIPT_DIR}/resources" ]]; then
        run rm -rf "${INSTALL_DIR}/resources"
        run cp -r  "${SCRIPT_DIR}/resources" "${INSTALL_DIR}/"
        run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}/resources"
        ok "Resources → ${INSTALL_DIR}/resources/"
    fi

    # ── 5. New service unit, metadata, restart ────────────────────────────────
    generate_service_unit
    save_meta
    run systemctl daemon-reload
    info "Starting ${SERVICE_NAME} ..."
    run systemctl start "${SERVICE_NAME}"
    if ! $DRY_RUN; then
        sleep 3
        systemctl status "${SERVICE_NAME}" --no-pager -l || true
    fi

    # ── summary ───────────────────────────────────────────────────────────────
    echo
    hr
    ok "Migration to v${VERSION} complete  [${INSTANCE_NAME}]"
    info "Configuration moved  : ${CONF_DIR}  (canonical location going forward)"
    info "Old config kept at   : ${INSTALL_DIR}/config.migrated.${TIMESTAMP}"
    info "Full snapshot        : ${ROLLBACK_DIR}"
    echo
    warn "Verify your settings in ${CONF_DIR}/odilon.properties"
    warn "Once satisfied, the config.migrated.* directory can be removed:"
    echo "    sudo rm -rf ${INSTALL_DIR}/config.migrated.${TIMESTAMP}"
    echo
    info "Rollback if needed:"
    echo "    sudo systemctl stop ${SERVICE_NAME}"
    echo "    sudo cp -a ${ROLLBACK_DIR}/. ${INSTALL_DIR}/"
    echo "    sudo systemctl start ${SERVICE_NAME}"
    echo
    hr
    exit 0
fi

# =============================================================================
#  FRESH INSTALL
# =============================================================================

# ── 1. System user ────────────────────────────────────────────────────────────
if ! id "${ODILON_USER}" &>/dev/null; then
    info "Creating system user '${ODILON_USER}' ..."
    run useradd \
        --system \
        --no-create-home \
        --shell /sbin/nologin \
        --home-dir "${INSTALL_DIR}" \
        --comment "Odilon Object Storage Server" \
        "${ODILON_USER}"
    ok "User '${ODILON_USER}' created."
else
    ok "System user '${ODILON_USER}' already exists."
fi

# ── 2. Directories ────────────────────────────────────────────────────────────
info "Creating directories ..."
for d in \
    "${INSTALL_DIR}" \
    "${INSTALL_DIR}/app" \
    "${INSTALL_DIR}/bin" \
    "${INSTALL_DIR}/resources" \
    "${INSTALL_DIR}/tmp" \
    "${CONF_DIR}" \
    "${LOG_DIR}" \
    "${DATA_DIR}" \
    "${BACKUP_ROOT}"
do
    run mkdir -p "$d"
done
ok "Directories created."

# ── 3. Binaries ───────────────────────────────────────────────────────────────
info "Installing server JAR ..."
run cp "$JAR" "${INSTALL_DIR}/app/"
ok "${JAR_NAME} → ${INSTALL_DIR}/app/"

info "Installing scripts ..."
run cp -r "${SCRIPT_DIR}/bin/". "${INSTALL_DIR}/bin/"
ok "Scripts → ${INSTALL_DIR}/bin/"

if [[ -d "${SCRIPT_DIR}/resources" ]]; then
    info "Installing resources ..."
    run cp -r "${SCRIPT_DIR}/resources/". "${INSTALL_DIR}/resources/"
    ok "Resources → ${INSTALL_DIR}/resources/"
fi

# ── 4. Configuration template ─────────────────────────────────────────────────
info "Installing configuration template → ${CONF_DIR} ..."
run cp -r "${SCRIPT_DIR}/config/". "${CONF_DIR}/"
# Stamp the correct port into the template
if ! $DRY_RUN; then
    if grep -q "^server\.port=" "${CONF_DIR}/odilon.properties" 2>/dev/null; then
        sed -i "s|^server\.port=.*|server.port=${PORT}|" "${CONF_DIR}/odilon.properties"
    else
        echo "server.port=${PORT}" >> "${CONF_DIR}/odilon.properties"
    fi
fi
ok "Configuration template → ${CONF_DIR}"
save_meta

# ── 5. Permissions ────────────────────────────────────────────────────────────
info "Setting ownership and permissions ..."

run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}"
run chmod 750 "${INSTALL_DIR}"
run chmod 750 "${INSTALL_DIR}/app" "${INSTALL_DIR}/resources" "${INSTALL_DIR}/tmp"
run chmod 750 "${INSTALL_DIR}/bin"
run chmod 750 "${INSTALL_DIR}"/bin/*.sh

run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${CONF_DIR}"
run chmod 750 "${CONF_DIR}"
if ! $DRY_RUN; then
    find "${CONF_DIR}" -type f -exec chmod 640 {} \;
fi

run chown "${ODILON_USER}:${ODILON_GROUP}" "${LOG_DIR}"
run chmod 750 "${LOG_DIR}"

run chown "${ODILON_USER}:${ODILON_GROUP}" "${DATA_DIR}"
run chmod 750 "${DATA_DIR}"

run chown root:root "${BACKUP_ROOT}"
run chmod 700 "${BACKUP_ROOT}"

ok "Permissions set."

# ── 6. systemd ────────────────────────────────────────────────────────────────
generate_service_unit
run systemctl daemon-reload
run systemctl enable "${SERVICE_NAME}"
ok "Service enabled: ${SERVICE_NAME}"

# =============================================================================
#  Post-install instructions
# =============================================================================
echo
hr
printf "${C_GREEN}  Odilon v%s installed successfully!${C_RESET}  [instance: %s]\n" \
    "$VERSION" "$INSTANCE_NAME"
hr
echo
info "Before starting, edit the configuration file:"
echo
echo "    sudo nano ${CONF_DIR}/odilon.properties"
echo
echo "  ┌─ Mandatory settings ──────────────────────────────────────────────┐"
echo "  │  redundancyLevel  = RAID 1 | ErasureCoding | RAID 0              │"
echo "  │  dataStorage      = ${DATA_DIR}/drive0, ...                       │"
echo "  │  accessKey        = <your access key>                             │"
echo "  │  secretKey        = <your secret key>                             │"
echo "  └───────────────────────────────────────────────────────────────────┘"
echo
echo "  Create drive directories before starting:"
echo "    sudo mkdir -p ${DATA_DIR}/drive0 ${DATA_DIR}/drive1"
echo "    sudo chown -R ${ODILON_USER}:${ODILON_GROUP} ${DATA_DIR}"
echo
info "Start:"
echo "    sudo systemctl start ${SERVICE_NAME}"
echo "    sudo systemctl status ${SERVICE_NAME}"
echo
info "Verify (API ping):"
echo "    curl -u odilon:odilon http://localhost:${PORT}/info"
echo
hr
info "Layout:"
printf "    %-18s %s\n" "Binaries:"      "${INSTALL_DIR}"
printf "    %-18s %s\n" "Configuration:" "${CONF_DIR}  ← edit this"
printf "    %-18s %s\n" "Logs:"          "${LOG_DIR}"
printf "    %-18s %s\n" "Data (default):" "${DATA_DIR}"
printf "    %-18s %s\n" "Rollbacks:"     "${BACKUP_ROOT}"
hr
echo
