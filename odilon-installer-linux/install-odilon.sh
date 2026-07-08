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
#  Configuration template
#  ----------------------
#    config/odilon.properties.template  ships in the tarball (clearly a template)
#    /etc/<name>/odilon.properties      written at install time — this is what you edit
#    The .template file is NEVER copied as-is; it is expanded and renamed on install.
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

# ── OS detection ──────────────────────────────────────────────────────────────
OS_KERNEL="$(uname -s)"
case "$OS_KERNEL" in
    Linux)  OS_TYPE="linux"  ;;
    Darwin) OS_TYPE="macos"  ;;
    *)      OS_TYPE="unknown"; warn "Unrecognised OS '${OS_KERNEL}' — proceeding best-effort." ;;
esac

# ── portable resolve_path (readlink -f is GNU-only; not on macOS stock shell) ─
resolve_path() {
    if command -v realpath &>/dev/null; then
        realpath "$1"
    elif [[ "$OS_TYPE" == "macos" ]] && command -v greadlink &>/dev/null; then
        greadlink -f "$1"
    else
        # Pure POSIX fallback: resolve one level of symlink via cd + pwd
        local target="$1"
        local dir
        dir="$(cd "$(dirname "$target")" 2>/dev/null && pwd -P)" || dir="$(dirname "$target")"
        echo "${dir}/$(basename "$target")"
    fi
}

# ── portable sed in-place (macOS BSD sed requires '' after -i) ────────────────
sed_inplace() {
    if [[ "$OS_TYPE" == "macos" ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

SCRIPT_DIR="$(cd "$(dirname "$(resolve_path "$0")")" && pwd)"
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
    # Linux (systemd)
    SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
    # macOS (launchd) — reverse-DNS label convention
    SERVICE_PLIST="/Library/LaunchDaemons/io.odilon.${SERVICE_NAME}.plist"
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
    # Only fill blanks — CLI flags already set OPT_* variables.
    # Use if-statements, NOT [[ ]] && assignment: with set -e a false [[ ]]
    # returns exit code 1 which silently kills the script.
    if [[ -z "$OPT_PREFIX" && -n "$_prefix" ]]; then OPT_PREFIX="$_prefix"; fi
    if [[ -z "$OPT_CONF"   && -n "$_conf"   ]]; then OPT_CONF="$_conf";     fi
    if [[ -z "$OPT_LOGS"   && -n "$_logs"   ]]; then OPT_LOGS="$_logs";     fi
    if [[ -z "$OPT_USER"   && -n "$_user"   ]]; then OPT_USER="$_user";     fi
    if [[ -z "$OPT_PORT"   && -n "$_port"   ]]; then OPT_PORT="$_port";     fi
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
JAVA_BIN="$(resolve_path "$(which java)")"
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

# An existing JAR at INSTALL_DIR/app/ means a live installation is present
# even if the config was already manually moved elsewhere.
EXISTING_JAR="$(find "${INSTALL_DIR}/app" -maxdepth 1 -name "odilon-server*.jar" 2>/dev/null | head -n1)"

if [[ -d "$CONF_DIR" ]]; then
    # /etc/<name>/ exists → modern layout — normal upgrade
    MODE="upgrade"
elif [[ -f "${LEGACY_CONF_DIR}/odilon.properties" ]]; then
    # config still inside INSTALL_DIR/config/ → classic legacy layout
    MODE="migrate"
elif [[ -n "$EXISTING_JAR" ]]; then
    # Binary present but config already moved manually or in unexpected location.
    # Treat as migrate so we snapshot before touching anything.
    MODE="migrate"
    warn "Existing installation detected at ${INSTALL_DIR} (no config found at ${LEGACY_CONF_DIR})."
    warn "Running in migrate mode. Review ${CONF_DIR}/odilon.properties after install."
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

# ── portable snapshot pruning (replaces bash4-only mapfile) ──────────────────
prune_snapshots() {
    local dir="$1"
    local keep="$2"
    local snaps=()
    while IFS= read -r line; do
        snaps+=("$line")
    done < <(find "$dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort)
    local excess=$(( ${#snaps[@]} - keep ))
    if (( excess > 0 )); then
        for snap in "${snaps[@]:0:$excess}"; do
            rm -rf "$snap"
            info "Pruned old snapshot: $snap"
        done
        ok "Snapshots pruned (keeping last ${keep})."
    fi
}

# ── resolve group to a value chown(1) will always accept ─────────────────────
# On Linux  → group name works fine (useradd creates it atomically).
# On macOS  → dscl-created groups are not immediately visible to BSD chown via
#             name; numeric GID always works.  We try three sources in order:
#               1. dscl PrimaryGroupID on the group record
#               2. dscl gid attribute on the group record
#               3. id -g of the already-created user
#             Falls back silently to the name string if nothing is found.
resolve_odilon_group() {
    if [[ "$OS_TYPE" != "macos" ]]; then
        ODILON_GROUP="${ODILON_USER}"
        return
    fi
    local gid=""
    gid=$(dscl . -read "/Groups/${ODILON_USER}" PrimaryGroupID 2>/dev/null \
            | awk '{print $2}') || true
    if [[ -z "$gid" ]]; then
        gid=$(dscl . -read "/Groups/${ODILON_USER}" gid 2>/dev/null \
                | awk '{print $2}') || true
    fi
    if [[ -z "$gid" ]] && id "${ODILON_USER}" &>/dev/null; then
        gid=$(id -g "${ODILON_USER}" 2>/dev/null) || true
    fi
    if [[ -n "$gid" ]]; then
        ODILON_GROUP="$gid"
    fi
    # If nothing resolved keep the name — chown will error with a clear message
}

# ── OS-abstracted user creation ───────────────────────────────────────────────
os_create_user() {
    local user="$1" home="$2"
    if [[ "$OS_TYPE" == "linux" ]]; then
        run useradd \
            --system \
            --no-create-home \
            --shell /sbin/nologin \
            --home-dir "$home" \
            --comment "Odilon Object Storage Server" \
            "$user"
    elif [[ "$OS_TYPE" == "macos" ]]; then
        # Find next available UID in macOS system-user range (200+)
        local next_uid
        next_uid=$(dscl . -list /Users UniqueID 2>/dev/null \
            | awk '{print $2}' | sort -n \
            | awk 'BEGIN{u=200} {if($1==u)u++} END{print u}')
        # Find next available GID in the same range
        local next_gid
        next_gid=$(dscl . -list /Groups gid 2>/dev/null \
            | awk '{print $2}' | sort -n \
            | awk 'BEGIN{g=200} {if($1==g)g++} END{print g}')
        # Create a dedicated group first (so chown user:group works)
        if ! dscl . -read "/Groups/${user}" &>/dev/null; then
            run dscl . -create "/Groups/${user}"
            run dscl . -create "/Groups/${user}" gid             "$next_gid"
            run dscl . -create "/Groups/${user}" GroupMembership "$user"
        fi
        # Create the service user
        run dscl . -create "/Users/${user}"
        run dscl . -create "/Users/${user}" UserShell        /usr/bin/false
        run dscl . -create "/Users/${user}" RealName         "Odilon Object Storage Server"
        run dscl . -create "/Users/${user}" UniqueID         "$next_uid"
        run dscl . -create "/Users/${user}" PrimaryGroupID   "$next_gid"
        run dscl . -create "/Users/${user}" NFSHomeDirectory "$home"
        # Flush directory services cache so BSD tools (chown, id) see the new
        # group immediately without requiring a reboot or logout.
        dscacheutil -flushcache 2>/dev/null || true
        dsmemberutil flushcache 2>/dev/null || true
    else
        warn "Unknown OS — skipping user creation. Create user '${user}' manually."
    fi
}

# ── OS-abstracted service management ─────────────────────────────────────────
os_service_is_active() {
    local svc="$1"
    if [[ "$OS_TYPE" == "linux" ]]; then
        # Only exact "active" state counts — not activating/failed/unknown
        [[ "$(systemctl is-active "$svc" 2>/dev/null || true)" == "active" ]]
    elif [[ "$OS_TYPE" == "macos" ]]; then
        # Exact instance label — launchctl print exits non-zero when not loaded
        launchctl print "system/io.odilon.${svc}" >/dev/null 2>&1
    else
        return 1
    fi
}

os_stop_service() {
    local svc="$1"
    if [[ "$OS_TYPE" == "linux" ]]; then
        run systemctl stop "$svc"
    elif [[ "$OS_TYPE" == "macos" ]]; then
        local label="io.odilon.${svc}"
        # bootout stops and unloads; ignore error if already stopped
        launchctl bootout "system/${label}" 2>/dev/null || true
    fi
}

os_start_service() {
    local svc="$1"
    if [[ "$OS_TYPE" == "linux" ]]; then
        run systemctl start "$svc"
    elif [[ "$OS_TYPE" == "macos" ]]; then
        local label="io.odilon.${svc}"
        launchctl bootstrap system "${SERVICE_PLIST}" 2>/dev/null || \
            launchctl load "${SERVICE_PLIST}" 2>/dev/null || true
        launchctl enable "system/${label}" 2>/dev/null || true
    fi
}

os_reload_daemon() {
    if [[ "$OS_TYPE" == "linux" ]]; then
        run systemctl daemon-reload
    fi
    # launchd picks up plist changes on next bootstrap — no reload needed
}

os_enable_service() {
    local svc="$1"
    if [[ "$OS_TYPE" == "linux" ]]; then
        run systemctl enable "$svc"
    fi
    # launchd: plist presence in /Library/LaunchDaemons is sufficient for boot start
}

os_service_status() {
    local svc="$1"
    if [[ "$OS_TYPE" == "linux" ]]; then
        systemctl status "$svc" --no-pager -l || true
    elif [[ "$OS_TYPE" == "macos" ]]; then
        launchctl print "system/io.odilon.${svc}" 2>/dev/null || true
    fi
}

os_stop_cmd()  {
    if [[ "$OS_TYPE" == "linux" ]]; then echo "sudo systemctl stop  ${SERVICE_NAME}"
    else                                   echo "sudo launchctl bootout system ${SERVICE_PLIST}"; fi
}
os_start_cmd() {
    if [[ "$OS_TYPE" == "linux" ]]; then echo "sudo systemctl start ${SERVICE_NAME}"
    else                                   echo "sudo launchctl bootstrap system ${SERVICE_PLIST}"; fi
}
os_status_cmd() {
    if [[ "$OS_TYPE" == "linux" ]]; then echo "sudo systemctl status ${SERVICE_NAME}"
    else                                   echo "sudo launchctl list io.odilon.${SERVICE_NAME}"; fi
}

# ── service unit generation (Linux: systemd, macOS: launchd) ─────────────────
generate_service_unit() {
    if [[ "$OS_TYPE" == "linux" ]]; then
        info "Writing ${SERVICE_FILE} ..."
        if $DRY_RUN; then
            printf "  ${C_GREY}[dry-run]${C_RESET}  would write %s\n" "$SERVICE_FILE"; return
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
        ok "systemd unit written → ${SERVICE_FILE}"

    elif [[ "$OS_TYPE" == "macos" ]]; then
        info "Writing ${SERVICE_PLIST} ..."
        if $DRY_RUN; then
            printf "  ${C_GREY}[dry-run]${C_RESET}  would write %s\n" "$SERVICE_PLIST"; return
        fi
        mkdir -p "$(dirname "$SERVICE_PLIST")"
        cat > "$SERVICE_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>             <string>io.odilon.${INSTANCE_NAME}</string>
  <key>UserName</key>          <string>${ODILON_USER}</string>
  <key>WorkingDirectory</key>  <string>${INSTALL_DIR}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${INSTALL_DIR}/bin/start-service.sh</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>ODILON_HOME</key>      <string>${INSTALL_DIR}</string>
    <key>ODILON_CONF</key>      <string>${CONF_DIR}</string>
    <key>ODILON_LOGS</key>      <string>${LOG_DIR}</string>
    <key>INSTANCE_NAME</key>    <string>${INSTANCE_NAME}</string>
  </dict>
  <key>RunAtLoad</key>         <true/>
  <key>KeepAlive</key>
  <dict><key>SuccessfulExit</key><true/></dict>
  <key>StandardOutPath</key>   <string>${LOG_DIR}/launchd.log</string>
  <key>StandardErrorPath</key> <string>${LOG_DIR}/launchd.log</string>
</dict>
</plist>
EOF
        ok "launchd plist written → ${SERVICE_PLIST}"
    else
        warn "Unknown OS — service unit not written. Start Odilon manually with: ${INSTALL_DIR}/bin/start.sh"
    fi
}

# ── persist installation parameters for future upgrades ──────────────────────
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
    # Resolve group to numeric GID on macOS (BSD chown requires it for dscl groups)
    resolve_odilon_group

    # ── 1. Stop service ───────────────────────────────────────────────────────
    if os_service_is_active "${SERVICE_NAME}"; then
        info "Stopping ${SERVICE_NAME} ..."
        os_stop_service "${SERVICE_NAME}"
        if ! $DRY_RUN; then
            for _ in $(seq 1 30); do
                if ! os_service_is_active "${SERVICE_NAME}"; then break; fi
                sleep 1
            done
            if os_service_is_active "${SERVICE_NAME}"; then
                die "${SERVICE_NAME} did not stop within 30 s — aborting."
            fi
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
    $DRY_RUN || prune_snapshots "${BACKUP_ROOT}" "${MAX_ROLLBACK_SNAPSHOTS}"

    # ── 3. Replace binaries — never touch CONF_DIR ───────────────────────────
    info "Replacing binaries (${CONF_DIR} untouched) ..."

    run mkdir -p "${INSTALL_DIR}/app"
    run rm -f "${INSTALL_DIR}"/app/odilon-server*.jar
    run cp "$JAR" "${INSTALL_DIR}/app/"
    ok "Server JAR → ${INSTALL_DIR}/app/${JAR_NAME}"

    run cp -r "${SCRIPT_DIR}/bin/". "${INSTALL_DIR}/bin/"
    run chmod 750 "${INSTALL_DIR}"/bin/*.sh
    ok "Scripts → ${INSTALL_DIR}/bin/"

    if [[ -d "${SCRIPT_DIR}/resources" ]]; then
        run rm -rf "${INSTALL_DIR}/resources"
        run cp -r  "${SCRIPT_DIR}/resources" "${INSTALL_DIR}/"
        ok "Resources → ${INSTALL_DIR}/resources/"
    fi

    # Rechown the entire install tree — covers parent dir, new JAR, scripts, resources
    run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}"
    run chmod 750 "${INSTALL_DIR}"
    ok "Ownership set → ${INSTALL_DIR}"

    # ── 3b. Log directory — always ensure odilon user can write logs ──────────
    # mkdir -p is safe when the dir already exists; chown re-applies even after
    # a manual intervention or an OS upgrade that reset ownership.
    run mkdir -p "${LOG_DIR}"
    run chown "${ODILON_USER}:${ODILON_GROUP}" "${LOG_DIR}"
    run chmod 750 "${LOG_DIR}"
    ok "Log dir ownership confirmed → ${LOG_DIR}"

    # ── 4. Refresh service unit and restart ───────────────────────────────────
    generate_service_unit
    os_reload_daemon
    info "Starting ${SERVICE_NAME} ..."
    os_start_service "${SERVICE_NAME}"
    if ! $DRY_RUN; then
        sleep 3
        os_service_status "${SERVICE_NAME}"
    fi

    # ── summary ───────────────────────────────────────────────────────────────
    echo
    hr
    ok "Upgrade to v${VERSION} complete  [${INSTANCE_NAME}]"
    info "Configuration unchanged : ${CONF_DIR}"
    info "Rollback if needed:"
    echo
    echo "    $(os_stop_cmd)"
    echo "    sudo cp -a ${ROLLBACK_DIR}/. ${INSTALL_DIR}/"
    echo "    $(os_start_cmd)"
    echo
    hr
    exit 0
fi

# =============================================================================
#  MIGRATE — legacy install (config in $INSTALL_DIR/config/)
#  Moves config to /etc/<name>, replaces binaries, regenerates service unit.
# =============================================================================
if [[ "$MODE" == "migrate" ]]; then

    # Resolve group to numeric GID on macOS before any chown calls
    resolve_odilon_group

    # ── Try to recover the existing log directory from the old service unit ───
    # The log path is NOT in odilon.properties — it is injected as an
    # Environment= variable in the systemd unit / launchd plist.
    # Read it from there so we preserve and fix the real legacy log dir.
    _legacy_log=""
    if [[ "$OS_TYPE" == "linux" ]] && [[ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]]; then
        _legacy_log=$(grep -E '^Environment="?ODILON_LOGS=' \
            "/etc/systemd/system/${SERVICE_NAME}.service" 2>/dev/null \
            | sed 's/.*ODILON_LOGS="\?\([^"]*\)"\?.*/\1/' | head -n1) || true
    elif [[ "$OS_TYPE" == "macos" ]] && [[ -f "${SERVICE_PLIST}" ]]; then
        _legacy_log=$(grep -A1 'ODILON_LOGS' "${SERVICE_PLIST}" 2>/dev/null \
            | grep '<string>' | sed 's/.*<string>\(.*\)<\/string>.*/\1/' | head -n1) || true
    fi
    if [[ -n "$_legacy_log" && -z "$OPT_LOGS" ]]; then
        LOG_DIR="$_legacy_log"
        info "Preserving existing log directory from service unit: ${LOG_DIR}"
    fi

    warn "Legacy installation detected — config found in ${LEGACY_CONF_DIR:-${INSTALL_DIR}}"
    warn "Migrating configuration → ${CONF_DIR}"
    echo
    info "What will happen:"
    echo "  1. Service stopped (if running)"
    echo "  2. Full snapshot saved  → ${BACKUP_ROOT}/${TIMESTAMP}  (config included)"
    echo "  3. Config copied        → ${CONF_DIR}  (originals kept as config.migrated.${TIMESTAMP}/)"
    echo "  4. Binaries replaced       (app/, bin/, resources/)"
    echo "  5. Log dir ownership fixed → ${LOG_DIR}"
    echo "  6. New service unit written with ODILON_CONF=${CONF_DIR}"
    echo "  7. Service restarted"
    echo

    # ── 1. Stop service ───────────────────────────────────────────────────────
    if os_service_is_active "${SERVICE_NAME}"; then
        info "Stopping ${SERVICE_NAME} ..."
        os_stop_service "${SERVICE_NAME}"
        if ! $DRY_RUN; then
            for _ in $(seq 1 30); do
                if ! os_service_is_active "${SERVICE_NAME}"; then break; fi
                sleep 1
            done
            if os_service_is_active "${SERVICE_NAME}"; then
                die "${SERVICE_NAME} did not stop within 30 s — aborting."
            fi
        fi
        ok "Service stopped."
    else
        warn "${SERVICE_NAME} was not running."
    fi

    # ── 2. Full snapshot (config included) ───────────────────────────────────
    ROLLBACK_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
    run mkdir -p "${ROLLBACK_DIR}"
    for sub in app bin resources config; do
        [[ -d "${INSTALL_DIR}/${sub}" ]] && run cp -a "${INSTALL_DIR}/${sub}" "${ROLLBACK_DIR}/"
    done
    ok "Full snapshot saved → ${ROLLBACK_DIR}"

    $DRY_RUN || prune_snapshots "${BACKUP_ROOT}" "${MAX_ROLLBACK_SNAPSHOTS}"

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
    ok "Server JAR → ${INSTALL_DIR}/app/${JAR_NAME}"

    run cp -r "${SCRIPT_DIR}/bin/." "${INSTALL_DIR}/bin/"
    run chmod 750 "${INSTALL_DIR}"/bin/*.sh
    ok "Scripts → ${INSTALL_DIR}/bin/"

    if [[ -d "${SCRIPT_DIR}/resources" ]]; then
        run rm -rf "${INSTALL_DIR}/resources"
        run cp -r  "${SCRIPT_DIR}/resources" "${INSTALL_DIR}/"
        ok "Resources → ${INSTALL_DIR}/resources/"
    fi

    # Rechown the entire install tree — covers parent dir, new JAR, scripts, resources
    run chown -R "${ODILON_USER}:${ODILON_GROUP}" "${INSTALL_DIR}"
    run chmod 750 "${INSTALL_DIR}"
    ok "Ownership set → ${INSTALL_DIR}"

    # ── 4b. Log directory — always ensure odilon user can write logs ──────────
    run mkdir -p "${LOG_DIR}"
    run chown "${ODILON_USER}:${ODILON_GROUP}" "${LOG_DIR}"
    run chmod 750 "${LOG_DIR}"
    ok "Log dir ownership confirmed → ${LOG_DIR}"

    # ── 5. New service unit, metadata, restart ────────────────────────────────
    generate_service_unit
    save_meta
    os_reload_daemon
    info "Starting ${SERVICE_NAME} ..."
    os_start_service "${SERVICE_NAME}"
    if ! $DRY_RUN; then
        sleep 3
        os_service_status "${SERVICE_NAME}"
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
    echo "    $(os_stop_cmd)"
    echo "    sudo cp -a ${ROLLBACK_DIR}/. ${INSTALL_DIR}/"
    echo "    $(os_start_cmd)"
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
    os_create_user "${ODILON_USER}" "${INSTALL_DIR}"
    ok "User '${ODILON_USER}' created."
else
    ok "System user '${ODILON_USER}' already exists."
fi
# Resolve ODILON_GROUP to numeric GID on macOS so all subsequent chown calls work.
# Must come after os_create_user so the group exists in the directory service.
resolve_odilon_group

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

# ── 4. Configuration ──────────────────────────────────────────────────────────
info "Installing configuration → ${CONF_DIR} ..."

# Copy all non-template config files (e.g. log4j2.xml) directly
if ! $DRY_RUN; then
    for f in "${SCRIPT_DIR}"/config/*; do
        [[ "$f" == *.template ]] && continue
        cp "$f" "${CONF_DIR}/"
    done
else
    printf "  ${C_GREY}[dry-run]${C_RESET}  would copy non-template files from %s → %s\n" \
        "${SCRIPT_DIR}/config/" "${CONF_DIR}/"
fi

# Expand odilon.properties.template → odilon.properties
# The .template file itself is NEVER placed in /etc/<name>/
TEMPLATE="${SCRIPT_DIR}/config/odilon.properties.template"
[[ -f "$TEMPLATE" ]] || die "config/odilon.properties.template not found — tarball incomplete?"
run cp "$TEMPLATE" "${CONF_DIR}/odilon.properties"

# Stamp the correct port
if ! $DRY_RUN; then
    if grep -q "^server\.port=" "${CONF_DIR}/odilon.properties" 2>/dev/null; then
        sed_inplace "s|^server\.port=.*|server.port=${PORT}|" "${CONF_DIR}/odilon.properties"
    else
        echo "server.port=${PORT}" >> "${CONF_DIR}/odilon.properties"
    fi
fi
ok "Configuration → ${CONF_DIR}/odilon.properties  (from template, port=${PORT})"
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

run chown 0:0 "${BACKUP_ROOT}"
run chmod 700 "${BACKUP_ROOT}"

ok "Permissions set."

# ── 6. Service unit ───────────────────────────────────────────────────────────
generate_service_unit
os_reload_daemon
os_enable_service "${SERVICE_NAME}"
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
echo "    $(os_start_cmd)"
echo "    $(os_status_cmd)"
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
