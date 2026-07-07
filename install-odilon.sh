#!/bin/bash
# =============================================================================
#  This file is a development convenience wrapper.
#
#  The canonical installer lives in:
#      odilon-installer-linux/install-odilon.sh
#
#  It is shipped inside the distribution tarball.
#  To install from the tarball:
#
#      tar xvf odilon-server-<version>.tar.gz
#      cd odilon-server-<version>
#      sudo ./install-odilon.sh [--dry-run]
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
exec "$SCRIPT_DIR/odilon-installer-linux/install-odilon.sh" "$@"