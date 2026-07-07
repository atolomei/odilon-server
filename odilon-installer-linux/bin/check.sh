#!/bin/bash

# Colores (macOS usa BSD echo, que no interpreta -e por defecto)
RED="\033[31m"
GREEN="\033[32m"
ENDCOLOR="\033[0m"

# readlink -f no existe en macOS, usamos una alternativa con perl o cd + pwd
resolve_path() {
  TARGET="$1"
  cd "$(dirname "$TARGET")" || exit 1
  TARGET=$(basename "$TARGET")

  while [ -L "$TARGET" ]; do
    TARGET=$(readlink "$TARGET")
    cd "$(dirname "$TARGET")" || exit 1
    TARGET=$(basename "$TARGET")
  done

  DIR=$(pwd -P)
  echo "$DIR/$TARGET"
}

export ODILON_HOME=$(cd "$(dirname "$(resolve_path "$0")")/.." && pwd)

# Cargar configuración
source "$ODILON_HOME/bin/config.sh"

# Buscar proceso
pid=$(ps aux | grep -E ".*[j]ava.*odilon-server"  | grep   $OID  | awk '{print $2}')

# Mostrar estado
if [[ -n "$pid" ]]; then
    printf "Odilon running on pid ${GREEN}%s${ENDCOLOR}\n" "$pid"
else
    printf "Odilon ${RED}not running${ENDCOLOR}\n"
fi
