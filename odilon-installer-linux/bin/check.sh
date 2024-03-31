#!/bin/bash

RED="\e[31m"
GREEN="\e[32m"
ENDCOLOR="\e[0m"

export ODILON_HOME=$(cd "$(dirname $(readlink -f "$0"))/..";pwd; cd)


source $ODILON_HOME/bin/config.sh

pid=$(ps aux | grep -E ".*[j]ava.*odilon" | awk '{print $2}')

if [[ ! -z "$pid" ]]
then
                echo -e "Odilon running on pid ${GREEN}$pid${ENDCOLOR}"
else
                echo -e "Odilon ${RED}not running${ENDCOLOR}"
fi

