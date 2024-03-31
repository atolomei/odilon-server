#!/bin/bash

export ODILON_HOME=$(cd "$(dirname $(readlink -f "$0"))/..";pwd;cd )
source $ODILON_HOME/bin/config.sh


	pid=$(ps aux | grep -E ".*[j]ava.*odilon*" | awk '{print $2}')
	#pid=$(ps aux | grep -E ".*odilon*" | awk '{print $2}')

	if [[ ! -z "$pid" ]]
	then
			echo "killing pid $pid."
			kill -9 $pid
	else
			echo "Process not found."
	fi



