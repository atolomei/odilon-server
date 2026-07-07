#!/bin/bash


 
export ODILON_HOME=$(cd "$(dirname $(readlink -f "$0"))/..";pwd;cd )
source $ODILON_HOME/bin/config.sh


pid=$(ps aux | grep -E ".*[j]ava.*odilon-server" | grep $OID |  awk '{print $2}')

if [[ ! -z "$pid" ]]
then
                echo "ODILON is already running on pid $pid."
                exit 1
fi

if [ ! $APP_USER == $(whoami) ]
then
        echo "Odilon must be run with user '$APP_USER'"
        exit 1
fi

echo "This script starts up -> $APP"

rm -f $ODILON_LOGS/startup.log  2> /dev/null
nohup $ODILON_HOME/bin/start-cmd.sh < /dev/null > /dev/null 2>&1 &


#nohup ./start-cmd.sh < /dev/null &

echo "Odilon background process launched."

if ! [ "$1" = "noTail" ]; then
	echo "Appending 100 lines of startup.log to standard output, pressing ctrl+c to stop printing"
	echo "If no output is generated after 1 minute, try start-cmd.sh to start the server in foreground and check the errors on the console"

	sleep 4

	while [ ! -f $ODILON_LOGS/startup.log ]
	do
	  echo "waiting for startup.log file to be created."
	  sleep 2
	done
	
	echo "startup.log created"

	sleep 3

	echo "collecting startup log info"

	sleep 4

	# tail -F $ODILON_HOME/logs/startup.log

	 cat $ODILON_LOGS/startup.log



fi