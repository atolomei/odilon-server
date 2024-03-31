#!/bin/bash




export ODILON_HOME=$(cd "$(dirname $(readlink -f "$0"))/..";pwd;cd )

source $ODILON_HOME/bin/config.sh



pid=$(ps aux | grep -E ".*[j]ava.*odilon" | awk '{print $2}')

if [[ ! -z "$pid" ]]
then
                echo "Odilon is already running on pid $pid."
                exit 1
fi


if [ ! $APP_USER == $(whoami) ]
then
        echo "Application must be run with user '$APP_USER'"
        exit 1
fi


lib_files="$ODILON_HOME/app/:$ODILON_HOME/lib/*"
config_path="$ODILON_HOME/config"


echo
echo "Changing current directory to $ODILON_HOME"


cd "$ODILON_HOME"

if [ -z "$JAVA_HOME" ]; then
	JAVA_CMD="java"
	javaPath=$(readlink -nf $(which java) | xargs dirname | xargs dirname | xargs dirname)
	echo "JAVA_HOME not set. Using default java installation ($javaPath)"
else
	#JAVA_CMD="$JAVA_HOME"	
	JAVA_CMD="$JAVA_HOME"
	echo "Using java from JAVA_HOME variable ($JAVA_HOME)"
	echo
fi

echo Starting the server... to shutdown the server press CRTL-C
echo

"$JAVA_CMD" $DEBUG_PROP $MEM_PROPS -cp "$config_path:$lib_files" $ODILON_PROPS -jar $APP



 
