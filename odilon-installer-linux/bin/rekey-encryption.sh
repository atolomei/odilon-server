#!/bin/bash




export ODILON_HOME=$(cd "$(dirname $(readlink -f "$0"))/..";pwd;cd )

source $ODILON_HOME/bin/config.sh

echo "Odilon Home ->" $ODILON_HOME

pid=$(ps aux | grep -E ".*[j]ava.*odilon" | awk '{print $2}')

if [[ ! -z "$pid" ]]
then
                echo "Odilon is already running on pid $pid."
                exit 1
fi


#if [ ! $APP_USER == $(whoami) ]
#then
#        echo "Application must be run with user '$APP_USER'"
#        exit 1
#fi


KEY=

while getopts m: param
do
    case "${param}" in
        m) KEY=${OPTARG};;
    esac
done


if [ -z "$KEY" ]; then	
	echo
	echo "Usage ./rekey-encryption.sh -m masterKey"
	echo
	exit 1
fi




lib_files="$ODILON_HOME/app/:$ODILON_HOME/lib/*"
config_path="$ODILON_HOME/config"

echo "Changing current directory to $ODILON_HOME"
cd "$ODILON_HOME"

if [ -z "$JAVA_HOME" ]; then
	JAVA_CMD="java"
	javaPath=$(readlink -nf $(which java) | xargs dirname | xargs dirname | xargs dirname)
	echo "JAVA_HOME not set. Using default java installation ($javaPath)"
else
	JAVA_CMD="$JAVA_HOME"
	echo "Using java from JAVA_HOME variable ($JAVA_HOME)"
fi


"$JAVA_CMD" $DEBUG_PROP $MEM_PROPS -cp "$config_path:$lib_files" $ODILON_PROPS -jar $APP --initializeEncryption="true" --DmasterKey = $KEY$




 
