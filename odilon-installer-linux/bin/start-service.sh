#!/bin/bash


source $ODILON_HOME/bin/config.sh

rm -f $ODILON_HOME/logs/startup.log  2> /dev/null

# lib_files="$ODILON_HOME/app/"
# config_path="$ODILON_HOME/config"


cd "$ODILON_HOME"

if [ -z "$JAVA_HOME" ]; then
        JAVA_CMD="java"
		javaPath=$(readlink -nf $(which java) | xargs dirname | xargs dirname | xargs dirname)
		echo "JAVA_HOME not set. Using default java installation ($javaPath)"
	else
		JAVA_CMD="$JAVA_HOME"
		echo "Using java from JAVA_HOME variable ($JAVA_HOME)"
fi

# "$JAVA_CMD" $DEBUG_PROP $MEM_PROPS -cp "$config_path:$lib_files" $ODILON_PROPS -jar $APP

"$JAVA_CMD" $DEBUG_PROP $MEM_PROPS $ODILON_PROPS -jar $APP
	
