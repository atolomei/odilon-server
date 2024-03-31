#!/bin/bash


# JAVA_HOME is found from the redlink command, if you want to use another JVM 
# you have to set JAVA_HOME, example
# export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-11.0.22.0.7-1.el7_9.x86_64/bin/java"


# This command will show where Java is in most Linux 
# note that JAVA_HOME must point to Java 11 (v1.1-beta)  or Java 17+ (v.1.5-beta or newer)
#
# readlink -f $(which java)
#
#
#


export JAVA_HOME="$(readlink -f $(which java))"


# Define the app directory
directory="$ODILON_HOME/app"


# Find the first file inside the directory whose name starts with "odilon-server"
file=$(find "$directory" -type f -name "odilon-server*" | head -n 1)

# Assign the found file to the variable APP
if [ -n "$file" ]; then

     filename=$(basename "$file")
    export APP="$ODILON_HOME/app/$filename"

else
    echo "No file found with name starting with 'odilon-server' in directory '$directory'"

fi


export JETTY_STOP_PWD="OdilonShutd0wn"
export APP_USER="odilon"

export ODILON_PROPS="
-Xbootclasspath/a:$ODILON_HOME/resources:$ODILON_HOME/config  
-Xms1G 
-Xmx4G 
-Dwork=$ODILON_HOME/tmp/ 
-Dlog-path=$ODILON_HOME/logs 
-Dlog4j.configurationFile=log4j2.xml 
-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector 
-Djava.net.preferIPv4Stack=true 
-Dfile.encoding="UTF-8"
-Dsun.jnu.encoding="UTF-8""

export DEBUG_PROP=""

