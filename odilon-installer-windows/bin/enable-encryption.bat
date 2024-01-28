@echo off
call setvars.bat

cd ..
set ODILON_HOME=%CD%

start /b java -Xrs -Xms4G -Xmx8G -Xbootclasspath/a:%ODILON_HOME%\config -cp %ODILON_HOME%\config -Dwork=%ODILON_HOME%\tmp -Dlog4j2.configurationFile=%ODILON_HOME%\config\log4j2.xml -Dlog-path=%ODILON_HOME%\logs -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Djava.net.preferIPv4Stack=true -Duser.timezone=America/Argentina/Buenos_Aires -DstopId=%ODILON_APP_STOP_ID% -jar %ODILON_HOME%\app\%ODILON_APP% --initializeEncryption="true"


cd bin










