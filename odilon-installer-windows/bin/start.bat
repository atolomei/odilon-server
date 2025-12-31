@echo off
call setvars.bat

cd ..
set ODILON_HOME=%CD%

start /b java -Xrs -Xms%ODILON_XMS% -XX:G1HeapRegionSize=%ODILON_G1HEAPREGIONSIZE% -Xmx%ODILON_XMX% -Xbootclasspath/a:%ODILON_HOME%\config -cp %ODILON_HOME%\config -Dwork=%ODILON_HOME%\tmp -Dlog4j2.configurationFile=%ODILON_HOME%\config\log4j2.xml  -Dlog-path=%ODILON_HOME%\logs -Dfile.encoding="UTF-8" -Dsun.jnu.encoding="UTF-8"  -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Djava.net.preferIPv4Stack=true -Duser.language=%ODILON_USER_LANGUAGE% -DstopId=%ODILON_APP_STOP_ID% -Duser.timezone=%ODILON_USER_TIMEZONE%  -jar %ODILON_HOME%\app\%ODILON_APP%
cd bin


















