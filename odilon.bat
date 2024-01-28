
set ODILON_HOME=%CD%

java -Xms4G -Xmx8G -Xbootclasspath/a:%ODILON_HOME%\config -cp %ODILON_HOME%\src\main\resources -Dwork=%ODILON_HOME%\tmp -Dlog4j.configurationFile=%ODILON_HOME%\config\log4j2.xml -Dlog-path=%ODILON_HOME%\logs -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Djava.net.preferIPv4Stack=true -Duser.timezone=America/Argentina/Buenos_Aires -jar .\target\odilon-server-0.6-SNAPSHOT.jar &









