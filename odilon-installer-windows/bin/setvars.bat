set ODILON_HOME=%CD%
set ODILON_USER_TIMEZONE="America/Argentina/Buenos_Aires"
set ODILON_APP_STOP_ID=4970010beta
set ODILON_XMS=4G
set ODILON_XMX=12G
set ODILON_USER_LANGUAGE=en
set ODILON_APP_STOP_ID=odilon4970

rem set ODILON_APP=odilon-server-1.13.jar 

rem Initialize variable
set "ODILON_APP="

rem Scan the directory and find the first .jar file
for %%f in ("..\app\*.jar") do (    
    set "ODILON_APP=%%~nxf"
)


