
@echo off
call setvars.bat

jps -v | find "%ODILON_APP%" | find "%ODILON_APP_STOP_ID%"










