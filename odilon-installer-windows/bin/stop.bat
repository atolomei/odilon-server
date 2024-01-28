
@echo off
call setvars.bat

for /f "tokens=1" %%i in ('jps -v ^| find "%ODILON_APP%" ^| find "%ODILON_APP_STOP_ID%"') do ( echo 'stopping Odilon PID %%i' && taskkill /F /PID %%i )








