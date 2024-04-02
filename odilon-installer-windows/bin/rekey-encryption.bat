

@echo off
call setvars.bat

cd ..
set ODILON_HOME=%CD%


SETLOCAL
SET CMD=%~1

IF "%CMD%" == "" (
  GOTO usage
)


SET MASTERKEY=

:: Define foreground and background ANSI colors:
Set _fBlack=[30m
Set _bBlack=[40m
Set _fRed=[31m
Set _bRed=[41m
Set _fGreen=[32m
Set _bGreen=[42m
Set _fYellow=[33m
Set _bYellow=[43m
Set _fBlue=[34m
Set _bBlue=[44m
Set _fMag=[35m
Set _bMag=[45m
Set _fCyan=[36m
Set _bCyan=[46m
Set _fLGray=[37m
Set _bLGray=[47m
Set _fDGray=[90m
Set _bDGray=[100m
Set _fBRed=[91m
Set _bBRed=[101m
Set _fBGreen=[92m
Set _bBGreen=[102m
Set _fBYellow=[93m
Set _bBYellow=[103m
Set _fBBlue=[94m
Set _bBBlue=[104m
Set _fBMag=[95m
Set _bBMag=[105m
Set _fBCyan=[96m
Set _bBCyan=[106m
Set _fBWhite=[97m
Set _bBWhite=[107m
Set _RESET=[0m

SET PARAM=%~1
SET MASTERKEY=%~2

IF [%~1] == [] GOTO usage
IF [%~2] == [] GOTO usage
IF NOT "%PARAM%" == "-m" GOTO usage

echo Starting Odilon ...

start /b java -Xrs -Xms4G -Xmx8G -Xbootclasspath/a:%ODILON_HOME%\config -cp %ODILON_HOME%\config -Dwork=%ODILON_HOME%\tmp -Dlog4j2.configurationFile=%ODILON_HOME%\config\log4j2.xml -Dlog-path=%ODILON_HOME%\logs -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Djava.net.preferIPv4Stack=true -Duser.timezone=EST -DstopId=%ODILON_APP_STOP_ID% -jar %ODILON_HOME%\app\%ODILON_APP% --initializeEncryption="true" -DmasterKey=%MASTERKEY%
cd bin

EXIT /B 1


:usage
echo(
echo This script is used to generate a new encryption key for Odilon. 
echo The Encryption Service must have been initialized for this script to work
echo (ie. Odilon must already have an encryption key).
echo(
echo Usage: 
echo [93m\.rekey-encryption.bat[0m --[90mm[0m ^<masterkey^>
echo(
echo Example: 
echo [93m.\rekey-encryption.bat[0m --[90mm[0m 663480aab0a93a1459d91a649cf12408
echo(

EXIT /B 1









