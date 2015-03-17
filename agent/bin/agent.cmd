setlocal
set CFGDIR=%~dp0%..\conf
set LOG_DIR=%~dp0%..
set LOG4J_PROP=INFO,CONSOLE

REM for sanity sake assume Java 1.6
REM see: http://java.sun.com/javase/6/docs/technotes/tools/windows/java.html

REM add the zoocfg dir to classpath
set CLASSPATH=%CFGDIR%

REM make it work in the release
SET CLASSPATH=%~dp0..\*;%~dp0..\lib\*;%CLASSPATH%

REM make it work for developers
SET CLASSPATH=%~dp0..\build\classes;%~dp0..\build\lib\*;%CLASSPATH%

set CFG=%CFGDIR%\agent.cfg

set AGENTMAIN=flowy.agent.Shell
echo on
java "-Dzookeeper.log.dir=%LOG_DIR%" "-Dzookeeper.root.logger=%ZOO_LOG4J_PROP%" -cp "%CLASSPATH%" %AGENTMAIN% "%CFG%" %*

endlocal