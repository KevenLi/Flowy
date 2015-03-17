#!/usr/bin/env bash

AGENTMAIN="flowy.agent.Shell"
CFGFILE="../conf/agent.cfg"
PIDFILE="agent.pid"
for i in ../*.jar
do
    CLASSPATH="$i:$CLASSPATH"
done
for i in ../lib/*.jar
do
    CLASSPATH="$i:$CLASSPATH"
done
_AGENT_OUT="agent.out"

case $1 in
start)
    echo  -n "Starting flowy agent ... "
    if [ -f "$PIDFILE" ]; then
      if kill -0 `cat "$PIDFILE"` > /dev/null 2>&1; then
         echo $command already running as process `cat "$PIDFILE"`. 
         exit 0
      fi
    fi
    nohup "java" -cp "$CLASSPATH" $AGENTMAIN $CFGFILE> "$_AGENT_OUT" 2>&1 < /dev/null &
    if [ $? -eq 0 ]
    then
      if /bin/echo -n $! > "$PIDFILE"
      then
		echo /bin/echo -n $!
        sleep 1
        echo STARTED
      else
        echo FAILED TO WRITE PID
        exit 1
      fi
    else
      echo SERVER DID NOT START
      exit 1
    fi
    ;;
start-foreground)
    "java" "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" "-Dzookeeper.root.logger=${ZOO_LOG4J_PROP}" \
    -cp "$CLASSPATH" $AGENTMAIN $CFGFILE
    ;;
print-cmd)
    echo "\"$JAVA\" -Dzookeeper.log.dir=\"${ZOO_LOG_DIR}\" -Dzookeeper.root.logger=\"${ZOO_LOG4J_PROP}\" -cp \"$CLASSPATH\" $JVMFLAGS $ZOOMAIN \"$ZOOCFG\" > \"$_ZOO_DAEMON_OUT\" 2>&1 < /dev/null"
    ;;
stop)
    echo -n "Stopping flowy agent ... "
    if [ ! -f "$PIDFILE" ]
    then
      echo "no agent to stop (could not find file $PIDFILE)"
    else
      kill -9 $(cat "$PIDFILE")
      rm "$PIDFILE"
      echo STOPPED
    fi
    exit 0
    ;;
restart)
    shift
    "$0" stop ${@}
    sleep 3
    "$0" start ${@}
    ;;
auto-restart)
    exist=`cat /etc/crontab|grep /root/flowyagent-0.0.1/bin/start`
    if [ -z "$exist" ]; then
    echo "15 6 * * * root bash /root/flowyagent-0.0.1/bin/start">>/etc/crontab
    service crond start
    echo auto-restart set success
    echo flowyagent will restart every 6.15am
    else
    service crond reload
    echo auto-restart has exist
    fi
    ;;
*)
    echo "Usage: $0 {start|start-foreground|stop|restart}" >&2
esac
