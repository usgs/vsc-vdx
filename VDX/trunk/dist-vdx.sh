#!/bin/sh

# vdx user, or whoever is running it, needs to have the following jar file in their $CLASSPATH
# /path/to/VDX/lib/vdx.jar
# alternatively, you can use -cp flag with the path to the vdx.jar file in the java command

VDX=/path/to/VDX
LOG=/path/to/vdx.log
CLASSPATH=$VDX/lib/vdx.jar

start () {
  echo "starting vdx ..."
  cd $VDX
  java -cp $CLASSPATH -Xmx512M gov.usgs.volcanoes.vdx.server.VDX --noinput > $LOG 2>&1 &
}

stop () {
  echo "stopping vdx ..."
  pkill -f gov.usgs.volcanoes.vdx.server.VDX
}

status () {
  pgrep -l -f gov.usgs.volcanoes.vdx.server.VDX
}


case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    status
    ;;
  restart)
    stop
    start
    ;;
  *)
    echo "Usage: $0 { start|stop|status|restart }"
    ;;
esac

exit 0
