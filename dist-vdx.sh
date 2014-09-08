#!/bin/sh

# vdx user, or whoever is running it, needs to have the following jar file in their $CLASSPATH
# /path/to/VDX/lib/vdx.jar
# alternatively, you can use -cp flag with the path to the vdx.jar file in the java command

JAVA=/path/to/bin/java
BASE=/path/to/VDX
LOGDIR=/path/to/log/dir
LOG=vdx.log

case "$1" in
'start')
	su - vdx -c "cd $BASE ; java -Xmx256M gov.usgs.vdx.server.VDX --noinput > $LOGDIR/$LOG 2>&1 & "
	echo "Successfully started VDX"
        ;;

'stop') 
	kill `ps -ef | grep gov.usgs.vdx.server.VDX | grep -v grep | awk '{print $2}'` 
	echo "Successfully stopped VDX"
        ;;

'restart')
	$0 stop
	sleep 2
	$0 start
        ;;

*)
        echo "Usage: $0 { start | stop | restart }"
        ;;
esac
exit 0
