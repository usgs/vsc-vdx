#!/bin/sh

java -Xmx256M -cp lib/winston.jar;lib/mysql.jar;lib/colt.jar gov.usgs.vdx.server.VDX $*