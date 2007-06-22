#!/bin/sh

java -Xmx256M -cp lib/vdx.jar;lib/mysql.jar;lib/colt.jar gov.usgs.vdx.server.VDX $*