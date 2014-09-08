#!/usr/bin/perl

system( ("java", "-Xmx256M", "usgs.gov.winston.importSuppdata", $ARGV[0]) );
exit;
