#!/usr/bin/perl

system( ("java", "-Xmx256M", "usgs.gov.winston.importMetadata", $ARGV[0]) );
exit;
