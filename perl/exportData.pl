#!/usr/bin/perl
use URI::Escape;
 
# The URL w/o request parameters
my $csv_request_path = "http://localhost:8080/valve3/valve3.jsp";

# Check the number of arguments
my $argc = $#ARGV + 1;
die "Wrong number of arguments: " . $argc  unless $argc % 2 == 0;

my $abortmsg = "";	# if not "", abort w/ this message
my $key;
my $querymap = {};	# map of query args to their valuse

# argument definitions
# 	1st char indicates kind of argument:
#		! = exactly 1 instance allowed
#		? = up to 1 instance allowed
#		+ = 0 or more instances allowed
#			(values will be comma-separated)
#		* = special case for columns
#	Rest of value is name used in query string
my %arg_def = ("source"=>"!src.0", "channel"=>"!chNames.0", "start"=>"!st.0", 
	"end"=>"?et.0", "timezone"=>"?tz", 
	"datatype"=>"+dt", "column"=>"*colNames", "rank"=>"?rkName" );
my %cols = ();

# Walk the command-line arguments, filling querymap
for ( $i=0; $i<$argc; $i += 2 ) {
	if ( substr( $ARGV[$i], 0, 1 ) eq "-" ) {
		$key = substr( $ARGV[$i], 1 );
		if ( exists $arg_def{$key} ) {
		} else {
			$abortmsg = $abortmsg . "Unknown argument: -" . $key . "\n";
			next;
		}
		my $def = substr($arg_def{ $key },0,1);
		my $qkey = substr( $arg_def{ $key }, 1 );
		if ( exists $querymap{$qkey} ) {
			if ( $def eq "+" ) {
				$querymap{$qkey} = $querymap{$qkey} ."," . $ARGV[$i+1];
			} elsif ( $def eq "*" ) {
				if ( exists $cols{$ARGV[$i+1]} ) {
					$abortmsg = $abortmsg . "-" . $key . " " . $ARGV[$i+1] . " is repeated\n";
				} else {
					$cols{$ARGV[$i+1]} = "T";
				}
			} else {
				$abortmsg = $abortmsg . "-" . $key . " is repeated (" . $def . $qkey . ") '" . $arg_def{ $key } . "'\n";
			}
		} elsif ( $def eq "*" ) {
			$querymap{$qkey} = 1;
			$cols{$ARGV[$i+1]} = "T";
		} else {
			$querymap{$qkey} = $ARGV[$i+1];
		}
	} else {
		$abortmsg = $abortmsg .  "Argument #" . ($i + 1) . " missing leading -\n";
	}
}

# Now check for presence of required arguments
while (($key,$spec) = each ( %arg_def )) {
	if ( substr($key,0,4) eq "HASH" ) {
		next;
	} 
	my $a = substr( $spec, 0, 1 );
	my $b = substr( $spec, 1 );
	if ( $a eq "!" && ! exists $querymap{ $b } ) {
		$abortmsg = $abortmsg .  "-". $key . " is a required argument\n";
	}
}

# Validate -output value; handle default
if ( exists $querymap{ "o" } ) {
	my $o = $querymap{ "o" };
	if ( $o eq "csv" ) {
		#$querymap{ "o" } = "xml";
	} elsif ( $o eq "binary" ) {
		$querymap{ "o" } = "seed";
	} else {
		$abortmsg = $abortmsg . "-output must be csv or binary\n";
	}
} else {
	$querymap{"o"} = "csv";
}

die $abortmsg unless $abortmsg eq "";

# Handle default for -timezone
if ( ! exists $querymap{ "tz" } ) {
	$querymap{"tz"} = "UTC";
}

# Handle columns
foreach $key (keys %cols) {
	$querymap{ $key . ".0" } = "T";
}

# Build the string of query arguments
my $csv_request_args = "&chCnt.0=1";
my $vkey;
while (($key,$vkey) = each ( %querymap ) ) {
	$csv_request_args = $csv_request_args . "&" . $key . "=" . uri_escape( $vkey );
}

# Now we can build the whole URL
my $csv_request_url = $csv_request_path . "?a=rawData" . $csv_reqtype . $csv_request_args;

# Make request of server; should get back XML containing data file's URL
use LWP;
my $browser = LWP::UserAgent->new;

my $response = $browser->get( $csv_request_url );
die "Can't get $url -- ", $response->status_line
	unless $response->is_success;
die "Was expecting XML; got ", $response->content_type
	unless $response->content_type eq 'text/xml';

my $xml_with_csv_url = $response->content;

# Parse the XML and extract URL of CSV file
use XML::Simple;
$xml = new XML::Simple;
$parsed_xml = $xml->XMLin( $xml_with_csv_url );
$csv_url = $parsed_xml->{'rawData'}->{'url'};

# Now ask server for the csv file itself
$response = $browser->get( $csv_url );
die "Can't get $url -- ", $response->status_line
	unless $response->is_success;

my $type_expected = 'application/octet-stream';
die "Was expecting " . $type_expected . "; got ", $response->content_type
	unless $response->content_type eq $type_expected;

# Dump the csv file
print $response->content;

exit;