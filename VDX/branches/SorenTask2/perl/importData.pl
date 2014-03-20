#!/usr/bin/perl
use URI::Escape;
use File::Spec;

# Remove whitespace from the start and end of the string
sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

# The URL w/ request parameters
my $csv_request_path = "http://localhost:8080/valve3/valve3.jsp";

# Build the arguments string
my $argc = $#ARGV + 1;

die "Wrong number of arguments: " . $argc  unless $argc == 1;

my $path = $ARGV[0];
($volume,$directories,$file) = File::Spec->splitpath( $path );

open FILE, $path or die $!;

# Read in comments, store in reverse order
my @cmts = [];
my $line;
my $timezone;
while (<FILE>) {
	$line = trim($_);
	if ( substr( $line, 0, 1 ) eq "#" ) {
		push( @cmts, trim(substr( $line, 1 )) );
	} else {
		my $i = $#cmts;
		for ( $i--; $i > 0; $i-- ) {
			print $cmts[$i] . "\n";
		}
		# $line should be the header line
		my @headers = split(',', $line);
		$timezone = $headers[1];
		my @parts = split(/\(/,$timezone);
		@parts  = split(/\)/, $parts[1]);
		$timezone = $parts[0];
		last;
	}
}

print "Timezone = " . $timezone . "\n";



exit;

my $abortmsg = "";
my $key;
my $vkey;
my $querymap = {};
my @def;

my %arg_def = ("source"=>"!src.0", "channel"=>"!chName.0", "start"=>"!st.0", 
	"end"=>"?et.0", "timezone"=>"?tz", 
	"datatype"=>"+dt", "column"=>"+colName", "rank"=>"+rkName" );

for ( $i=0; $i<$argc; $i += 2 ) {
	if ( substr( $ARGV[$i], 0, 1 ) eq "-" ) {
		$key = substr( $ARGV[$i], 1 );
		if ( exists $arg_def{$key} ) {
		} else {
			$abortmsg = $abortmsg . "Unknown argument: -" . $key . "\n";
			next;
		}
		$def = substr($arg_def{ $key },0,1);
		my $qkey = substr( $arg_def{ $key }, 1 );
		if ( exists $querymap{$qkey} ) {
			if ( $def eq "+" ) {
				$querymap{$qkey} = $querymap{$qkey} ."," . $ARGV[$i+1];
			} else {
				$abortmsg = $abortmsg . "-" . $key . " is repeated\n";
			}
		} else {
			$querymap{$qkey} = $ARGV[$i+1];
		}
	} else {
		$abortmsg = $abortmsg .  "Argument #" . ($i + 1) . " missing leading -\n";
	}
}

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

if ( exists $querymap{ "o" } ) {
	my $o = $querymap{ "o" };
	if ( $o eq "csv" ) {
		$querymap{ "o" } = "xml";
	} elsif ( $o eq "binary" ) {
		$querymap{ "o" } = "bin";
	} else {
		$abortmsg = $abortmsg . "-output must be csv or binary\n";
	}
} else {
	$querymap{"o"} = "csv";
}

while (($key,$vkey) = each ( %querymap )) {
	print "argmap[" . $key . "] = " . $vkey . "\n";
}

die $abortmsg unless $abortmsg eq "";

my $csv_request_args = "&chCnt.0=1";
while (($key,$vkey) = each ( %querymap ) ) {
	$csv_request_args = $csv_request_args . "&" . $key . "=" . uri_escape( $vkey );
}

# Now we can build the URL
my $csv_request_url = $csv_request_path . "?a=rawData" . $csv_reqtype . $csv_request_args;

print $csv_request_url . "\n";

exit;

# Make request of server; should get back XML containing data file's URL
use LWP
my $browser = LWP::UserAgent->new;

my $response = $browser->get( $csv_request_url );
die "Can't get $url -- ", $response->status_line
	unless $response->is_success;

die "Hey, I was expecting HTML, not ", $response->content_type
	unless $response->content_type eq 'text/html';
	# or whatever content-type you're equipped to deal with

# Parse the XML and extract URL of CSV file
use XML::Simple;
$xml = new XML::Simple;
$parsed_xml = $xml->XMLin( $xml_with_csv_url );
$csv_url = $parsed_xml->{'rawData'}->{'url'};

# Now ask server for the csv file itself
$csv_data = get $csv_url;
die "Download of CSV file failed" unless defined $csv_data;

# Dump the csv file
print $csv_data;

exit;