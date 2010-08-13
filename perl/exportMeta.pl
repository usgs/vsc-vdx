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
my %arg_def = ("source"=>"!src", "channel"=>"!ch", "column"=>"?col", "rank"=>"?rk" );
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

die $abortmsg unless $abortmsg eq "";

# Build the string of query arguments
my $csv_request_args = "";
my $vkey;
while (($key,$vkey) = each ( %querymap ) ) {
	$csv_request_args = $csv_request_args . "&" . $key . "=" . uri_escape( $vkey );
}

# Now we can build the whole URL
my $csv_request_url = $csv_request_path . "?a=data&da=metadata" . $csv_request_args;

# Make request of server; should get back XML containing data file's URL
use LWP;
my $browser = LWP::UserAgent->new;

my $response = $browser->get( $csv_request_url );
die "Can't get $url -- ", $response->status_line
	unless $response->is_success;
	
if ( $response->content_type eq 'text/html' ) {
	die "Unexpected response; maybe VDX isn't running?";
} 
die "Was expecting XML; got ", $response->content_type
	unless $response->content_type eq 'text/xml';

# Parse the XML and extract the CSV file
use XML::Simple;
$xml = new XML::Simple;
$parsed_xml = $xml->XMLin( $response->content );

# Extract the lines into an array
my @lines;
if ( ref($parsed_xml->{'list-item'}) eq "ARRAY" ) {
	# Multiple items returned
	@lines = @{$parsed_xml->{'list-item'}};
} elsif ( $parsed_xml->{'list-item'} == null ) {
	# No items returned
	@lines = ();
} else {
	# Only 1 item returned
	@lines = ( $parsed_xml->{'list-item'} );
}

# Format & write the lines to output file
my $fn = ">meta_$querymap{'src'}.csv";
open (MYFILE, $fn);
foreach $e (@lines) {
	#print $e . "\n";
	my @qp = split(/"/, $e );
	my @cp = split(/,/, $qp[0]);
	print MYFILE "$cp[0], \"$qp[5]\". \"$qp[1]\", \"$qp[3]\"";
	if ( $#cp < 10 ) {
		print MYFILE "\n";
	} else {
		print MYFILE "\"$qp[7]\", \"$qp[9]\"\n";
	}
close(MYFILE);
}
exit;