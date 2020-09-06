#!/usr/bin/perl -w
use strict;
use warnings;
use DBI;
use Time::Piece;
use File::Basename;
use Term::ProgressBar;
use Term::ANSIColor;

my $time = Time::Piece->new;
$|=1; # autoflush

# Get the ticker from Filename:   DAT_ASCII_AUDCAD_T_200905.csv
my @t_a = split /_/, $ARGV[0];
my $ticker = $t_a[2];

# Create table for ticker
my $dbh = DBI->connect("DBI:mysql:database=forex;host=server", "user", "pass");
my $sth = $dbh->prepare("create table $ticker (date datetime, bid float, ask float)");
$sth->execute();

# Enable table compress, requires "innodb_file_per_table=1" in my.cnf
#$sth = $dbh->prepare("ALTER TABLE $ticker engine=InnoDB row_format=compressed key_block_size=16");
#$sth->execute();

# Get list of files to parse
foreach my $file (@ARGV) {
        open(FH, "<$file") or die "Couldn't open file $!";

	# Get current date for file contents
	@t_a = split /_/, $file;
	my $fdate_t = $t_a[4];
	my $fdate_y = substr $fdate_t, 0, 4;
	my $fdate_m = substr $fdate_t, 4, 2;

	# Get count for progress bar
	my $fileCnt=0;
	while(<FH>) { $fileCnt++; }
	seek FH, 0, 0;  # Reset to beginning
	print color('bold blue');
	print "\nTotal entries: $fileCnt\n";
	my $progress = Term::ProgressBar->new ({count => $fileCnt});	

	# Read tick data for this file
	my $query = "insert into $ticker (date, bid, ask) values ";
	my $count = 0;
	$. = 0;
	print color('bold green');
        while(<FH>) {
		$count++;
                my @tick = split /,/, $_;
		my $bid = $tick[1];
		my $ask = $tick[2];
		$bid =~ s/^\s*(.*?)\s*$/$1/;
		$ask =~ s/^\s*(.*?)\s*$/$1/;
		my $date = Time::Piece->strptime($tick[0] =~ s/[0-9]{3}\z//r, "%Y%m%d %H%M%S")->strftime("%F %T");

		$query .= "(\"$date\", \"$bid\", \"$ask\"), ";

		if($count == 1000) {
			chop($query); # space
			chop($query); # ,

			my $sth = $dbh->prepare($query);
			$sth->execute() or die "Couldnt insert tick $DBI::errstr\n $query\n";
			$query = "insert into $ticker (date, bid, ask) values ";
			$count = 0;
			$progress->update($.);
		}		
		

        }
	if($query ne "insert into $ticker (date, bid, ask) values ") {  # DAT_ASCII_AUDNZD_T_200911.csv  when file is exactly % 1000  len
		chop($query);
		chop($query);
		$sth = $dbh->prepare($query);
		$sth->execute() or die "Couldnt insert tick $DBI::errstr\n$query\n";
		$progress->update($.);
	}
	
	# Do a count check to verify # of entries
	my $file_q  = "select count(*) from $ticker where date <= \"$fdate_y-$fdate_m-31 23:59:59\" and date >= \"$fdate_y-$fdate_m-01 00:00:00\"";
	$sth = $dbh->prepare($file_q);
	$sth->execute() or die "$DBI::errstr\n";
	my $ref = $sth->fetchrow_hashref();
	my $dbcount = $ref->{'count(*)'};

	print color('bold cyan');

	if($. != $dbcount) {
		print "Count is wrong: File! $.  DB Count:  $dbcount\n";
	
	} else {
		print "Count comparison was correct: $dbcount\n";
	}


	print basename($file) . ": Completed\n";
	print color('reset');
        close(FH);
	sleep(2);
}

$sth->finish();
$dbh->disconnect();
