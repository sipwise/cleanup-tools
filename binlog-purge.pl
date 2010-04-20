#!/usr/bin/perl

use strict;
use warnings;
use POSIX;
use DBI;

our $DBUSER;
our $DBPASS;
our $DBREMOTEUSER;
our $DBREMOTEPASS;;


my $config_file = "/etc/cleanup-tools.conf";
open CONFIG, "$config_file" or die "Program stopping, couldn't open the configuration file '$config_file'.\n";

no strict 'refs';
while (<CONFIG>) {
    chomp;                  # no newline
    s/#.*//;                # no comments
    s/^\s+//;               # no leading white
    s/\s+$//;               # no trailing white
    next unless length;     # anything left?
    my ($var, $value) = split(/\s*=\s*/, $_, 2);
        $$var = $value;
}
use strict 'refs';
close CONFIG;


my (undef, $me)		= uname();
my @creds = ('dbi:mysql:', $DBUSER, $DBPASS);
my @remotecreds = ('dbi:mysql:', $DBREMOTEUSER, $DBREMOTEPASS);


my $dbh = DBI->connect(@creds) or die;

my $mast = $dbh->selectrow_hashref("show master status");
$mast or die;
$mast->{File} =~ /^\Q$me\E-bin\./ or die;

my $slaves = 0;
my @logs;
my $proc = $dbh->selectall_arrayref("show processlist", {Slice => {}});
for my $p (@$proc) {
	$p->{Command} eq "Binlog Dump" or next;
	($p->{Host} && $p->{Host} =~ /^([\d.]+):/) or next;
	my $ip = $1;

	my @dsn = @remotecreds;
	$dsn[0] .= ";host=$ip";
	my $slave = DBI->connect(@dsn) or die;
	my $st = $slave->selectrow_hashref("show slave status");
	$st or die;
	push(@logs, $st->{Relay_Master_Log_File});
	push(@logs, $st->{Master_Log_File});
}

my @min;
for my $log (@logs) {
	$log =~ /^\Q$me\E-bin\.(\d+)$/ or next;
	my $num = $1 + 0;
	(!@min || $min[0] > $num) and @min = ($num, $log);
}

@min or exit();

my $bins = $dbh->selectall_arrayref("show binary logs");
for my $bp (reverse(0 .. $#$bins)) {
	$min[1] eq $bins->[$bp]->[0] or next;
	my $ni = $bp - 2;
	$ni < 0 and last;
	my $ll = $bins->[$ni]->[0];
	$dbh->do("purge binary logs to '$ll'");
	last;
}
