#!/usr/bin/perl

use strict;
use warnings;
use POSIX;
use DBI;

my (undef, $me)		= uname();
my @creds		= qw(dbi:mysql: root 1freibier!);
my @remotecreds		= qw(dbi:mysql: replicator wait4Data);

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
