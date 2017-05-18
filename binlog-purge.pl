#!/usr/bin/perl

use strict;
use warnings;
use POSIX;
use DBI;
use Sys::Syslog;

openlog("binglog-purge", "ndelay,pid", "daemon");
$SIG{__WARN__} = $SIG{__DIE__} = sub { ## no critic (Variables::RequireLocalizedPunctuationVars)
	syslog('warning', "@_");
};

my %config = map { $_ => undef } qw(dbuser dbpass dbremoteuser dbremotepass);

my $config_file = "/etc/ngcp-cleanup-tools/binlog-purge.conf";
open my $config_fh, $config_file or die "Program stopping, couldn't open the configuration file '$config_file'.\n";

while (<$config_fh>) {
    chomp;                  # no newline
    s/#.*//;                # no comments
    s/^\s+//;               # no leading white
    s/\s+$//;               # no trailing white
    next unless length;     # anything left?
    my ($var, $value) = split(/\s*=\s*/, $_, 2);
        $config{lc $var} = $value;
}
close $config_fh;


my (undef, $me)		= uname();
#This $me variable is used for matching the logfile name. It is valid for sip:carrier but not for sip:provider
# hack needed here
if ($me eq "sp1") {
        $me = "db1";
}
elsif ($me eq "sp2") {
        $me = "db2";
}
 

my @creds = ('dbi:mysql:', $config{dbuser}, $config{dbpass});
my @remotecreds = ('dbi:mysql:', $config{dbremoteuser}, $config{dbremotepass});


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
