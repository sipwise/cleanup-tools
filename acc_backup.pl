#!/usr/bin/perl

use strict;
use warnings;
use DBI;

our $MONTHS;
our $MONTHS_BACK;
our $ARCHIVE_MONTHS;
our $ARCHIVE_DIR;
our $ACC_DB;
our $ACC_TABLES;
our $CDR_DB;
our $CDR_TABLES;
our $BATCH;
our $DBUSER;
our $DBPASS;
our $DBHOST;


my $config_file = "/etc/ngcp-cleanup-tools/acc_backup.conf";
open CONFIG, "$config_file" or die "Program stopping, couldn't open the configuration file '$config_file'.\n";

while (<CONFIG>) {
    chomp;                  # no newline
    s/#.*//;                # no comments
    s/^\s+//;               # no leading white
    s/\s+$//;               # no trailing white
    next unless length;     # anything left?
    my ($var, $value) = split(/\s*=\s*/, $_, 2);
	no strict 'refs';
	$$var = $value;
} 
close CONFIG;

my @ACC_TABLE_ARR = split(",",$ACC_TABLES);
my @CDR_TABLE_ARR = split(",",$CDR_TABLES);

########################################################################

sub delete_loop {
	my ($dbh, $table, $mtable, $col, $mstart) = @_;

	while (1) {
		my $res = $dbh->selectcol_arrayref("select id from $table
				where $col >= ?
				and $col < date_add(?, interval 1 month) limit $BATCH",
				undef, $mstart, $mstart);

		$res or last;
		@$res or last;

		my $idlist = join(",", @$res);
		$dbh->do("insert into $mtable select * from $table where id in ($idlist)")
			or last;
		$dbh->do("delete from $table where id in ($idlist)");
	}
}

sub archive_dump {
	my ($tables, $db) = @_;

	my $dbh = DBI->connect("dbi:mysql:$db;host=$DBHOST", $DBUSER, $DBPASS);
	$dbh or return 0;

	for my $table (@$tables) {
		my $month = $ARCHIVE_MONTHS;
		while (1) {
			my $now = time();
			my $bt = $now - int(30.4375 * 86400 * $month);
			my @bt = localtime($bt);
			my $mtable = $table . "_" . sprintf('%04i%02i', $bt[5] + 1900, $bt[4] + 1);
			my $res = $dbh->selectcol_arrayref("show table status like ?", undef, $mtable);
			($res && @$res && $res->[0]) or last;
			$month++;
			my $target = "$ARCHIVE_DIR/$mtable." . sprintf('%04i%02i%02i%02i%02i%02i', $bt[5] + 1900, $bt[4] + 1, @bt[3,2,1,0]) . ".sql";
			if (system("mysqldump -u$DBUSER -p$DBPASS -h$DBHOST --opt $db $mtable > $target")) {
				print STDERR ("MySQL DUMP of table $mtable into file $target failed\n");
				next;
			}
			if (system("nice gzip -9 $target")) {
				print STDERR ("Gzipping of dump file $target failed\n");
				unlink($target, "$target.gz");
				next;
			}
			$dbh->do("drop table $mtable");
		}
	}

	return 1;
}

sub backup_table {
	my ($tables, $db, $col) = @_;

	my $dbh = DBI->connect("dbi:mysql:$db;host=$DBHOST", $DBUSER, $DBPASS);
	$dbh or return 0;

	for my $cmonth (0 .. ($MONTHS_BACK - 1)) {
		my $tmonths = $cmonth + $MONTHS;
		my $bt = time() - int(30.4375 * 86400 * $tmonths);
		my @bt = localtime($bt);
		my $tstampl = sprintf('%04i-%02i', $bt[5] + 1900, $bt[4] + 1);
		my $tstamp = sprintf('%04i%02i', $bt[5] + 1900, $bt[4] + 1);
		my $mstart = "$tstampl-01 00:00:00";

		for my $table (@$tables) {
			my $mtable = $table . "_$tstamp";

			$dbh->do("create table if not exists $mtable like $table");

			delete_loop($dbh, $table, $mtable, $col, $mstart);
		}
	}

	return 1;
}

########################################################################



backup_table(\@ACC_TABLE_ARR, $ACC_DB, "time") or die();
backup_table(\@CDR_TABLE_ARR, $CDR_DB, "start_time") or die();

archive_dump(\@ACC_TABLE_ARR, $ACC_DB);
archive_dump(\@CDR_TABLE_ARR, $CDR_DB);
