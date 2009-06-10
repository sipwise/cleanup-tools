#!/usr/bin/perl

use strict;
use warnings;
use DBI;

# backing up all data older than $MONTHS months
# Note: if set for example to 1, this will backup the overall month,
# not only the records older than 30 days. This logic applies generally,
# so it will go back $MONTHS months, and starting from there, backing up
# the full months back to $MONTHS_BACK
my $MONTHS = 3;
# from the starting month above, also go back $MONTHS_BACK months and
# backup them
my $MONTHS_BACK = 6;

# archive tables older than $ARCHIVE_MONTHS will be dumped to a file,
# gzipped and then dropped afterwards
my $ARCHIVE_MONTHS = 12;
# drop the dump files into $ARCHIVE_DIR
my $ARCHIVE_DIR = "/tmp";

# accounting database on proxies (openser) or db1 (accounting)
my $ACC_DB = "accounting";
# accounting tables on proxies (acc) or db1 (acc acc_backup acc_trash)
my @ACC_TABLES = qw(acc acc_backup acc_trash);
# cdr database (accounting)
my $CDR_DB = "accounting";
# cdr tables on proxies (empty) or db1 (cdr)
my @CDR_TABLES = qw(cdr);

# how many entries to move and delete at the same time
my $BATCH = 1000;

# DB access credentials
my $DBUSER = "root";
my $DBPASS = "1freibier!";
my $DBHOST = "localhost";

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

backup_table(\@ACC_TABLES, $ACC_DB, "time") or die();
backup_table(\@CDR_TABLES, $CDR_DB, "start_time") or die();

archive_dump(\@ACC_TABLES, $ACC_DB);
archive_dump(\@CDR_TABLES, $CDR_DB);
