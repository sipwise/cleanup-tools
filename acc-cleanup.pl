#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Sys::Syslog;

openlog("acc-cleanup", "ndelay,pid", "daemon");
$SIG{__WARN__} = $SIG{__DIE__} = sub { ## no critic (Variables::RequireLocalizedPunctuationVars)
	syslog('warning', "@_");
};

my $config_file = "/etc/ngcp-cleanup-tools/acc-cleanup.conf";

########################################################################

my (%vars, $dbh);

#   time-column = start_time
#   backup-months = 7
#   backup-retro = 3
#   backup cdr
$vars{'backup-months'} = 7;
$vars{'backup-retro'} = 3;
backup_table('cdr');

sub move_loop {
	my ($table, $mtable, $col, $start, $stop) = @_;

	my $limit = '';
	$vars{batch} and $vars{batch} > 0 and $limit = " limit $vars{batch}";

	my $sth = $dbh->prepare("show fields from $table");
	$sth->execute;
	my $fieldinfo = $sth->fetchall_hashref('Field');
	$sth->finish;
	my @keycols = ();
	foreach my $fieldname (keys %$fieldinfo) {
		if (uc($fieldinfo->{$fieldname}->{'Key'}) eq 'PRI') {
			push @keycols,$fieldname;
		}
	}

	die("No primary key columns for table $table") unless @keycols;

	my $primary_key_cols = join(",",@keycols);

	#$mstart = '2016-12-01 00:00:00';

	while (1) {
		my $temp_table = $table . "_tmp";
		my $size = $dbh->do("create temporary table $temp_table as ".
		        "(select $primary_key_cols from $table " .
				#"where $col >= ? and $col < date_add(?, interval 1 month) $limit)",undef, $mstart, $mstart)
				"where $col >= ? and $col < ? $limit)",undef, $start, $stop)
			or die("Failed to create temporary table $temp_table: " . $DBI::errstr);
		if ($size > 0) {
			$dbh->do("insert into $mtable select s.* from ".
				"$table as s inner join $temp_table as t using ($primary_key_cols)")
				or die("Failed to insert into monthly table $mtable: " . $DBI::errstr);
			$dbh->do("delete d.* from $table as d inner join $temp_table as t using ($primary_key_cols)")
				or die("Failed to delete records out of $table: " . $DBI::errstr);
		}
		$dbh->do("drop temporary table $temp_table")
			or die("Failed to drop temporary table $temp_table: " . $DBI::errstr);
		last unless $size > 0;
	}
}

sub archive_dump {
	my ($table) = @_;

	my $month = $vars{"archive-months"};
	while (1) {
		my $now = time();
		my $bt = $now - int(30.4375 * 86400 * $month);
		my @bt = localtime($bt);
		my $mtable = $table . "_" . sprintf('%04i%02i', $bt[5] + 1900, $bt[4] + 1);
		my $res = $dbh->selectcol_arrayref("show table status like ?", undef, $mtable);
		($res && @$res && $res->[0]) or last;
		$month++;
		if ($vars{"archive-target"} ne '/dev/null') {
			my $target = $vars{"archive-target"} . "/$mtable." . sprintf('%04i%02i%02i%02i%02i%02i', $bt[5] + 1900, $bt[4] + 1, @bt[3,2,1,0]) . ".sql";

			my @cmd = ('mysqldump');
			$vars{username} and push(@cmd, "-u" . $vars{username});
			$vars{password} and push(@cmd, "-p" . $vars{password});
			$vars{host} and push(@cmd, "-h" . $vars{host});
			push(@cmd, "--opt", $dbh->{private_db}, $mtable);

			for (@cmd) { s/'/'"'"'/g; $_ = "'$_'" }
			my $cmd = join(' ', @cmd);

			if (system("$cmd > $target")) {
				unlink($target);
				die("MySQL DUMP of table $mtable into file $target failed\n");
			}
			if ($vars{compress} && $vars{compress} eq 'gzip') {
				if (system("nice gzip -9 $target")) {
					unlink($target, "$target.gz");
					die("Gzipping of dump file $target failed\n");
				}
			}
		}
		$dbh->do("drop table $mtable");
	}
}

sub backup_table {
	my ($table) = @_;

	#for my $cmonth (0 .. ($vars{"backup-retro"} - 1)) {
	#	my $tmonths = $cmonth + $vars{"backup-months"};
	#	my $bt = time() - int(30.4375 * 86400 * $tmonths);
	#	my @bt = localtime($bt);
	#	my $tstampl = sprintf('%04i-%02i', $bt[5] + 1900, $bt[4] + 1);
	#	my $tstamp = sprintf('%04i%02i', $bt[5] + 1900, $bt[4] + 1);
	#	my $mstart = "$tstampl-01 00:00:00";
	#	my $date = sprintf('%04i-%02i-%02i', $bt[5] + 1900, $bt[4] + 1, $bt[3]);
	#
	#	my $mtable = $table . "_$tstamp";
	#	$dbh->do("create table if not exists $mtable like $table");
	#	move_loop($table, $mtable, $vars{"time-column"}, $mstart, $date);
	#}

	my @range = ();
	foreach my $cmonth (($vars{"backup-retro"} - 1,0)) {
		my $tmonths = $cmonth + $vars{"backup-months"};
		my $t = time() - int(30.4375 * 86400 * $tmonths);
		push(@range,[localtime($t)]);
	}
	my $date = sprintf('%04i-%02i-01', $range[0][5] + 1900, $range[0][4] + 1);
	my $stop = sprintf('%04i-%02i-%02i', $range[1][5] + 1900, $range[1][4] + 1,
		_days_of_month($range[1][4] + 1, $range[1][5] + 1900));
	my $next;
	while (($date cmp $stop) <= 0) {
		my ($y,$m,$d) = _split_date($date);
		my $mtable = $table . "_$y$m";
		#$dbh->do("create table if not exists $mtable like $table");
		$next = _add_days($date,1);
		print "$date 00:00:00 - $next 00:00:00\n";
		#move_loop($table, $mtable, $vars{"time-column"}, "$date 00:00:00", "$next 00:00:00");
	} continue {
		$date = $next;
	}

	return 1;
}

sub cleanup {
	my ($table) = @_;

	my $limit = '';
	$vars{batch} and $vars{batch} > 0 and $limit = " limit $vars{batch}";
	my $col = $vars{"time-column"};

	while (1) {
		my $aff = $dbh->do("delete from $table where $col < date(date_sub(now(), interval ? day)) $limit",
			undef, $vars{"cleanup-days"});
		$aff or die("Unable to delete records from $table");
		$aff == 0 and last;
	}
}

sub _add_days {

	my ($date,$ads) = @_;

	my ($year,$month,$day) = _split_date($date);

	my $rday = $day;
	my $rmonth = $month;
	my $ryear = $year;

	my $result;

	if($ads >= 0) { # addition
		for (1 .. $ads) {
			# increment day, turn month forward:
			if ($rday < _days_of_month($rmonth,$ryear)) {
				$rday++;
			} else {
				$rmonth++;
				$rday = 1;
			}
			# turn year forward:
			if ($rmonth > 12) {
				$rday = 1;
				$rmonth = 1;
				$ryear++;
			}
		}
	} else { # difference
		my $subs = -1 * $ads;
		for (1 .. $subs) {
			# decrement day, turn month backward
			if ($rday > 1) {
				$rday--;
			} else {
				$rmonth--;
				$rday = _days_of_month($rmonth,$ryear);
			}
			# turn year backward:
			if ($rmonth < 1) {
				$rmonth = 12;
				$rday = _days_of_month($rmonth,$ryear);
				$ryear--;
			}
		}
	}

	return $ryear . '-' . _zerofill($rmonth,2) . '-' . _zerofill($rday,2);

}

sub _split_date {

	my $datestring = shift;
	return split /-/,$datestring,3;

}

sub _zerofill {
	my ($i,$d) = @_;
	my $z = $d - length($i);
	my $res = $i;
	if ($d > 0) {
		foreach (1 .. $z) {
			$res = '0' . $res;
		}
	}
	return $res;
}

sub _days_of_month {

	my ($month, $year) = @_;
	if ($month > 0  and $month <= 12) {
		if ($month == 2 and _is_leapyear($year)) { # leapyear
			return 29;
		} else {
			my @daysofmonths = (31,28,31,30,31,30,31,31,30,31,30,31);
			return $daysofmonths[$month - 1];
		}
	} else {
		return 0;
	}

}

sub _is_leapyear {

	my $year = shift;
	my $v = 0;
	if (!$year) {
		return -1;
	}
	if ($year % 4 == 0) {
		$v = 1;
	}
	if ($year % 100 == 0) {
		$v = 0;
	}
	if ($year % 400 == 0) {
		$v = 1;
	}
	return $v;

}

########################################################################

my %cmds;

$cmds{unset} = sub {
	my ($var) = @_;

	$var or die("Syntax error in unset command");

	delete($vars{$var});
};

$cmds{set} = sub {
	my ($var,$val) = @_;

	$var or die("Syntax error in set command");
	$val = $val->[1] if 'ARRAY' eq ref $val;

	$vars{$var} = $val;
};

$cmds{connect} = sub {
	my ($db) = @_;

	undef($dbh);

	$db or die("Missing DB name for connect command");

	my $dbi = "dbi:mysql:$db";
	$vars{host} and $dbi .= ";host=$vars{host}";

	$dbh = DBI->connect($dbi, $vars{username}, $vars{password});
	$dbh or die("Failed to connect to DB $db ($vars{host}): " . $DBI::errstr);

	$dbh->{private_db} = $db;
};

$cmds{backup} = sub {
	my ($table) = @_;

	$table or die("No table name given in backup command");
	$dbh or die("Not connected to a DB in backup command");
	$vars{"time-column"} or die("Variable time-column not set in backup command");
	$vars{"backup-months"} or die("Variable backup-months not set in backup command");
	$vars{"backup-retro"} or die("Variable backup-retro not set in backup command");

	backup_table($table);
};

$cmds{archive} = sub {
	my ($table) = @_;

	$table or die("No table name given in archive command");
	$dbh or die("Not connected to a DB in archive command");
	$vars{"archive-months"} or die("Variable archive-months not set in archive command");
	$vars{"archive-target"} or die("Variable archive-target not set in archive command");

	archive_dump($table);
};

$cmds{cleanup} = sub {
	my ($table) = @_;

	$table or die("No table name given in backup command");
	$dbh or die("Not connected to a DB in backup command");
	$vars{"time-column"} or die("Variable time-column not set in cleanup command");
	$vars{"cleanup-days"} or die("Variable cleanup-days not set in cleanup command");

	cleanup($table);
};

open my $config_fh, '<', $config_file or die "Program stopping, couldn't open the configuration file '$config_file'.\n";

my @deferred = ();

while (my $line = <$config_fh>) {
	$line =~ s/^\s*//s;
	$line =~ s/\s*$//s;

	$line =~ /^#/ and next;
	$line =~ /^$/ and next;

	if ($line =~ /^([\w-]+)\s*=\s*(\S*)$/) {
		if (lc($1) eq 'maintenance' and $2 eq 'yes') {
		    @deferred = ();
			last;
		}
		push(@deferred,{ 'sub' => $cmds{set}, 'arg' => $1, 'args' => [ $1, $2 ] });
		next;
	}

	my ($cmd, $arg) = $line =~ /^([\w-]+)(?:\s+(.*?))?$/;
	$cmd or die("Syntax error in config file: '$line'");

	my $sub = $cmds{$cmd};
	$sub or die("Unrecognized statement '$cmd'");

	my @args;
	$arg and @args = split(/\s+/, $arg);

	push(@deferred,{ 'sub' => $sub, 'arg' => $arg, 'args' => \@args });
}

close $config_fh;

foreach my $cmd (@deferred) {
	$cmd->{'sub'}->($cmd->{'arg'}, $cmd->{'args'});
}
