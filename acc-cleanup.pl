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

use constant TIME_COL_TRANSFORMATION_NONE => {
		value => sub {
			my ($value,@args) = @_;
			return $value;
		},
		sql_rhs =>	sub {
			my ($col_name,@args) = @_;
			return sprintf('%s',$col_name);
		},
		sql_lhs =>	sub {
			my ($col_name,@args) = @_;
			return sprintf('%s',$col_name);
		},
	};
use constant TIME_COL_TRANSFORMATION_FROM_UNIXTIME => {
		value => sub {
			my ($value,@args) = @_;
			return $value;
		},
		sql_rhs =>	sub {
			my ($col_name,@args) = @_;
			return sprintf('unix_timestamp(%s)',$col_name);
		},
		sql_lhs =>	sub {
			my ($col_name,@args) = @_;
			return sprintf('from_unixtime(%s)',$col_name);
		},
	};

sub _get_time_column {
	my ($col,$transformation,$sql_rhs_args,$sql_lhs_args,$value_args) = (undef,undef);
	my $var_name = "time-column";
	my $col_name_re = '[a-z0-9_-]+';
	# from_unixtime(x):
	if ($vars{$var_name} =~ /^\s*from_unixtime\(\s*($col_name_re)\s*\)\s*$/i) {
		$col = $1;
		$transformation = TIME_COL_TRANSFORMATION_FROM_UNIXTIME;
		$sql_rhs_args = []; $sql_lhs_args = []; $value_args = [];
	# add other supported syntax here. eg.
	#  date(x)
	#  date_sub(x INTERVAL y day), etc.
	# raw column name:
	} elsif ($vars{$var_name} =~ /^\s*($col_name_re)\s*$/i) {
		$col = $1;
		$transformation = TIME_COL_TRANSFORMATION_NONE;
		$sql_rhs_args = []; $sql_lhs_args = []; $value_args = [];
	} else {
		die("Variable $var_name must show a column name, or an expression to convert to datetime (supported: 'from_unixtime(column name)')");
	}
	return {
		expression => $vars{$var_name},
		column_name => $col,
		transformation => $transformation,
		value_args => $value_args,
		sql_rhs_args => $sql_rhs_args,
		sql_lhs_args => $sql_lhs_args,
	};
}

sub _get_transformed_sql_rhs {
	my ($col,$literal) = @_;
	return &{$col->{transformation}->{sql_rhs}}($literal,@{$col->{sql_rhs_args}});
}

sub _get_transformed_sql_lhs {
	my ($col,$literal) = @_;
	return &{$col->{transformation}->{sql_lhs}}($literal,@{$col->{sql_lhs_args}});
}

sub _get_transformed_value {
	my ($col,$value) = @_;
	return &{$col->{transformation}->{value}}($value,@{$col->{value_args}});
}

sub _connect {

	my ($db) = @_;
	my $dbi = "dbi:mysql:$db";
	$vars{host} and $dbi .= ";host=$vars{host}";

	$dbh = DBI->connect($dbi, $vars{username}, $vars{password});
	$dbh or die("Failed to connect to DB $db ($vars{host}): " . $DBI::errstr);

	$dbh->{private_db} = $db;

}

sub _delete_loop {

	my ($table, $col, $start, $stop) = @_;
	my $stmt = "delete from $table where ";

	my @params = ();
	$stmt .= "$col->{column_name} >= " . _get_transformed_sql_rhs($col,'?');
	push(@params,_get_transformed_value($col,$start));
	$stmt .= " and $col->{column_name} < " . _get_transformed_sql_rhs($col,'?');
	push(@params,_get_transformed_value($col,$stop));

	$stmt .= " limit $vars{batch}" if $vars{batch} and $vars{batch} > 0;

	my $total = 0;
	while (1) {
		my $size = $dbh->do($stmt,undef, @params)
			or die("Unable to delete records from $table: " . $DBI::errstr);
		$total += $size;
		last unless $size > 0;
	}
	return $total;

}

sub _move_loop {

	my ($table, $mtable, $col, $start, $stop) = @_;

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

	my $temp_table = $table . "_tmp";
	my $stmt = "select $primary_key_cols from $table where ";

	my @params = ();
	$stmt .= "$col->{column_name} >= " . _get_transformed_sql_rhs($col,'?');
	push(@params,_get_transformed_value($col,$start));
	$stmt .= " and $col->{column_name} < " . _get_transformed_sql_rhs($col,'?');
	push(@params,_get_transformed_value($col,$stop));

	$stmt .= " limit $vars{batch}" if $vars{batch} and $vars{batch} > 0;

	my $total = 0;
	while (1) {
		my $size = $dbh->do("create temporary table $temp_table as ($stmt)",undef, @params)
			or die("Failed to create temporary table $temp_table: " . $DBI::errstr);
		if ($size > 0) {
			$dbh->do("insert ignore into $mtable select s.* from ".
				"$table as s inner join $temp_table as t using ($primary_key_cols)")
				or die("Failed to insert into monthly table $mtable: " . $DBI::errstr);
			$dbh->do("delete d.* from $table as d inner join $temp_table as t using ($primary_key_cols)")
				or die("Failed to delete records out of $table: " . $DBI::errstr);
		}
		$dbh->do("drop temporary table $temp_table")
			or die("Failed to drop temporary table $temp_table: " . $DBI::errstr);
		$total += $size;
		last unless $size > 0;
	}
	return $total;

}

sub archive_dump {
	my ($table) = @_;

	my $month = $vars{"archive-months"};
	my @now = localtime(time());
	while (1) {
		my ($m,$y) = _add_months($now[4] + 1, $now[5] + 1900, -1 * $month);
		my $mtable = $table . "_" . sprintf('%04i%02i', $y, $m);
		my $res = $dbh->selectcol_arrayref("show table status like ?", undef, $mtable);
		($res && @$res && $res->[0]) or last;
		$dbh->disconnect;
		#print "archiving $mtable\n";
		$month++;
		if ($vars{"archive-target"} ne '/dev/null') {
			my $target = $vars{"archive-target"} . "/$mtable." . sprintf('%04i%02i%02i%02i%02i%02i', $now[5] + 1900, $now[4] + 1, @now[3,2,1,0]) . ".sql";

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
		_connect($dbh->{private_db});
		$dbh->do("drop table $mtable");
	}
}

sub backup_table {
	my ($table) = @_;

	my $col = _get_time_column();

	my @range = ();
	my @now = localtime(time());
	foreach my $cmonth (($vars{"backup-retro"} - 1,0)) {
		my $tmonths = $cmonth + $vars{"backup-months"};
		my ($m,$y) = _add_months($now[4] + 1, $now[5] + 1900, -1 * $tmonths);
		push(@range,[$y,$m,_days_of_month($m,$y)]);
	}
	my $date = sprintf('%04i-%02i-01', $range[0][0], $range[0][1]);
	my $stop = sprintf('%04i-%02i-%02i', $range[1][0], $range[1][1], ((not $vars{"daily"} or $now[3] > $range[1][2]) ? $range[1][2] : $now[3]));
	my $next;
	my $time = '00:00:00';
	my %counts = ();
	while (($date cmp $stop) <= 0) {
		my ($y,$m,$d) = _split_date($date);
		my $mtable = $table . "_$y$m";
		$dbh->do("create table if not exists $mtable like $table");
		$next = _add_days($date,1);
		my $t = time();
		my $total = _move_loop($table, $mtable, $col, "$date $time", "$next $time");
		print "$date $time - $next $time: $total records moved from $table to $mtable (" . (time() - $t) . " secs)\n";
		$counts{$mtable} = 0 unless exists $counts{$mtable};
		$counts{$mtable} += $total;
	} continue {
		$date = $next;
	}

	foreach my $mtable (keys %counts) {
		unless ($counts{$mtable} > 0) {
			$dbh->do("drop table $mtable");
		}
	}

	return 1;
}

sub cleanup {
	my ($table) = @_;

	my $col = _get_time_column();

	my $sth = $dbh->prepare('select ' . _get_transformed_sql_lhs($col,"min($col->{column_name})") . " from $table");
	$sth->execute;
	my ($min_time) = $sth->fetchrow_array();
	$sth->finish;

	return unless $min_time;

	(my $date,undef) = _split_datetime($min_time);
	my $t = time() - int(86400 * $vars{"cleanup-days"});
	my @t = localtime($t);
	my $stop = sprintf('%04i-%02i-%02i', $t[5] + 1900, $t[4] + 1, $t[3]);
	my $stop_time = sprintf('%02i:%02i:%02i', $t[2], $t[1], $t[0]);

	my $next;
	my $time = '00:00:00';
	my $total;
	while (($date cmp $stop) < 0) {
		$next = _add_days($date,1);
		$t = time();
		$total = _delete_loop($table, $col, "$date $time", "$next $time");
		print "$date $time - $next $time: $total records deleted from $table (" . (time() - $t) . " secs)\n";
	} continue {
		$date = $next;
	}

	$t = time();
	$total = _delete_loop($table, $col, "$date $time", "$date $stop_time");
	print "$date $time - $date $stop_time: $total records deleted from $table (" . (time() - $t) . " secs)\n";

}

# date manip helper methods. todo: port to DateTime.

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

sub _add_months {

	my ($month, $year, $ads) = @_;

	if ($month > 0  and $month <= 12) {

		my $sign = ($ads > 0) ? 1 : -1;
		my $rmonths = $month + $sign * (abs($ads) % 12);
		my $ryears = $year + int( $ads / 12 );

		if ($rmonths < 1) {
			$rmonths += 12;
			$ryears -= 1;
		} elsif ($rmonths > 12) {
			$rmonths -= 12;
			$ryears += 1;
		}

		return ($rmonths,$ryears);

	} else {

		return (undef,undef);

	}

}

sub _split_date {

	my $datestring = shift;
	return split /-/,$datestring,3;

}

sub _split_time {

	my $timestring = shift;
	return split /:/,$timestring,3;

}

sub _split_datetime {

	my $timestampstring = shift;
	return split / /,$timestampstring,2;

}

sub _split_timestamp {

	my $timestampstring = shift;
	my ($date,$time) = _split_datetime($timestampstring);
	my ($year,$month,$day) = _split_date($date);
	my ($hour,$minute,$second) = _split_time($time);
	return ($year,$month,$day,$hour,$minute,$second);

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

	_connect($db);

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
