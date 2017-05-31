#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use Sys::Syslog;
use Config::Any;

openlog("acc-cleanup", "ndelay,pid", "daemon");
$SIG{__WARN__} = $SIG{__DIE__} = sub { ## no critic (Variables::RequireLocalizedPunctuationVars)
	syslog('warning', "@_");
};

my $config_file = "/etc/ngcp-cleanup-tools/acc-cleanup.conf";
my $config_yml = "/etc/ngcp-config/config.yml";

########################################################################

my (%vars, $dbh);

sub delete_loop {
	my ($table, $mtable, $col, $mstart) = @_;

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
				"where $col >= ? and $col < date_add(?, interval 1 month) $limit)",undef, $mstart, $mstart)
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

	for my $cmonth (0 .. ($vars{"backup-retro"} - 1)) {
		my $tmonths = $cmonth + $vars{"backup-months"};
		my $bt = time() - int(30.4375 * 86400 * $tmonths);
		my @bt = localtime($bt);
		my $tstampl = sprintf('%04i-%02i', $bt[5] + 1900, $bt[4] + 1);
		my $tstamp = sprintf('%04i%02i', $bt[5] + 1900, $bt[4] + 1);
		my $mstart = "$tstampl-01 00:00:00";

		my $mtable = $table . "_$tstamp";
		$dbh->do("create table if not exists $mtable like $table");
		delete_loop($table, $mtable, $vars{"time-column"}, $mstart);
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

########################################################################

my %cmds;

$cmds{unset} = sub {
	my ($var) = @_;

	$var or die("Syntax error in unset command");

	delete($vars{$var});
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
			die "Program stopping, maintenance mode enabled.\n";
		}
		$vars{$1} = $2;
		next;
	}

	my ($cmd, $arg) = $line =~ /^([\w-]+)(?:\s+(.*?))?$/;
	$cmd or die("Syntax error in config file: '$line'");

	my $sub = $cmds{$cmd};
	$sub or die("Unrecognized statement '$cmd'");

	my @args;
	$arg and @args = split(/\s+/, $arg);

	push(@deferred,{ 'sub' => $sub, 'context' => { %vars }, 'arg' => $arg, 'args' => \@args });
}

close $config_fh;

foreach my $cmd (@deferred) {
	%vars = %{$cmd->{'context'}};
	$cmd->{'sub'}->($cmd->{'arg'}, $cmd->{'args'});
}

