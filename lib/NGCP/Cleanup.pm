package NGCP::Cleanup;

use strict;
use warnings;
use English;
use DBI;
use DateTime;
use Data::Dumper;
use Log::Log4perl;
use Log::Log4perl::Level;

my @queue = ();
my %env = ();
my %cmds = ();
my $log;

my $MAX_PARTITIONS = 24;

sub new {
    my $class  = shift;
    my (%args) = @_;
    my $self   = bless {}, $class;

    my $config_file = $args{config_file}
        or die "config_file argument is required\n";

    $self->init_config($config_file);
    $self->init_log();
    $self->init_cmds();

    local $OUTPUT_AUTOFLUSH = 1;

    return $self;
}

sub cmd {
    my ($self, $cmd) = @_;
    die "No command found $cmd" unless $cmds{$cmd};
    return $cmds{$cmd};
}

sub queue {
    my $self = shift;

    return \@queue;
}

sub env {
    my ($self, @val) = @_;
    return unless @val;
    if ($#val == 0) {
        return $env{$val[0]} // undef;
    }
    for (my $i = 0; $i < $#val; $i++) {
        next unless ($i % 2) == 0;
        $env{$val[$i]} = $val[$i+1];
    }

    return '';
}

sub logger {
    my ($self, $level, $str) = @_;

    my @caller = (caller 2);
    unless (@caller) {
        @caller = (caller 1);
    }
    my $caller = $caller[3];
    $caller = (split(/::/, $caller))[-1] // '';
    $log->log($level, $caller,': ',$str);

    return;
}

sub info  { shift->logger($INFO, shift); }
sub debug { shift->logger($DEBUG, shift); }
sub error { shift->logger($ERROR, shift); }

sub init_log {
    my $self = shift;

    my $debug = $self->env('debug') ? 'DEBUG' : 'INFO';

Log::Log4perl->init(\<<EOF);
log4perl.category.ngcp-cleanup=$debug, SYSLOG, SCREEN

log4perl.appender.SYSLOG=Log::Dispatch::Syslog
log4perl.appender.SYSLOG.facility=local0
log4perl.appender.SYSLOG.ident=ngcp-cleanup
log4perl.appender.SYSLOG.layout=PatternLayout
log4perl.appender.SYSLOG.layout.ConversionPattern=%-5p %m%n

log4perl.appender.SCREEN=Log::Log4perl::Appender::Screen
log4perl.appender.SCREEN.mode=append
log4perl.appender.SCREEN.layout=PatternLayout
log4perl.appender.SCREEN.layout.ConversionPattern=%-5p %m%n
EOF

    $log = Log::Log4perl->get_logger("ngcp-cleanup");
}

sub init_cmds {
    %cmds = (
        connect => sub {
            my ($self, $db) = @_;
            my $host = $self->env('host');
            my $user = $self->env('user');
            my $pass = $self->env('pass');
            my $dbi = "dbi:mysql:$db;host=$host";
            my $dbh = DBI->connect($dbi, $user, $pass,
                { PrintError => 0, mysql_auto_reconnect => 1 })
                or die "Failed to connect to DB $db ($host):".$DBI::errstr;
            $self->env(dbh => $dbh);
            $self->env(own_db => $db);
        },
        backup => sub {
            my ($self, $table) = @_;
            $table or die "No table name given in backup command";
            $self->env('dbh') or die "Not connected to a DB in backup command";
            if ($self->env('time-column')) {
                $self->env('time-column-mode' => 'time');
            } elsif ($self->env('timestamp-column')) {
                $self->env('time-column-mode' => 'timestamp');
                $self->env('time-column' => $self->env('timestamp-column'));
            }
            foreach my $v (qw(time-column time-column-mode
                                           backup-months backup-retro)) {
                $self->env($v)
                    or die "Variable '$v' not set in backup command";
            }
            $self->backup_table($table);
        },
        archive => sub {
            my ($self, $table) = @_;
            $table or die "No table name given in archive command";
            $self->env('dbh') or die "Not connected to a DB in archive command";
            foreach my $v (qw(archive-months archive-target)) {
                $self->env($v)
                    or die "Variable '$v' not set in archive command";
            }
            $self->archive_dump($table);
        },
        cleanup => sub {
            my ($self, $table) = @_;
            $table or die "No table name given in backup command";
            $self->env('dbh') or die "Not connected to a DB in archive command";
            foreach my $v (qw(time-column cleanup-days)) {
                $self->env($v)
                    or die "Variable '$v' not set in cleanup command";
            }
            $self->cleanup_table($table);
        },
    );
}

sub init_config {
    my ($self, $config_file) = @_;

    open(my $config_fh, '<', $config_file)
        or die "Couldn't open the configuration file '$config_file'.\n";

    while (my $line = <$config_fh>) {
        $line =~ s/^\s*//s;
        $line =~ s/\s*$//s;
        next if $line =~ /^#/;
        next if $line =~ /^$/;

        if ($line =~ /^([\w-]+)\s*=\s*(\S*)$/) {
            if (lc($1) eq 'maintenance' and $2 eq 'yes') {
                @queue = ();
                last;
            }
            $env{$1} = $2;
            next;
        }

        if ($line =~ /^([\w-]+)(?:\s+(.*?))?$/) {
            my ($cmd, $arg) = ($1, $2);
            push @queue, {cmd => $cmd, arg => $arg, env => {%env}};
        }
    }

    close $config_fh;
}

sub process {
    my $self = shift;

    foreach my $q (@{$self->queue}) {
        $self->env(%{$q->{env}});
        $self->cmd($q->{cmd})->($self, $q->{arg});
    }
}

sub fetch_row {
    my ($self, $db, $table, $cols, $where) = @_;
    my $dbh = $self->env('dbh');

    $db ||= $self->env('own_db');
    $table = $db.'.'.$table;
    $where ||= '';
    my @res = $dbh->selectrow_array(<<SQL);
SELECT $cols
  FROM $table
$where
SQL
    die "Cannot fetch row: ".$DBI::errstr if $DBI::err;

    return @res;
}

sub check_table_exists {
    my ($self, $table) = @_;

    my ($pvalue) = $self->fetch_row(
        'information_schema', 'tables', 'table_name',
        "where table_schema = 'accounting' and table_name = '$table'");

    return $pvalue;
}

sub check_partition_exists {
    my ($self, $table, $pname) = @_;

    my ($pvalue) = $self->fetch_row(
        'information_schema', 'partitions', 'partition_description',
        "where table_schema = 'accounting' and table_name = '$table' and partition_name = '$pname'");

    return $pvalue;
}

sub check_table_partitioned {
    my ($self, $table) = @_;

    my ($pvalue) = $self->fetch_row(
        'information_schema', 'partitions', 'partition_name',
        "where table_schema = 'accounting' and table_name = '$table' and partition_name IS NOT NULL limit 1");

    return $pvalue;
}

sub build_partitions_list {
    my ($self, $table, $mpart, $mdt, $gap) = @_;

    my @parts = ();
    for (my $i = $gap; $i >= 0; $i--) {
        last if $i ==0 && $mpart eq 'pmax';
        my $pname = $mdt->strftime('p%Y%m');
        my $ndt = $mdt->clone;
        $ndt->add(months => 1)->truncate(to => "month");
        my $nymd = $ndt->ymd('-');
        push @parts, sprintf(
                        "PARTITION %s VALUES LESS THAN(UNIX_TIMESTAMP('%s'))",
                        $pname, $nymd);
        $mdt->subtract(months => 1);
    }
    @parts = reverse @parts;
    if ($mpart eq 'pmax') {
        push @parts, sprintf("PARTITION %s VALUES LESS THAN(MAXVALUE)", $mpart);
    }

    return join(",\n", @parts);
}

sub reorganize_partitions {
    my ($self, $table, $mpart, $mdt, $gap) = @_;
    my $dbh = $self->env('dbh');

    my $parts = $self->build_partitions_list($table, $mpart, $mdt, $gap);
    my $sql = <<SQL;
ALTER TABLE $table REORGANIZE PARTITION $mpart INTO
(
$parts
);
SQL
    $self->debug($sql);
    $dbh->do($sql);
    die "Cannot reorganize partitions: ".$DBI::errstr if $DBI::err;

    return;
}

sub create_partitions {
    my ($self, $table, $mpart, $mdt, $gap) = @_;
    my $dbh = $self->env('dbh');
    my $col = $self->env('time-column');

    my $parts = $self->build_partitions_list($table, $mpart, $mdt, $gap);
    my $sql = <<SQL;
ALTER TABLE $table PARTITION BY RANGE(FLOOR($col))
(
$parts
);
SQL
    $self->debug($sql);
    $dbh->do($sql);
    die "Cannot create partitions: ".$DBI::errstr if $DBI::err;
}

sub drop_partition {
    my ($self, $table, $pname) = @_;
    my $dbh = $self->env('dbh');

    $dbh->do(<<SQL);
ALTER TABLE $table DROP PARTITION $pname
SQL
    $self->debug("drop partition=$pname from table=$table");
    die "Cannot drop partition $pname: ".$DBI::errstr if $DBI::err;
}

sub update_partitions {
    my ($self, $table) = @_;

    my $dbh = $self->env('dbh');
    my $db = $self->env('own_db');
    my $col = $self->env('timestamp-column');
    my $backup_months = $self->env('backup-months');
    my $backup_retro = $self->env('backup-retro');
    my @cols = ("min($col)", "max($col)");
    my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols));

    unless ($min_ts) {
        #$self->debug("table=$table: empty, nothing to partition");
        return unless $min_ts;
    }

    my $dt_min = DateTime->now(time_zone => 'local');
    $dt_min->subtract(months => $backup_months + $backup_retro-1)->truncate(to => 'month');
    my $dt_max = DateTime->now(time_zone => 'local', epoch => $max_ts);
    $dt_max->add(months => 1)->truncate(to => 'month'); # extra month
    my $months = ($dt_max - $dt_min)->months;

    $self->debug(sprintf "table=%s checking start=%s end=%s",
        $table, $dt_min->strftime('%Y-%m-%d'), $dt_max->strftime('%Y-%m-%d'));

    if ($months >= $MAX_PARTITIONS) {
        die sprintf "%s%s",
            "Cannot update partitions for table=$table,",
            "the gap between min/max record is too large (more than 24 months).";
    }

    if ($self->check_partition_exists($table, 'pmax')) { # already partitionined
        my $pname = $dt_max->strftime('p%Y%m');
        my $mpart = 'pmax';
        my $mdt   = $dt_max->clone;
        my $gap   = 0;
        my $diff  = ($dt_max - $dt_min)->months;
        my $all_parts = $dbh->selectall_hashref(<<SQL, 'partition_name');
SELECT partition_name, partition_description as value
  FROM information_schema.partitions
 WHERE table_schema = "$db"
   AND table_name = "$table"
SQL
        die "Cannot select partitions: ".$DBI::errstr if $DBI::err;
        delete $all_parts->{pmax};
        while ($diff >= 0) {
            my $pname = $dt_max->strftime('p%Y%m');
            unless ($self->check_partition_exists($table, $pname)) {
                $gap++;
            } else {
                if ($gap) {
                    $self->reorganize_partitions($table, $mpart, $mdt, $gap);
                }
                $gap   = 0;
                $mpart = $pname;
                $mdt   = $dt_max->clone;
                delete $all_parts->{$pname};
            }
            $dt_max->subtract(months => 1);
            last unless $diff;
            $diff = ($dt_max - $dt_min)->months;
        }
        if ($gap) {
            $self->reorganize_partitions($table, $mpart, $mdt, $gap);
        }
        # drop existing obsolete and unused partitions
        foreach my $part (keys %{$all_parts}) {
            my $pdt = DateTime->now(time_zone => 'local',
                                    epoch => $all_parts->{$part}->{value});
            # drop partitions older than the possible min
            if ($pdt <= $dt_min) {
                $self->drop_partition($table, $part);
            }
        }
    } else {
        my $mpart = 'pmax';
        my $mdt   = $dt_max->clone;
        $self->create_partitions($table, $mpart, $mdt, $months+1);
    }
}

sub backup_partition {
    my ($self, $table, $bm) = @_;

    my $dbh = $self->env('dbh');
    my $col = $self->env('timestamp-column');
    my @cols = ("min($col)", "max($col)");
    my $mtable = $table . '_' . $bm->strftime('%Y%m');
    my $pname = 'p'.$bm->strftime('%Y%m');
    my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols));
        return unless $min_ts; # empty table

    $self->debug("checking table=$table pname=$pname");

    if ($self->check_partition_exists($table, $pname)) {
        my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols),
                                    "PARTITION ($pname)");
        return unless $min_ts;

        $self->debug("table=$table backup=$mtable");

        if ($self->check_table_exists($mtable)) {
            die sprintf "%s %s",
                "$mtable: already exists, partition $pname",
                "cannot be exchanged, manual fix is required.";
        } else {
            $dbh->do("create table $mtable like $table");
        }
        die "Cannot create table: $mtable: ".$DBI::errstr if $DBI::err;
        if ($self->check_table_partitioned($mtable)) {
            $dbh->do("alter table $mtable remove partitioning");
        }
        $dbh->do("alter table $table exchange partition $pname with table $mtable");
        die "Cannot exchange partition $table -> $mtable: ".$DBI::errstr if $DBI::err;
    }
}

sub delete_loop {
    my ($self, $table, $mtable, $mstart, $mend) = @_;

    $self->debug("table=$table backup=$mtable");

    my $dbh = $self->env('dbh');
    my $batch = $self->env('batch') // 0;
    my $limit = $batch ? "limit $batch" : '';

    my $col = $self->env('time-column');
    my $col_mode = $self->env('time-column-mode');

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

    die "No primary key columns for table $table" unless @keycols;

    my $primary_key_cols = join(",",@keycols);

    $dbh->do("create table if not exists $mtable like $table");

    while (1) {
        my $temp_table = $table . "_tmp";
        my $size = 0;
        if ($col_mode eq "time") {
            $size = $dbh->do(<<SQL, undef, $mstart, $mend)
CREATE TEMPORARY TABLE $temp_table AS
(SELECT $primary_key_cols
   FROM $table
  WHERE $col BETWEEN ? AND ?
 $limit)
SQL
                or die "Failed to create temporary table $temp_table: " . $DBI::errstr;
        } else {
            $size = $dbh->do(<<SQL, undef, $mstart, $mend)
CREATE TEMPORARY TABLE $temp_table AS
(SELECT $primary_key_cols
   FROM $table
  WHERE $col BETWEEN UNIX_TIMESTAMP(?) AND UNIX_TIMESTAMP(?)
 $limit)
SQL
                or die "Failed to create temporary table $temp_table: " . $DBI::errstr;
        }
        if ($size > 0) {
            $dbh->do(<<SQL)
INSERT INTO $mtable
SELECT s.*
  FROM $table AS s
 INNER JOIN $temp_table AS t USING ($primary_key_cols)
SQL
                or die "Failed to insert into monthly table $mtable: " . $DBI::errstr;
            $dbh->do(<<SQL)
DELETE d.*
  FROM $table AS d
 INNER JOIN $temp_table AS t USING ($primary_key_cols)
SQL
                or die "Failed to delete records out of $table: " . $DBI::errstr;
        }
        $dbh->do("DROP TEMPORARY TABLE $temp_table")
            or die "Failed to drop temporary table $temp_table: " . $DBI::errstr;
        last unless $size > 0;
    }
}

sub archive_dump {
    my ($self, $table) = @_;

    $self->debug($table);

    my $dbh = $self->env('dbh');
    my $db = $self->env('own_db');
    my $month = $self->env('archive-months');
    my $target = $self->env('archive-target');
    my $compress = $self->env('compress');
    my $user = $self->env('username');
    my $pass = $self->env('password');
    my $host = $self->env('host');

    while (1) {
        my $now = DateTime->now(time_zone => 'local');
        my $bm = $now->clone;
        $bm->subtract(months => $month)->truncate(to => "month");
        my $mtable = $table . "_" . $bm->strftime('%Y%m');
        my $res = $dbh->selectcol_arrayref("show table status like ?", undef, $mtable);
        ($res && @$res && $res->[0]) or last;
        if ($target ne '/dev/null') {
            my $fname = sprintf("%s/%s.%s.sql", $target,
                                    $mtable, $now->strftime("%Y%m%d%H%M%S"));

            my @cmd = ('mysqldump');
            $user and push(@cmd, "-u" . $user);
            $pass and push(@cmd, "-p" . $pass);
            $host and push(@cmd, "-h" . $host);
            push(@cmd, "--opt", $db, $mtable);

            for (@cmd) { s/'/'"'"'/g; $_ = "'$_'" }
            my $cmd = join(' ', @cmd);

            if (system("$cmd > $fname")) {
                unlink($fname);
                die "MySQL DUMP of table $mtable into file $fname failed\n";
            }
            if ($compress eq 'gzip') {
                if (system("nice gzip -9 $fname")) {
                    unlink($fname, "$fname.gz");
                    die "Gzipping of dump file $fname failed\n";
                }
            }
            $self->debug("created backup=$fname");
        }
        $self->debug("drop table=$mtable");
        $dbh->do("drop table $mtable");
        $month++;
    }
}

sub backup_table {
    my ($self, $table) = @_;

    my $use_part = $self->env('use-partitioning') // 'no';
    if ($use_part eq 'yes') {
        $self->update_partitions($table);
    }

    my $backup_months = $self->env('backup-months');
    my $backup_retro = $self->env('backup-retro');

    for my $cmonth (0 .. ($backup_retro - 1)) {
        my $tmonths = $cmonth + $backup_months;
        my $now = DateTime->now(time_zone => 'local');
        my $bm = $now->clone;
        $bm->subtract(months => $tmonths)->truncate(to => 'month');
        my $mstart = $bm->strftime('%Y-%m-01 00:00:00');
        my $mend = $bm->add(months => 1)->subtract(seconds => 1)
                    ->strftime('%Y-%m-%d %H:%M:%S');
        my $mtable = $table . '_' . $bm->strftime('%Y%m');
        if ($use_part eq 'yes') {
            $self->backup_partition($table, $bm);
        } else {
            $self->delete_loop($table, $mtable, $mstart, $mend);
        }
    }

    return 1;
}

sub cleanup_table {
    my ($self, $table) = @_;

    $self->debug($table);

    my $batch = $self->env('batch') // 0;
    my $limit = $batch ? "limit $batch" : '';
    my $col = $self->env('time-column');
    my $dbh = $self->env('dbh');
    my $cleanup_days = $self->env('cleanup_days');
    my $deleted_rows = 0;

    while (1) {
        my $aff = $dbh->do(<<SQL, undef, $cleanup_days);
DELETE FROM $table WHERE $col < date(date_sub(now(), interval ? day)) $limit
SQL
        $deleted_rows += $aff;
        last unless $aff > 0;
    }
    $self->debug("table=$table deleted rows=$deleted_rows");
}

1;
