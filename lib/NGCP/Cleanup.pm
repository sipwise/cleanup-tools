package NGCP::Cleanup;

use strict;
use warnings;
use English;
use DBI;
use DateTime;
use Data::Dumper;
use Log::Log4perl;
use Log::Log4perl::Level;
use Redis;

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
        'connect-redis' => sub {
            my ($self, $db) = @_;
            my $host = $self->env('host');
            my $port = $self->env('redis-port');
            my $redis = Redis->new(server => $host.':'.$port)
                or die "Failed to connect to Redis $host:$port";
            $redis->select($db);
            $self->env(redis => $redis);
        },
        backup => sub {
            my ($self, $table) = @_;
            $table or die "No table name provided in the 'backup' command";
            $self->env('dbh') or die "Not connected to a DB in the 'backup' command";
            if ($self->env('timestamp-column')) {
                $self->env('time-column-mode' => 'timestamp');
                $self->env('time-column' => $self->env('timestamp-column'));
            } elsif ($self->env('time-column')) {
                $self->env('time-column-mode' => 'time');
            }
            foreach my $v (qw(time-column time-column-mode keep-months)) {
                $self->env($v)
                    or die "Variable '$v' is not set in the 'backup' command";
            }
            # deprecated commands, config upgrade is mandatory
            $self->env('backup-months') and
                die "backup-months is deprecated and replaced by keep-months, please upgrade or adjust the respective config file\n";
            $self->env('retro-months') and
                die "retro-months is deprecated and removed, plase upgarde or adjust the respective config file\n";
            #
            $self->backup_table($table);
        },
        archive => sub {
            my ($self, $table) = @_;
            $table or die "No table name provided in the 'archive' command";
            $self->env('dbh') or die "Not connected to a DB in the 'archive' command";
            foreach my $v (qw(archive-months archive-target)) {
                $self->env($v)
                    or die "Variable '$v' is not set in the 'archive' command";
            }
            $self->archive_dump($table);
        },
        cleanup => sub {
            my ($self, $table) = @_;
            $table or die "No table name provided in the 'cleanup' command";
            $self->env('dbh') or die "Not connected to a DB in the 'cleanup' command";
            foreach my $v (qw(time-column cleanup-days)) {
                $self->env($v)
                    or die "Variable '$v' is not set in the 'cleanup' command";
            }
            $self->cleanup_table($table);
        },
        'cleanup-redis' => sub {
            my ($self, $scan_keys) = @_;
            $scan_keys or die "No key-pattern provided in the 'cleanup-redis' command";
            foreach my $v (qw(time-column cleanup-days cleanup-mode)) {
                $self->env($v)
                    or die "Variable '$v' is not set in the 'cleanup-redis' command";
            }
            $self->env('redis') or die "Not connected to Redis in the 'cleanup-redis' command";
            $self->cleanup_redis($scan_keys);
        },
    );
}

sub init_config {
    my ($self, $config_file) = @_;

    open(my $config_fh, '<', $config_file)
        or die "Couldn't open configuration file '$config_file'.\n";

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
    my $keep_months = $self->env('keep-months');
    my $col = $self->env('time-column');
    my @cols = ("min($col)", "max($col)");
    my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols));

    return unless $min_ts;

    my $dt_min = DateTime->now(epoch => $min_ts, time_zone => 'local');
    $dt_min->truncate(to => "month");
    my $dt_max = DateTime->now(epoch => $max_ts, time_zone => 'local');
    $dt_max->add(months => 1);
    $dt_max->truncate(to => 'month');
    my $dt_diff = $dt_max-$dt_min;
    my $months = ($dt_diff->years*12+$dt_diff->months) * ($dt_diff->is_negative() ? -1 : 1);

    $self->debug(sprintf "table=%s checking start=%s end=%s",
        $table, $dt_min->strftime('%Y-%m'), $dt_max->strftime('%Y-%m'));

    if ($months >= $MAX_PARTITIONS) {
        die sprintf "%s%s",
            "Cannot update partitions for table=$table, ",
            "the gap between max record time and min record time is too large (more than 24 months).";
    }

    if ($self->check_partition_exists($table, 'pmax')) { # already partitionined
        my $pname = $dt_max->strftime('p%Y%m');
        my $mpart = 'pmax';
        my $mdt   = $dt_max->clone;
        my $gap   = 0;
        my $all_parts = $dbh->selectall_hashref(<<SQL, 'partition_name');
SELECT partition_name, partition_description as value
  FROM information_schema.partitions
 WHERE table_schema = "$db"
   AND table_name = "$table"
SQL
        die "Cannot select partitions: ".$DBI::errstr if $DBI::err;
        delete $all_parts->{pmax};
        while ($dt_max >= $dt_min) {
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
        }
        if ($gap) {
            $self->reorganize_partitions($table, $mpart, $mdt, $gap);
        }
        # drop existing unused partitions
        foreach my $part (sort { $a cmp $b } keys %{$all_parts}) {
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
    my $col = $self->env('time-column');
    my @cols = ("min($col)", "max($col)");
    my $mtable = $table . '_' . $bm->strftime('%Y%m');
    my $pname = 'p'.$bm->strftime('%Y%m');

    $self->debug("checking table=$table pname=$pname");

    if ($self->check_partition_exists($table, $pname)) {
        my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols),
                                    "PARTITION ($pname)");
        return unless $min_ts;

        $self->debug("table=$table backup=$mtable");

        if ($self->check_table_exists($mtable)) {
            $self->info("table $mtable already exists, fallback to the table backup method");
            $self->delete_loop($table, $bm);
        } else {
            $dbh->do("create table $mtable like $table");
            die "Cannot create table: $mtable: ".$DBI::errstr if $DBI::err;
            if ($self->check_table_partitioned($mtable)) {
                # drop partitioning layout inherited from copied table creation
                $dbh->do("alter table $mtable remove partitioning");
            }
            $dbh->do("alter table $table exchange partition $pname with table $mtable");
            die "Cannot exchange partition $table -> $mtable: ".$DBI::errstr if $DBI::err;
        }
    }
}

sub delete_loop {
    my ($self, $table, $bm) = @_;

    my $dbh = $self->env('dbh');
    my $batch = $self->env('batch') // 0;
    my $limit = $batch ? "limit $batch" : '';

    my $col = $self->env('time-column');
    my $col_mode = $self->env('time-column-mode');

    my $mstart = $bm->strftime('%Y-%m-01 00:00:00');
    my $mend   = $bm->add(months => 1)->subtract(seconds => 1)
                          ->strftime('%Y-%m-%d %H:%M:%S');
    my $mtable = $table . '_' . $bm->strftime('%Y%m');

    $self->debug("table=$table backup=$mtable");

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
        eval { $self->update_partitions($table) };
        if ($EVAL_ERROR) {
            $self->debug("table=$table cannot be used for partitioning, falling back to sql backups");
            $use_part = 'no';
        }
    }

    my $data_moved = 0;;
    my $keep_months = $self->env('keep-months');
    my $col = $self->env('time-column');
    my @cols = ("min($col)", "max($col)");
    my ($min_ts, $max_ts) = $self->fetch_row(undef, $table, join(',', @cols));
    return unless $min_ts;

    my $dt_min = DateTime->now(epoch => $min_ts, time_zone => 'local');
    $dt_min->truncate(to => "month");
    my $dt_max = DateTime->now(time_zone => 'local');
    $dt_max->truncate(to => "month");
    $dt_max->subtract(months => $keep_months);

    while ($dt_min <= $dt_max) {
        if ($use_part eq 'yes') {
            $self->backup_partition($table, $dt_min);
        } else {
            $self->delete_loop($table, $dt_min);
        }
        $dt_min->add(months => 1);
        $data_moved = 1;
    }

    if ($use_part eq 'yes' && $data_moved) {
        $self->update_partitions($table);
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
    my $cleanup_days = $self->env('cleanup-days');
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

sub cleanup_redis {
    my ($self, $scan_keys) = @_;

    $self->debug("cleanup-redis: ".$scan_keys);

    my $batch = $self->env('redis-batch') // 0;
    my $redis = $self->env('redis');
    my $time_col = $self->env('time-column');
    my $dbh = $self->env('dbh') // undef;
    my $db = $self->env('own_db') // undef;
    my $table = 'acc_backup';
    my $cleanup_days = $self->env('cleanup-days');
    my $cleanup_mode = $self->env('cleanup-mode');

    my $deleted_rows = 0;

    my $cursor = 0;
    my $now = time;
    my @sql_buffer;
    my @cols = qw(method callid);
    my $query = "";

    if ($cleanup_mode eq "mysql") {
        @cols = map { $_->[0] }
            @{$dbh->selectall_arrayref(<<SQL, undef, $db, $table)
SELECT column_name FROM information_schema.columns
 WHERE table_schema = ?
   AND table_name = ?
   AND column_name != "id"
ORDER BY ordinal_position ASC
SQL
};
        die "Cannot select data: ".$DBI::errstr if $DBI::err;
    }

    while (1) {
        my $res = $redis->scan($cursor, MATCH => $scan_keys, COUNT => $batch);
        $cursor = shift @{ $res };
        my $keys = shift @{ $res };

        my $sql = "INSERT INTO $table (" . join(',', @cols) . ") VALUES ";

        my %vals = ();
        foreach my $key (@{ $keys }) {
            my %data = $redis->hgetall($key);
            my $data_ok = 1;
            foreach my $c (($time_col, qw/callid method/)) {
                unless ($data{$c}) {
                    $self->error("missing '$c' column") ;
                    $data_ok = 0;
                    last;
                }
                if ($c eq $time_col && $data{$c} !~ /^\d+(\.\d+)?$/) {
                    $self->error("invalid time column '$time_col' format, expected unixtime");
                    $data_ok = 0;
                    last;
                }
            }
            last if not $data_ok;

            my $time = $data{$time_col};

            next if (($time + $cleanup_days*86400) > $now);

            if ($cleanup_mode eq "mysql") {
                my @query_data = map { $data{$_} } @cols;
                $query .= "('" . join("','", @query_data) . "'),";
            }

            $vals{$key} = {
                meth => $data{method},
                cid  => $data{callid},
            };
        }
        last unless %vals;

        $sql .= substr($query, 0, -1);

        my $sql_ok = $cleanup_mode eq "mysql" && $dbh->do($sql);

        if ($cleanup_mode eq "delete" || $sql_ok) {
            foreach my $key (@{ $keys }) {
                # delete direct entry
                $redis->del($key);

                my $cid = $vals{$key}->{cid};
                my $meth = $vals{$key}->{meth};

                # delete from cid map
                $redis->srem("acc:cid::$cid", $key);
                # delete from meth map
                $redis->srem("acc:meth::$meth", $key);
            }
        } else {
            $self->error("insert into mysql: ".$DBI::errstr) if $DBI::err;
            last;
        }

        $deleted_rows += $#$keys+1;

        last unless $cursor;
    }

    if ($cleanup_mode eq "mysql") {
        $self->debug("redis=$scan_keys mode=$cleanup_mode table=$db.$table moved rows=$deleted_rows");
    } else {
        $self->debug("redis=$scan_keys mode=$cleanup_mode deleted rows=$deleted_rows");
    }
    return;
}

1;
